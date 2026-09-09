"""
Оценка отфильтрованного сообщения через LLM: превращает сырой текст
сигнала в короткую структурированную сводку, которую удобно читать
спросонья в пуш-уведомлении.

Поддерживает два провайдера — выбирается через .env -> LLM_PROVIDER:
  - "anthropic" (по умолчанию) — Claude API, ключ ANTHROPIC_API_KEY
  - "openai"    — ChatGPT API (OpenAI), ключ OPENAI_API_KEY

Если USE_LLM_EVAL=false или нужный ключ не задан — используется заглушка,
которая просто пересылает исходный текст без оценки (evaluate() всегда
возвращает dict одинаковой формы, вызывающему коду не важно, откуда он).
"""

import json
import logging
import os

logger = logging.getLogger("signal_watcher")

SYSTEM_PROMPT = """\
Ты — ассистент, который ночью фильтрует сообщения из крипто-чата со
спред/арбитражными торговыми сигналами, чтобы разбудить пользователя
только по делу.

Важный контекст про стиль этого конкретного чата: сигналы там пишут не
шаблонно ("LONG BTC entry/TP/SL"), а разговорным сленгом трейдеров,
часто вперемешку кириллицей и латиницей, с опечатками, и один сигнал
может быть растянут на несколько сообщений подряд от одного автора.
Типичные термины:
- "лонг"/"шорт" (и формы "шорчу", "зашортить", "лонгую") — направление;
- "твх" — точка/цена входа;
- "сайз" — размер позиции;
- "хедж"/"захеджить" — открыть противоположную позицию на другой бирже
  для страховки;
- "адл" — риск автоматического делевериджа позиции биржей;
- "спред"/"разрыв" — разница цены одного актива на разных биржах, на
  которой строится арбитраж;
- "памп"/"дамп" — резкий рост/падение цены;
- "стопы бу" — перенос стоп-лосса в безубыток;
- названия бирж (MEXC, Kucoin, Binance, Bybit и т.п.) как латиницей, так
  и кириллицей ("кукоин", "байбит").

Тебе дают текст нового сообщения и, возможно, несколько последних
сообщений из этого же чата как контекст (они могут быть частью того же
сигнала — например, инструкция пришла раньше, а сейчас автор уточняет
размер позиции или биржу). Используй контекст только для понимания, а
оценивай и суммируй именно ПОСЛЕДНЕЕ сообщение.

Определи:
1. Это реально торговая инструкция (конкретная сделка/действие: монета,
   направление или конкретное указание что делать — открыть, захеджить,
   подвинуть стоп и т.п.), или это общая болтовня, мем, вопрос, реклама,
   не относящееся к делу сообщение?
2. Если это сигнал — извлеки параметры (монета, биржа(и), направление,
   уровни/твх/сайз, если есть) и дай короткую (1-3 предложения) сводку на
   русском, понятную человеку, который только что проснулся и не читал
   предыдущие сообщения.
3. Оцени уверенность (confidence, 0.0-1.0), что это именно конкретное
   торговое указание, а не общие рассуждения о рынке.

Ответь СТРОГО в виде JSON без пояснений вокруг, по схеме:
{
  "is_signal": true/false,
  "symbol": "XVG/USDT" | null,
  "exchange": "Bybit" | null,
  "direction": "long" | "short" | null,
  "summary": "короткая сводка на русском",
  "confidence": 0.0-1.0
}
"""


def _stub_result(text: str) -> dict:
    return {
        "is_signal": True,
        "symbol": None,
        "exchange": None,
        "direction": None,
        "summary": text.strip()[:300],
        "confidence": 1.0,
    }


def _build_user_content(text: str, context: list[str] | None) -> str:
    if not context:
        return text
    history = "\n---\n".join(context)
    return (
        f"Предыдущие сообщения в чате (контекст, не оценивать):\n{history}\n\n"
        f"=== Новое сообщение для оценки ===\n{text}"
    )


def _parse_json_reply(raw: str, text: str) -> dict:
    raw = raw.strip()
    # На случай если модель обернула JSON в ```json ... ```
    if raw.startswith("```"):
        raw = raw.strip("`")
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    data = json.loads(raw)
    return {
        "is_signal": bool(data.get("is_signal", True)),
        "symbol": data.get("symbol"),
        "exchange": data.get("exchange"),
        "direction": data.get("direction"),
        "summary": data.get("summary") or text.strip()[:300],
        "confidence": float(data.get("confidence", 0.5)),
    }


def _evaluate_anthropic(text: str, context: list[str] | None, api_key: str) -> dict:
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-5-20250929")

    resp = client.messages.create(
        model=model,
        max_tokens=300,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": _build_user_content(text, context)}],
    )
    raw = "".join(
        block.text for block in resp.content if getattr(block, "type", "") == "text"
    )
    return _parse_json_reply(raw, text)


def _evaluate_openai(text: str, context: list[str] | None, api_key: str) -> dict:
    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    # Дешёвая модель — этой задаче (короткая классификация + сводка)
    # хватает младшей модели линейки, проверьте актуальный id и цены на
    # https://platform.openai.com/docs/pricing
    model = os.getenv("OPENAI_MODEL", "gpt-5.6-luna")

    resp = client.chat.completions.create(
        model=model,
        max_completion_tokens=300,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_content(text, context)},
        ],
    )
    raw = resp.choices[0].message.content or ""
    return _parse_json_reply(raw, text)


def evaluate(text: str, context: list[str] | None = None) -> dict:
    """Оценивает сообщение. context — до нескольких предыдущих сообщений
    того же чата (старые первыми), только для понимания смысла; сводка и
    оценка всегда даются по последнему (текущему) сообщению.

    Всегда возвращает dict с ключами
    is_signal / symbol / exchange / direction / summary / confidence."""

    use_llm = os.getenv("USE_LLM_EVAL", "true").lower() == "true"
    provider = os.getenv("LLM_PROVIDER", "anthropic").strip().lower()

    if not use_llm:
        return _stub_result(text)

    try:
        if provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            if not api_key:
                return _stub_result(text)
            return _evaluate_openai(text, context, api_key)

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            return _stub_result(text)
        return _evaluate_anthropic(text, context, api_key)
    except Exception:
        logger.exception("LLM-оценка (%s) не удалась, отправляю сырой текст без оценки", provider)
        return _stub_result(text)
