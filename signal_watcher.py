"""
Слушатель крипто-чата: подключается к Telegram под вашим аккаунтом
(userbot, через Telethon), следит за новым сообщениями в указанном чате,
прогоняет их через локальный префильтр и (опционально) через Claude API,
и при обнаружении сигнала будит вас пуш-уведомлением через Pushover.

Первый запуск интерактивный — попросит код из Telegram (и пароль 2FA,
если включён) и сохранит сессию в файл signal_watcher.session, после
чего скрипт можно запускать без участия человека (см. README и
crypto-signal-watcher.service для автозапуска на VPS).
"""

import asyncio
import base64
import logging
import os
import sys
from collections import deque

from dotenv import load_dotenv
from telethon import TelegramClient, events

from evaluator import evaluate
from notifier import send_push
from prefilter import looks_like_signal

load_dotenv()

# Сколько последних сообщений чата хранить как контекст для LLM-оценки —
# сигналы в этом чате часто растянуты на несколько сообщений подряд
# (сначала "Short BUN_USDT", затем отдельным сообщением твх/сайз и т.д.).
CONTEXT_SIZE = int(os.getenv("CONTEXT_SIZE", "6"))
_recent_messages: deque[str] = deque(maxlen=CONTEXT_SIZE)

LOG_FILE = os.getenv("LOG_FILE", "signal_watcher.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("signal_watcher")


SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "signal_watcher")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        logger.error("Не задана обязательная переменная окружения %s (см. .env.example)", name)
        sys.exit(1)
    return value


def _restore_session_from_env() -> None:
    """На хостинге вроде Railway нет интерактивного терминала для первого
    логина, поэтому файл сессии создаётся один раз локально (см. README,
    раздел про Railway), кодируется в base64 и кладётся в переменную
    окружения TELEGRAM_SESSION_B64. При каждом старте контейнера, если
    локального файла сессии ещё нет, мы восстанавливаем его из этой
    переменной — дальше Telethon подключается уже без повторного логина."""

    session_path = f"{SESSION_NAME}.session"
    if os.path.exists(session_path):
        return

    b64 = os.getenv("TELEGRAM_SESSION_B64")
    if not b64:
        return

    try:
        with open(session_path, "wb") as f:
            f.write(base64.b64decode(b64))
        logger.info("Восстановил файл сессии из TELEGRAM_SESSION_B64")
    except Exception:
        logger.exception("Не удалось восстановить сессию из TELEGRAM_SESSION_B64")


async def resolve_chat(client: TelegramClient, chat_ref: str):
    """Пытается найти чат по username, числовому id или названию."""
    # username или числовой id Telethon разрулит сам
    try:
        if chat_ref.lstrip("-").isdigit():
            return await client.get_entity(int(chat_ref))
        return await client.get_entity(chat_ref)
    except Exception:
        pass

    # запасной вариант — поиск по точному названию среди диалогов
    async for dialog in client.iter_dialogs():
        if dialog.name == chat_ref:
            return dialog.entity

    raise RuntimeError(
        f"Не удалось найти чат '{chat_ref}'. Проверьте TELEGRAM_CHAT в .env "
        f"(username без @, числовой id вида -1001234567890, либо точное название)."
    )


async def handle_message(event, chat_title: str, client: TelegramClient, archive_entity=None):
    text = event.raw_text or ""

    # Дублируем КАЖДОЕ сообщение с текстом в архивный чат (если он задан),
    # независимо от того, похоже оно на сигнал или нет — это не пересылка
    # (forward), а обычная отправка нового сообщения с тем же текстом, так
    # что запрет пересылки в исходном чате тут ни при чём.
    if archive_entity is not None and text.strip():
        try:
            await client.send_message(archive_entity, text)
        except Exception:
            logger.exception("Не удалось продублировать сообщение в архивный чат")

    # Контекст берём ДО добавления текущего сообщения, чтобы не дублировать
    # его же самого; добавляем текущее сообщение в историю в любом случае —
    # даже отфильтрованные локально сообщения могут быть полезным контекстом
    # для оценки следующего.
    context = list(_recent_messages)
    if text.strip():
        _recent_messages.append(text)

    if not looks_like_signal(text):
        return

    logger.info("Похоже на сигнал, отправляю на оценку: %.120s", text)
    result = evaluate(text, context=context)

    min_confidence = float(os.getenv("MIN_CONFIDENCE", "0.5"))
    if not result["is_signal"] or result["confidence"] < min_confidence:
        logger.info(
            "Отфильтровано после оценки (is_signal=%s, confidence=%.2f)",
            result["is_signal"],
            result["confidence"],
        )
        return

    symbol = result.get("symbol") or "?"
    exchange = result.get("exchange") or ""
    direction = result.get("direction") or ""
    title = f"Сигнал {symbol} {exchange} {direction}".strip()
    message = result["summary"]

    chat_link = None
    if getattr(event.chat, "username", None):
        chat_link = f"https://t.me/{event.chat.username}"

    send_push(title=title, message=f"[{chat_title}] {message}", url=chat_link)


async def main():
    api_id = int(_require_env("TELEGRAM_API_ID"))
    api_hash = _require_env("TELEGRAM_API_HASH")
    chat_ref = _require_env("TELEGRAM_CHAT")
    phone = os.getenv("TELEGRAM_PHONE")

    _restore_session_from_env()
    client = TelegramClient(SESSION_NAME, api_id, api_hash)

    logger.info("Подключаюсь к Telegram...")
    await client.start(phone=phone)  # при первом запуске спросит код/пароль
    logger.info("Подключено как %s", (await client.get_me()).username or "unknown")

    chat_entity = await resolve_chat(client, chat_ref)
    chat_title = getattr(chat_entity, "title", None) or getattr(chat_entity, "username", chat_ref)
    logger.info("Слушаю чат: %s", chat_title)

    archive_ref = os.getenv("ARCHIVE_CHAT")
    archive_entity = None
    if archive_ref:
        archive_entity = await resolve_chat(client, archive_ref)
        archive_title = getattr(archive_entity, "title", None) or getattr(archive_entity, "username", archive_ref)
        logger.info("Дублирую все сообщения чата в: %s", archive_title)

    @client.on(events.NewMessage(chats=chat_entity))
    async def _handler(event):
        try:
            await handle_message(event, chat_title, client, archive_entity)
        except Exception:
            logger.exception("Ошибка при обработке сообщения")

    logger.info("Готов. Ожидаю новые сообщения...")
    await client.run_until_disconnected()


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception:
            logger.exception("Скрипт упал, перезапуск через 30 секунд")
            import time

            time.sleep(30)
