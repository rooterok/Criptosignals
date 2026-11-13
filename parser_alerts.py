"""
parser_alerts.py — фильтр каналов-парсеров для signal-watcher (Telethon + Pushover).

Подключение в основном скрипте бота, после создания client:

    import parser_alerts
    parser_alerts.register(client)

Новых зависимостей нет (только стандартная библиотека + Telethon).

Переменные окружения (все необязательные, кроме PUSHOVER_* для режима live):
    PUSHOVER_TOKEN, PUSHOVER_USER   - ключи Pushover
    PARSER_ALERTS_MODE              - shadow (по умолчанию: только лог) | live (слать пуши)
    PARSER_LOG_ALL                  - 1: писать в лог каждый разобранный пост (по умолчанию 1 в shadow, 0 в live)
    PARSER_DEDUPE_SECONDS           - не чаще одного пуша на тикер за N секунд (600)
    PARSER_TZ                       - часовой пояс для тихих часов (Asia/Ho_Chi_Minh)
    PARSER_QUIET_FROM / _TO         - тихие часы, час начала и конца (23 и 8)
    PARSER_QUIET_PRIORITY           - приоритет Pushover в тихие часы (-1: без звука; -2: без уведомления)

    Пороги (в процентах), стартовые значения подбираются по теневому режиму:
    NDA_MIN_SPREAD=30      Futures Arbitrage NDA (обычные посты там 10-17%)
    SPREADS_MIN=30         Futures Spreads 3% / 7%
    DEX_MIN=15             Mexc Dex Spread Tracker Alerts
    FEED_MIN_FAIR_GAP=20   Gate | Feed: отрыв last от fair
    FEED_MIN_MOVE=50       Gate | Feed: движение цены
    FAIR_LAST_MIN=8        FAIR / LAST PRICE SPREAD (бот и сам шлёт от 8%)
    SPLASH_MIN=100         splash-каналы: движение цены
    SPLASH_GAP_MIN=15      splash-каналы: отрыв last от справедливой
    KORM_MIN=30            Mexc Price | Kormushka

Пуши идут обычным приоритетом (0): не будят ночью и не пробивают режим «не беспокоить».
"""
import asyncio
import logging
import os
import re
import statistics
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("parser_alerts")


def _env_f(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return float(default)


MODE = os.getenv("PARSER_ALERTS_MODE", "shadow").lower()
LOG_ALL = os.getenv("PARSER_LOG_ALL", "1" if MODE == "shadow" else "0") == "1"
PUSHOVER_TOKEN = os.getenv("PUSHOVER_TOKEN", "")
PUSHOVER_USER = os.getenv("PUSHOVER_USER", "")
DEDUPE_SECONDS = int(_env_f("PARSER_DEDUPE_SECONDS", 600))
TZ = ZoneInfo(os.getenv("PARSER_TZ", "Asia/Ho_Chi_Minh"))
QUIET_FROM = int(_env_f("PARSER_QUIET_FROM", 23))
QUIET_TO = int(_env_f("PARSER_QUIET_TO", 8))
QUIET_PRIORITY = int(_env_f("PARSER_QUIET_PRIORITY", -1))

NDA_MIN_SPREAD = _env_f("NDA_MIN_SPREAD", 30)
SPREADS_MIN = _env_f("SPREADS_MIN", 30)
DEX_MIN = _env_f("DEX_MIN", 15)
FEED_MIN_FAIR_GAP = _env_f("FEED_MIN_FAIR_GAP", 20)
FEED_MIN_MOVE = _env_f("FEED_MIN_MOVE", 50)
FAIR_LAST_MIN = _env_f("FAIR_LAST_MIN", 8)
SPLASH_MIN = _env_f("SPLASH_MIN", 100)
SPLASH_GAP_MIN = _env_f("SPLASH_GAP_MIN", 15)
KORM_MIN = _env_f("KORM_MIN", 30)


@dataclass
class Alert:
    key: str       # для антидубля: правило + тикер
    title: str     # заголовок пуша
    message: str   # текст пуша


@dataclass
class Result:
    ticker: str
    metric: str                  # короткая строка для лога (что именно разобрали)
    alert: Optional[Alert] = None


NUM = r"[+-]?\d+(?:\.\d+)?"


def _first_ticker(text: str) -> str:
    for line in text.strip().splitlines():
        line = line.strip()
        if line:
            m = re.search(r"[\w$#]+", line)
            return m.group(0).lstrip("$#") if m else line[:12]
    return "?"


def _kv(text: str, label: str) -> Optional[str]:
    """Значение строки вида 'Max size: $2.17k' (всё после двоеточия)."""
    m = re.search(re.escape(label) + r"\s*:\s*([^\n]+)", text, re.I)
    return m.group(1).strip() if m else None


def _extras(text: str, labels: list[str]) -> str:
    parts = []
    for lb in labels:
        v = _kv(text, lb)
        if v:
            parts.append(f"{lb} {v}")
    return " · ".join(parts)


def _gap(last: float, ref: float) -> Optional[float]:
    return (last - ref) / ref * 100 if ref > 0 else None


# ---------- правила по каналам (форматы сняты с реальных постов) ----------

ROUTE_RE = re.compile(r"([A-Za-z0-9_.]+)\s*→\s*[^\w\s]*\s*([A-Za-z0-9_.]+)\s*:\s*(" + NUM + r")\s*%")
PRICE_ROW_RE = re.compile(r"^\s*([A-Z][A-Z0-9_.]+)\s*\|\s*(\d+(?:\.\d+)?)\s*\|", re.M)


def rule_nda(chan: str, text: str) -> Optional[Result]:
    """Futures Arbitrage NDA: таблица Price|Funding и строки 'A→B: X%'."""
    routes = [(a, b, float(p)) for a, b, p in ROUTE_RE.findall(text)]
    if not routes:
        return None
    a, b, top = max(routes, key=lambda r: r[2])
    ticker = _first_ticker(text)
    res = Result(ticker, f"{a}→{b} {top:.1f}%")
    if top < NDA_MIN_SPREAD:
        return res
    msg = f"{ticker}: {a}→{b} {top:.1f}%"
    prices = {ex: float(p) for ex, p in PRICE_ROW_RE.findall(text)}
    if len(prices) >= 3:
        med = statistics.median(prices.values())
        if med > 0:
            ex = max(prices, key=lambda k: max(prices[k] / med, med / prices[k]) if prices[k] > 0 else 0)
            ratio = max(prices[ex] / med, med / prices[ex]) if prices[ex] > 0 else 0
            msg += f"\nВыброс цены: {ex} {prices[ex]:g} при медиане {med:.4g} (x{ratio:.1f})"
            if ratio >= 3:
                msg += "\nПохоже на выброс last: сначала проверь mark/index на бирже"
    res.alert = Alert(f"nda:{ticker}", "Спред NDA", msg)
    return res


SPREAD_RE = re.compile(r"Spread:\s*(" + NUM + r")\s*%", re.I)


def rule_spreads(chan: str, text: str) -> Optional[Result]:
    """Futures Spreads 3% / 7%: '#INIT | Spread: 3.50%'. Пост 'aligned' = спред закрылся."""
    if "aligned" in text.lower():
        return None
    m = SPREAD_RE.search(text)
    if not m:
        return None
    val = abs(float(m.group(1)))
    t = re.search(r"#([A-Za-z0-9_]+)", text)
    ticker = t.group(1) if t else _first_ticker(text)
    res = Result(ticker, f"spread {val:.2f}%")
    if val < SPREADS_MIN:
        return res
    msg = f"{ticker}: спред {val:.1f}%"
    sh = re.search(r"Short\s+(\w+)\s*:\s*\$?([\d.]+)", text)
    lg = re.search(r"Long\s+(\w+)\s*:\s*\$?([\d.]+)", text)
    if sh and lg:
        msg += f"\nШорт {sh.group(1)} {sh.group(2)} / лонг {lg.group(1)} {lg.group(2)}"
    ms = re.search(r"Max Size\s*:\s*\$?([\d,]+)", text, re.I)
    if ms:
        msg += f"\nMax size ${ms.group(1)}"
    res.alert = Alert(f"spreads:{ticker}", "Futures Spreads", msg)
    return res


MOVE_IN_RE = re.compile(r"(" + NUM + r")\s*%\s+in\s+([^!\n]+)", re.I)
FEED_PRICE_RE = re.compile(r"price:\s*\$?\s*(\d+(?:\.\d+)?)\s*→\s*\$?\s*(\d+(?:\.\d+)?)", re.I)
FEED_FAIR_RE = re.compile(r"fair:\s*\$?\s*(\d+(?:\.\d+)?)", re.I)


def rule_feed(chan: str, text: str) -> Optional[Result]:
    """Gate | Feed: '$PLAY +5.90% in 15 secs', 'price: A → B', 'fair: C'. Сюда попадает отрыв last от fair."""
    mv = MOVE_IN_RE.search(text)
    pm = FEED_PRICE_RE.search(text)
    if not mv and not pm:
        return None
    ticker = _first_ticker(text)
    move = float(mv.group(1)) if mv else 0.0
    gap = None
    fm = FEED_FAIR_RE.search(text)
    if pm and fm:
        gap = _gap(float(pm.group(2)), float(fm.group(1)))
    metric = f"move {move:+.1f}%" + (f" gap_vs_fair {gap:+.1f}%" if gap is not None else "")
    res = Result(ticker, metric)
    parts = []
    if gap is not None and abs(gap) >= FEED_MIN_FAIR_GAP:
        parts.append(f"last {pm.group(2)} против fair {fm.group(1)}: отрыв {gap:+.0f}%")
    if mv and abs(move) >= FEED_MIN_MOVE:
        parts.append(f"движение {mv.group(1)}% за {mv.group(2).strip()}")
    if parts:
        msg = f"{ticker}: " + "; ".join(parts)
        ex = _extras(text, ["limit", "vol"])
        if ex:
            msg += "\n" + ex
        res.alert = Alert(f"feed:{ticker}", "Gate Feed", msg)
    return res


FAIRLAST_RE = re.compile(r"FAIR\s*([<>])\s*LAST\s*(" + NUM + r")\s*%", re.I)


def rule_fair_last(chan: str, text: str) -> Optional[Result]:
    """FAIR / LAST PRICE SPREAD (MEXC): 'FAIR > LAST +9.8%'. Бот сам шлёт от 8%."""
    m = FAIRLAST_RE.search(text)
    if not m:
        return None
    val = abs(float(m.group(2)))
    ticker = _first_ticker(text)
    res = Result(ticker, f"fair{m.group(1)}last {val:.1f}%")
    if val >= FAIR_LAST_MIN:
        msg = f"{ticker}: FAIR {m.group(1)} LAST на {val:.1f}% (MEXC)"
        ex = _extras(text, ["Max size", "Max lev", "24h vol"])
        if ex:
            msg += "\n" + ex
        res.alert = Alert(f"fairlast:{ticker}", "Fair / Last (MEXC)", msg)
    return res


SPLASH_CHG_RE = re.compile(r"(?:Изм\.?|Движение):\s*(" + NUM + r")\s*%(?:\s+за\s+(\S+))?")
LAST_RE = re.compile(r"(?:Now last price|Последняя):\s*\$?\s*(\d+(?:\.\d+)?)", re.I)
FAIR_RE = re.compile(r"(?:Справедл\.?\s*price|Справедливая):\s*\$?\s*(\d+(?:\.\d+)?)", re.I)
SPOT_RE = re.compile(r"Spot price:\s*\$?\s*(\d+(?:\.\d+)?)", re.I)


def rule_splash(chan: str, text: str) -> Optional[Result]:
    """splash-каналы (MEXC / GATE / ASTER). 'Изм.: +12.45%' или 'Движение: +22.74% за 10.0м',
    плюс last и справедливая цена: считаем отрыв last от справедливой."""
    m = SPLASH_CHG_RE.search(text)
    if not m:
        return None
    chg = float(m.group(1))
    t = re.search(r"\$([^\s]+)", text)
    ticker = t.group(1) if t else _first_ticker(text)
    lm, fm = LAST_RE.search(text), FAIR_RE.search(text)
    gap = _gap(float(lm.group(1)), float(fm.group(1))) if lm and fm else None
    metric = f"chg {chg:+.1f}%" + (f" gap_vs_fair {gap:+.1f}%" if gap is not None else "")
    res = Result(ticker, metric)
    parts = []
    if abs(chg) >= SPLASH_MIN:
        parts.append(f"движение {chg:+.0f}%")
    if gap is not None and abs(gap) >= SPLASH_GAP_MIN:
        parts.append(f"last против справедливой {gap:+.0f}%")
    if parts:
        msg = f"{ticker}: " + "; ".join(parts)
        ex = _extras(text, ["Max size", "Volume 24h"])
        if ex:
            msg += "\n" + ex
        res.alert = Alert(f"splash:{ticker}", chan, msg)
    return res


def rule_kormushka(chan: str, text: str) -> Optional[Result]:
    """Mexc Price | Kormushka: 'AKE +8.91% in 1m 56s! MEXC | Limit ~$29816'."""
    m = re.match(r"\s*(\S+)\s+(" + NUM + r")\s*%\s+in\s+([^!\n]+)", text)
    if not m:
        return None
    ticker, pct, dur = m.group(1).lstrip("$"), float(m.group(2)), m.group(3).strip()
    res = Result(ticker, f"{pct:+.1f}% in {dur}")
    if abs(pct) >= KORM_MIN:
        msg = f"{ticker}: {pct:+.1f}% за {dur}"
        lim = re.search(r"Limit\s*~?\$?([\d,]+)", text)
        if lim:
            msg += f"\nЛимит ~${lim.group(1)}"
        res.alert = Alert(f"korm:{ticker}", "MEXC Kormushka", msg)
    return res


DEX_RE = re.compile(r"#(\w+)\s+Spread\s+(" + NUM + r")\s*%", re.I)


def rule_dex(chan: str, text: str) -> Optional[Result]:
    """Mexc Dex Spread Tracker Alerts: 'SHORT? #BATON Spread -10.04% detected'. 'Aligned' = закрылся."""
    if "aligned" in text.lower():
        return None
    m = DEX_RE.search(text)
    if not m:
        return None
    val = abs(float(m.group(2)))
    ticker = m.group(1)
    res = Result(ticker, f"dex spread {val:.2f}%")
    if val >= DEX_MIN:
        res.alert = Alert(f"dex:{ticker}", "MEXC ↔ DEX", f"{ticker}: спред MEXC/DEX {m.group(2)}%")
    return res


def rule_neutral(chan: str, text: str) -> Optional[Result]:
    """neutral trade: пушим только 'Начало проторговки / можно смотреть нейтральную сетку'."""
    if not re.search(r"Начало проторговки|нейтральн\w+ сетк", text, re.I):
        return None
    t = re.search(r"\b([A-Z0-9]+_USDT)\b", text)
    ticker = t.group(1) if t else _first_ticker(text)
    c = re.search(r"Коридор:\s*([\d.]+)\s*%", text)
    s = re.search(r"Смещение:\s*([\d.]+)\s*%", text)
    msg = f"{ticker}: начало проторговки"
    if c:
        msg += f", коридор {c.group(1)}%"
    if s:
        msg += f", смещение {s.group(1)}%"
    msg += "\nМожно смотреть нейтральную сетку"
    return Result(ticker, "начало проторговки", Alert(f"neutral:{ticker}", "Neutral trade", msg))


# порядок важен: проверяется по подстроке названия канала (в нижнем регистре)
RULES: list[tuple[str, Callable[[str, str], Optional[Result]]]] = [
    ("arbitrage nda", rule_nda),
    ("futures spreads", rule_spreads),
    ("mexc dex spread", rule_dex),
    ("gate | feed", rule_feed),
    ("fair / last", rule_fair_last),
    ("kormushka", rule_kormushka),
    ("splash", rule_splash),
    ("neutral trade", rule_neutral),
]


def evaluate(chan_title: str, text: str) -> Optional[Result]:
    t = chan_title.lower()
    for needle, rule in RULES:
        if needle in t:
            return rule(chan_title, text)
    return None


# ---------- антидубль, приоритет, отправка ----------

_seen: dict[str, float] = {}


def _is_dup(key: str) -> bool:
    now = time.time()
    if len(_seen) > 500:
        for k in [k for k, ts in _seen.items() if now - ts > DEDUPE_SECONDS]:
            _seen.pop(k, None)
    last = _seen.get(key)
    if last is not None and now - last < DEDUPE_SECONDS:
        return True
    _seen[key] = now
    return False


def _priority() -> int:
    h = datetime.now(TZ).hour
    if QUIET_FROM > QUIET_TO:
        quiet = h >= QUIET_FROM or h < QUIET_TO
    else:
        quiet = QUIET_FROM <= h < QUIET_TO
    return QUIET_PRIORITY if quiet else 0


def _push_sync(title: str, message: str, url: str, priority: int) -> None:
    data = urllib.parse.urlencode({
        "token": PUSHOVER_TOKEN, "user": PUSHOVER_USER,
        "title": title, "message": message,
        "priority": priority, "url": url, "url_title": "Открыть пост",
    }).encode()
    req = urllib.request.Request("https://api.pushover.net/1/messages.json", data=data)
    with urllib.request.urlopen(req, timeout=10) as r:
        r.read()


async def handle_post(chan_title: str, text: str, link: str = "") -> Optional[Alert]:
    res = evaluate(chan_title, text)
    if res is None:
        return None
    if LOG_ALL:
        log.info("PARSER_SEEN | %s | %s | %s", chan_title, res.ticker, res.metric)
    alert = res.alert
    if alert is None or _is_dup(alert.key):
        return None
    log.info("PARSER_ALERT mode=%s | %s | %s", MODE, chan_title, alert.message.replace("\n", " | "))
    if MODE == "live" and PUSHOVER_TOKEN and PUSHOVER_USER:
        try:
            await asyncio.to_thread(_push_sync, alert.title, alert.message, link, _priority())
        except Exception:
            log.exception("Pushover failed")
    return alert


# ---------- подключение к Telethon ----------

def _link(event, username: Optional[str]) -> str:
    if username:
        return f"https://t.me/{username}/{event.id}"
    cid = str(event.chat_id)
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/{event.id}"
    return ""


def register(client) -> None:
    from telethon import events  # импорт здесь, чтобы самопроверка работала без Telethon

    meta: dict[int, tuple[str, Optional[str]]] = {}

    @client.on(events.NewMessage(incoming=True))
    async def _on_message(event):
        try:
            if not (event.is_channel or event.is_group):
                return
            text = event.raw_text or ""
            if not text:
                return
            info = meta.get(event.chat_id)
            if info is None:
                chat = await event.get_chat()
                info = (getattr(chat, "title", "") or "", getattr(chat, "username", None))
                meta[event.chat_id] = info
            title, username = info
            if not any(n in title.lower() for n, _ in RULES):
                return
            await handle_post(title, text, _link(event, username))
        except Exception:
            log.exception("parser_alerts handler failed")


# ---------- самопроверка: python parser_alerts.py ----------
# Тексты ниже воспроизведены со скринов реальных постов.

if __name__ == "__main__":
    nda_oura = """OURA
         | Price   | Funding
----------------------------
BINANCE | 48.46   | +0.0050%
        |         | 55m (8h)
----------------------------
BITGET  | 48.861  | +0.0050%
        |         | 55m (8h)
----------------------------
KUCOIN  | 48.60   | +0.0050%
        |         | 55m (8h)
----------------------------
BYBIT   | 48.39   | +0.0000%
        |         | 55m (8h)
----------------------------
GATE    | 7657.76 | +0.0000%
        |         | 55m (8h)
----------------------------
OKX     | 48.502  | +0.0000%
        |         | 55m (8h)

🟢BYBIT→🔴GATE: 14554.60%
🟢BINANCE→🔴GATE: 14551.58%
📈Посмотреть график спреда"""
    nda_kimchi = """KIMCHI
       | Price    | Funding
---------------------------
GATE  | 0.002328 | +0.0050%
      |          | 7m (4h)
---------------------------
MEXC  | 0.002058 | -0.0135%
      |          | 7m (4h)

🟢MEXC→🔴GATE: 11.40%
📈Посмотреть график спреда"""
    spreads_huma = """#HUMA | Spread: 59.88%
📌 HUMA_USDT (COPY: HUMA)

Short GATE : $0.127600000
Long OURBIT : $0.116600000

Funding GATE : -1.86%
Additional Info:
OURBIT Max Size : $1702
F/Interval GATE : 4H | 20:00 UTC"""
    spreads_bard = spreads_huma.replace("HUMA", "BARD").replace("59.88", "9.43")
    spreads_aligned = "#AKE | Spread: 5.11% 📌 AKE_USDT\n#AKE aligned in 179:49\nPrice MEXC : $0.006015"
    dex_det = "🔴🔴 SHORT? #BATON Spread -10.04% detected\nPrice DEX $0.006227\nPrice MEXC $0.006239"
    dex_big = dex_det.replace("BATON", "FOO").replace("-10.04", "-25.00")
    dex_aligned = "✅ #BATON #SOLANA Aligned in 37 sec\nPrice DEX $0.006227\nSpread: -0.19%\nPNL: 1.89%"
    feed_play = ("$PLAY +5.90% in 15 secs\n\nprice: $0.00779999 → $0.00825999\nfair: $0.00778899\n\n"
                 "limit: $10,000\nvol: $1,624,558\n\nfunding: +0.0176% in 1 mins\nshorts: 158 (49%) long: 166 (51%)")
    feed_needle = ("$OURA +1200.00% in 4 secs\n\nprice: $55 → $5000\nfair: $54.78\n\nlimit: $5,000\nvol: $12,000")
    fair_dexe = ("DEXE\n\nFAIR > LAST +9.8%\n\nFAIR → 3.44\nLAST → 3.104\n\nMax size: 9,932$\nMax lev: 20x\n"
                 "24h vol: 90.7M$\n\n⏱ Изменение\n10сек: +7.2%\n\n⚠ Сравнялся")
    splash_gang = ("🟢 $GANG\nИзм.: +12.45%\n\nMAX: 0.004624\nMIN: 0.004112\n\nMax size: $2.17k\n\n"
                   "Now last price: $0.004624\nСправедл. price: $0.004619\n\nSpot price: $0.004547\n\n"
                   "⏱ 4.7 min\nVolume 24h: $3.22m\n\n🕒 15:02:30 UTC+3")
    splash_gap = splash_gang.replace("$0.004619", "$0.003000")
    splash_big = splash_gang.replace("+12.45", "+240.00")
    aster_agt = ("🚀 $AGT\nДвижение: +22.74% за 10.0м\n\nMAX: 0.029580\nMIN: 0.024020\n\nПоследняя: 0.029580\n"
                 "Справедливая: 0.027325 (-7.62%)\nИндексная: 0.027228\n\n🕒 08:08:08 UTC")
    aster_gap = aster_agt.replace("0.027325", "0.020000")
    korm_small = "AKE +8.91% in 1m 56s!\nMEXC | Limit ~$29816"
    korm_hot = "KIMCHI +5.43% in 6 secs! 🔥 hot for 13m +17.29% · 3 moves (↑2 ↓1)\nMEXC | Limit ~$2476"
    korm_big = "XYZ +45.00% in 9 secs!\nMEXC | Limit ~$900"
    neutral_start = ("✅ RECRUITSTOCK_USDT\nНачало проторговки\n\nКоридор: 4.50%\nСмещение: 1.35%\n\n"
                     "👉 Можно смотреть нейтральную сетку")
    neutral_move = "🟢 RECRUITSTOCK_USDT\nИзм.: +20.00%\n\nMAX: 105.15\nMIN: 79.83\nСейчас: 103.45"

    # (канал, текст, ждём ли пуш, ждём ли что пост вообще распознан)
    samples = [
        ("Futures Arbitrage NDA", nda_oura, True, True),
        ("Futures Arbitrage NDA", nda_kimchi, False, True),
        ("Futures Spreads 7%", spreads_huma, True, True),
        ("Futures Spreads 7%", spreads_bard, False, True),
        ("Futures Spreads 3%", spreads_aligned, False, False),
        ("Mexc Dex Spread Tracker Alerts", dex_det, False, True),
        ("Mexc Dex Spread Tracker Alerts", dex_big, True, True),
        ("Mexc Dex Spread Tracker Alerts", dex_aligned, False, False),
        ("Gate | Feed", feed_play, False, True),
        ("Gate | Feed", feed_needle, True, True),
        ("FAIR / LAST PRICE SPREAD", fair_dexe, True, True),
        ("splash 12% MEXC", splash_gang, False, True),
        ("splash 12% MEXC", splash_gap, True, True),
        ("splash 50% GATE", splash_big, True, True),
        ("ASTER SPLASH 22%", aster_agt, False, True),
        ("ASTER SPLASH 22%", aster_gap, True, True),
        ("Mexc Price | Kormushka", korm_small, False, True),
        ("Mexc Price | Kormushka", korm_hot, False, True),
        ("Mexc Price | Kormushka", korm_big, True, True),
        ("neutral trade", neutral_start, True, True),
        ("neutral trade", neutral_move, False, False),
        ("HyperFilter Tracker", "👋 HyperFilter — мониторинг кошельков", False, False),
    ]
    ok = True
    for chan, text, want_alert, want_seen in samples:
        r = evaluate(chan, text)
        got_alert = bool(r and r.alert)
        got_seen = r is not None
        good = got_alert == want_alert and got_seen == want_seen
        ok &= good
        info = (r.alert.message.replace("\n", " | ") if got_alert else (r.metric if r else None))
        print(("OK  " if good else "FAIL"), f"[{chan}]", "->", info)
    print("ВСЁ ХОРОШО" if ok else "ЕСТЬ ОШИБКИ")
