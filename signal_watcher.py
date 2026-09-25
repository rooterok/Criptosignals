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
from telethon.tl.types import InputReplyToMessage

from evaluator import evaluate
from notifier import send_push
from prefilter import looks_like_signal

load_dotenv()

# Сколько последних сообщений чата хранить как контекст для LLM-оценки —
# сигналы в этом чате часто растянуты на несколько сообщений подряд
# (сначала "Short BUN_USDT", затем отдельным сообщением твх/сайз и т.д.).
CONTEXT_SIZE = int(os.getenv("CONTEXT_SIZE", "6"))
_recent_messages: deque[str] = deque(maxlen=CONTEXT_SIZE)

# Соответствие id сообщения в исходном чате -> id его копии в архивном чате.
# Нужно, чтобы если в исходном чате сообщение было ответом на другое, копия в
# архиве тоже была ответом (на копию того, другого сообщения, а не абы на
# что). Храним только последние ARCHIVE_MAP_SIZE пар, чтобы словарь не рос
# бесконечно при долгой работе — старые сообщения теряют этот линк, что
# нормально (ответ на что-то настолько старое — редкость, а если он всё же
# случится, копия просто уйдёт в общую тему, без реплая).
ARCHIVE_MAP_SIZE = 2000
_archived_message_ids: dict[int, int] = {}


def _remember_archived_id(source_id: int, archived_id: int) -> None:
    _archived_message_ids[source_id] = archived_id
    if len(_archived_message_ids) > ARCHIVE_MAP_SIZE:
        oldest_key = next(iter(_archived_message_ids))
        del _archived_message_ids[oldest_key]

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
    """Пытается найти чат по username, числовой id или названию."""
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


async def handle_message(event, chat_title: str, client: TelegramClient, archive_entity=None, archive_topic_id: int | None = None):
    text = event.raw_text or ""

    # Дублируем КАЖДОЕ сообщение с текстом в архивный чат (если он задан),
    # независимо от того, похоже оно на сигнал или нет — это не пересылка
    # (forward), а обычная отправка нового сообщения с тем же текстом, так
    # что запрет пересылки в исходном чате тут ни при чём. Если задан
    # ARCHIVE_TOPIC_ID — отправляем в конкретную тему форум-чата: reply_to
    # на id темы кладёт сообщение именно в неё (так же работает и в ботах
    # через message_thread_id).
    #
    # Если 0—