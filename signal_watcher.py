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
import random
import re
import sys
import time
from collections import deque
from datetime import datetime

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl import types as tl_types
from telethon.tl.functions.messages import SendMediaRequest, SendMessageRequest
from telethon.tl.types import InputReplyToMessage

from evaluator import evaluate
from notifier import send_push
from prefilter import looks_like_signal

load_dotenv()

CONTEXT_SIZE = int(os.getenv("CONTEXT_SIZE", "6"))
_recent_messages: deque[str] = deque(maxlen=CONTEXT_SIZE)

ARCHIVE_MAP_SIZE = 2000
_archived_message_ids: dict[int, int] = {}

_push_muted_until: float | None = None
DEFAULT_MUTE_SECONDS = int(os.getenv("DEFAULT_MUTE_SECONDS", str(60 * 60)))


def _parse_duration_to_seconds(text: str) -> int | None:
    """Парсит '1h', '90m', '2ч', '30 мин', '1ч30м' и т.п. в секунды.
    Пустая строка/непонятный формат -> None."""
    text = text.strip().lower().replace(" ", "")
    if not text:
        return None

    total = 0
    matched = False
    for value, unit in re.findall(r"(\d+)\s*(ч|час\w*|h|м|мин\w*|m)?", text):
        if not value:
            continue
        matched = True
        n = int(value)
        unit = unit or "h"
        if unit.startswith(("м", "m")):
            total += n * 60
        else:
            total += n * 3600
    return total if matched and total > 0 else None


def _remember_archived_id(source_id: int, archived_id: int) -> None:
    _archived_message_ids[source_id] = archived_id
    if len(_archived_message_ids) > ARCHIVE_MAP_SIZE:
        oldest_key = next(iter(_archived_message_ids))
        del _archived_message_ids[oldest_key]


async def _send_as_reply_to_archived(
    client: TelegramClient,
    entity,
    text: str,
    archive_topic_id: int,
    archived_parent_id: int,
    photo_bytes: bytes | None = None,
):
    """Отправляет сообщение (текст или фото) НАСТОЯЩИМ ответом на конкретное
    уже заархивированное сообщение (archived_parent_id), оставаясь при этом
    в нужной теме форума (archive_topic_id).

    client.send_message()/client.send_file() тут не подходят: их reply_to
    проходит через telethon.utils.get_message_id(), который принимает
    только int или Message и падает с TypeError на InputReplyToMessage —
    а чтобы ответить именно на сообщение ВНУТРИ темы (а не на само
    открывающее сообщение темы), протоколу нужен InputReplyToMessage сразу
    с reply_to_msg_id И top_msg_id. Высокоуровневый API так не умеет,
    поэтому собираем запрос вручную через сырое Telegram API (как это и
    делает Telethon внутри send_message/send_file, только с нужным
    reply_to)."""
    input_entity = await client.get_input_entity(entity)
    reply_to = InputReplyToMessage(
        reply_to_msg_id=archived_parent_id,
        top_msg_id=archive_topic_id,
    )
    random_id = random.randrange(-(2**63), 2**63 - 1)

    if photo_bytes is not None:
        uploaded = await client.upload_file(photo_bytes)
        media = tl_types.InputMediaUploadedPhoto(file=uploaded)
        request = SendMediaRequest(
            peer=input_entity,
            media=media,
            message=text or "",
            reply_to=reply_to,
            random_id=random_id,
        )
    else:
        request = SendMessageRequest(
            peer=input_entity,
            message=text,
            reply_to=reply_to,
            random_id=random_id,
        )

    result = await client(request)
    return client._get_response_message(request, result, input_entity)


LOG_FILE = os.getenv("LOG_FILE", "signal_watcher.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("signal_watcher")

WATCHDOG_INTERVAL_SECONDS = int(os.getenv("WATCHDOG_INTERVAL_SECONDS", "300"))
WATCHDOG_TIMEOUT_SECONDS = int(os.getenv("WATCHDOG_TIMEOUT_SECONDS", "30"))


async def _connection_watchdog(client: TelegramClient):
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL_SECONDS)
        try:
            await asyncio.wait_for(client.get_me(), timeout=WATCHDOG_TIMEOUT_SECONDS)
        except Exception:
            logger.exception(
                "Watchdog: Telegram не отвечает дольше %s сек — похоже, соединение "
                "молча умерло. Принудительно завершаю процесс, чтобы внешний цикл "
                "перезапустил его с чистого листа",
                WATCHDOG_TIMEOUT_SECONDS,
            )
            os._exit(1)


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
    раздел про Railway), кодируется в base64 и кладётся в переменную(ые)
    окружения TELEGRAM_SESSION_B64. При каждом старте контейнера, если
    локального файла сессии ещё нет, мы восстанавливаем его из этой
    переменной — дальше Telethon подключается уже без повторного логина.

    У Railway есть лимит на длину значения переменной (32768 символов), а
    base64 нашей сессии обычно его превышает. Поэтому, если TELEGRAM_SESSION_B64
    целиком не задан, пробуем собрать его из пронумерованных частей
    TELEGRAM_SESSION_B64_1, TELEGRAM_SESSION_B64_2, ... — каждая часть
    укладывается в лимит, а здесь мы их склеиваем обратно по порядку."""

    session_path = f"{SESSION_NAME}.session"
    if os.path.exists(session_path):
        return

    b64 = os.getenv("TELEGRAM_SESSION_B64")
    if not b64:
        parts = []
        i = 1
        while True:
            part = os.getenv(f"TELEGRAM_SESSION_B64_{i}")
            if not part:
                break
            parts.append(part)
            i += 1
        if parts:
            b64 = "".join(parts)

    if not b64:
        return

    try:
        with open(session_path, "wb") as f:
            f.write(base64.b64decode(b64))
        logger.info("Восстановил файл сессии из переменных окружения (TELEGRAM_SESSION_B64*)")
    except Exception:
        logger.exception("Не удалось восстановить сессию из переменных окружения (TELEGRAM_SESSION_B64*)")


async def resolve_chat(client: TelegramClient, chat_ref: str):
    """Пытается найти чат по username, числовому id или названию."""
    try:
        if chat_ref.lstrip("-").isdigit():
            return await client.get_entity(int(chat_ref))
        return await client.get_entity(chat_ref)
    except Exception:
        pass

    async for dialog in client.iter_dialogs():
        if dialog.name == chat_ref:
            return dialog.entity

    raise RuntimeError(
        f"Не удалось найти чат '{chat_ref}'. Проверьте TELEGRAM_CHAT в .env "
        f"(username без @, числовой id вида -1001234567890, либо точное название)."
    )


async def handle_message(event, chat_title: str, client: TelegramClient, archive_entity=None, archive_topic_id: int | None = None):
    text = event.raw_text or ""

    if archive_entity is not None and text.strip():
        try:
            reply_source_id = getattr(event.message, "reply_to_msg_id", None)
            archived_parent_id = _archived_message_ids.get(reply_source_id) if reply_source_id else None

            if archived_parent_id is not None:
                sent = await _send_as_reply_to_archived(
                    client, archive_entity, text, archive_topic_id, archived_parent_id
                )
            else:
                sent = await client.send_message(archive_entity, text, reply_to=archive_topic_id)

            _remember_archived_id(event.message.id, sent.id)
        except Exception:
            logger.exception("Не удалось продублировать сообщение в архивный чат")

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

    if _push_muted_until and time.time() < _push_muted_until:
        until_str = datetime.fromtimestamp(_push_muted_until).strftime("%H:%M")
        logger.info("Пуш подавлен (заглушено до %s): %s", until_str, title)
        return

    send_push(title=title, message=f"[{chat_title}] {message}", url=chat_link)


async def handle_mirror_message(
    event,
    client: TelegramClient,
    source_entity,
    target_entity,
    target_topic_id: int | None = None,
):
    """Простое зеркалирование БЕЗ анализа на сигналы — для дополнительных
    источников, которые нужно просто копировать в архив, а не мониторить.

    Всё уходит НОВЫМ сообщением от вашего имени (без пометки "Переслано
    от..."): картинка пересылается через send_file по её file-ссылке (без
    повторной загрузки/потери качества, просто без штампа авторства), а
    обычный текст — простой копией текста."""
    try:
        if event.message.photo:
            text = event.raw_text or ""
            await client.send_file(
                target_entity,
                event.message.photo,
                caption=text or None,
                reply_to=target_topic_id,
            )
            return

        text = event.raw_text or ""
        if not text.strip():
            return
        await client.send_message(target_entity, text, reply_to=target_topic_id)
    except Exception:
        logger.exception("Не удалось продублировать сообщение из доп. источника")


async def main():
    api_id = int(_require_env("TELEGRAM_API_ID"))
    api_hash = _require_env("TELEGRAM_API_HASH")
    chat_ref = _require_env("TELEGRAM_CHAT")
    phone = os.getenv("TELEGRAM_PHONE")

    _restore_session_from_env()
    client = TelegramClient(SESSION_NAME, api_id, api_hash)

    logger.info("Подключаюсь к Telegram...")
    await client.start(phone=phone)
    logger.info("Подключено как %s", (await client.get_me()).username or "unknown")

    chat_entity = await resolve_chat(client, chat_ref)
    chat_title = getattr(chat_entity, "title", None) or getattr(chat_entity, "username", chat_ref)
    logger.info("Слушаю чат: %s", chat_title)

    archive_ref = os.getenv("ARCHIVE_CHAT")
    archive_entity = None
    archive_topic_id = None
    if archive_ref:
        archive_entity = await resolve_chat(client, archive_ref)
        archive_title = getattr(archive_entity, "title", None) or getattr(archive_entity, "username", archive_ref)
        topic_raw = os.getenv("ARCHIVE_TOPIC_ID")
        if topic_raw:
            archive_topic_id = int(topic_raw)
            logger.info("Дублирую все сообщения чата в: %s (тема %s)", archive_title, archive_topic_id)
        else:
            logger.info("Дублирую все сообщения чата в: %s", archive_title)

    @client.on(events.NewMessage(chats=chat_entity))
    async def _handler(event):
        try:
            await handle_message(event, chat_title, client, archive_entity, archive_topic_id)
        except Exception:
            logger.exception("Ошибка при обработке сообщения")

    @client.on(events.NewMessage(chats="me", outgoing=True))
    async def _control_handler(event):
        global _push_muted_until
        text = (event.raw_text or "").strip()
        lowered = text.lower()

        try:
            if lowered.startswith("/mute"):
                arg = text[len("/mute"):].strip()
                seconds = _parse_duration_to_seconds(arg) if arg else DEFAULT_MUTE_SECONDS
                if seconds is None:
                    await event.reply("Не понял длительность. Примеры: /mute, /mute 2h, /mute 90m")
                    return
                _push_muted_until = time.time() + seconds
                until_str = datetime.fromtimestamp(_push_muted_until).strftime("%H:%M")
                logger.info("Пуши заглушены на %s сек (до %s)", seconds, until_str)
                await event.reply(f"🔇 Пуши заглушены до {until_str}")

            elif lowered.startswith("/unmute"):
                _push_muted_until = None
                logger.info("Пуши снова включены (команда /unmute)")
                await event.reply("🔔 Пуши снова включены")

            elif lowered.startswith("/mutestatus"):
                if _push_muted_until and time.time() < _push_muted_until:
                    until_str = datetime.fromtimestamp(_push_muted_until).strftime("%H:%M")
                    await event.reply(f"🔇 Пуши заглушены до {until_str}")
                else:
                    await event.reply("🔔 Пуши включены")
        except Exception:
            logger.exception("Ошибка при обработке команды управления пушами: %r", text)

    mirror2_source_ref = os.getenv("MIRROR2_SOURCE_CHAT")
    if mirror2_source_ref:
        try:
            mirror2_target_ref = os.getenv("MIRROR2_TARGET_CHAT") or archive_ref
            if not mirror2_target_ref:
                logger.error(
                    "MIRROR2_SOURCE_CHAT задан, но не задан ни MIRROR2_TARGET_CHAT, ни ARCHIVE_CHAT"
                )
            else:
                mirror2_source_entity = await resolve_chat(client, mirror2_source_ref)
                mirror2_target_entity = await resolve_chat(client, mirror2_target_ref)
                mirror2_topic_raw = os.getenv("MIRROR2_TARGET_TOPIC_ID")
                mirror2_topic_id = int(mirror2_topic_raw) if mirror2_topic_raw else None

                mirror2_source_title = getattr(mirror2_source_entity, "title", None) or getattr(
                    mirror2_source_entity, "username", mirror2_source_ref
                )
                mirror2_target_title = getattr(mirror2_target_entity, "title", None) or getattr(
                    mirror2_target_entity, "username", mirror2_target_ref
                )
                logger.info(
                    "Доп. дублирование: %s -> %s (тема %s)",
                    mirror2_source_title,
                    mirror2_target_title,
                    mirror2_topic_id,
                )

                @client.on(events.NewMessage(chats=mirror2_source_entity))
                async def _mirror2_handler(event):
                    try:
                        await handle_mirror_message(
                            event, client, mirror2_source_entity, mirror2_target_entity, mirror2_topic_id
                        )
                    except Exception:
                        logger.exception("Ошибка при дублировании доп. источника")
        except Exception:
            logger.exception(
                "Не удалось настроить доп. источник дублирования (MIRROR2_SOURCE_CHAT=%r) — "
                "проверьте, что chat id верный и аккаунт состоит в этом чате",
                mirror2_source_ref,
            )

    asyncio.create_task(_connection_watchdog(client))

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
