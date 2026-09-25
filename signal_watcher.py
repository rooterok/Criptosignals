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
    # Если исходное сообщение само было ответом на другое — и то, другое,
    # мы тоже успели продублировать (есть в _archived_message_ids) — копия
    # в архиве тоже оформляется как ответ именно на ту копию, а не просто
    # падает в общую тему. Если пары нет (например, ответ на сообщение из
    # истории до запуска скрипта) — просто уходит в тему, как раньше.
    if archive_entity is not None and text.strip():
        try:
            reply_source_id = getattr(event.message, "reply_to_msg_id", None)
            archived_parent_id = _archived_message_ids.get(reply_source_id) if reply_source_id else None

            if archived_parent_id is not None:
                reply_to = InputReplyToMessage(
                    reply_to_msg_id=archived_parent_id,
                    top_msg_id=archive_topic_id,
                )
            else:
                reply_to = archive_topic_id

            sent = await client.send_message(archive_entity, text, reply_to=reply_to)
            _remember_archived_id(event.message.id, sent.id)
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
    await client.start(phone=phone)  # при первом запуске спросит код/пароль
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

    # Доп. источник(и) только для дублирования, без анализа на сигналы —
    # MIRROR2_SOURCE_CHAT копируется в MIRROR2_TARGET_CHAT (по умолчанию
    # туда же, куда и основной архив — ARCHIVE_CHAT), в тему
    # MIRROR2_TARGET_TOPIC_ID, если она задана. Понадобится ещё один такой
    # источник — добавляйте по аналогии MIRROR3_* и т.д.
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
