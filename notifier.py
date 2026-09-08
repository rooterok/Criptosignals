"""
Отправка пуш-уведомления через Pushover.

Priority=2 (emergency) — единственный режим Pushover, который реально
шансует разбудить человека: уведомление повторяется со звуком каждые
PUSHOVER_RETRY_SECONDS, пока не будет подтверждено в приложении, вплоть
до PUSHOVER_EXPIRE_SECONDS. Для этого он обязателен вместе с retry/expire.
"""

import logging
import os

import requests

logger = logging.getLogger("signal_watcher")

PUSHOVER_URL = "https://api.pushover.net/1/messages.json"


def send_push(title: str, message: str, url: str | None = None) -> bool:
    token = os.getenv("PUSHOVER_TOKEN")
    user = os.getenv("PUSHOVER_USER")
    if not token or not user:
        logger.error("PUSHOVER_TOKEN / PUSHOVER_USER не заданы — уведомление не отправлено")
        return False

    priority = int(os.getenv("PUSHOVER_PRIORITY", "2"))

    payload = {
        "token": token,
        "user": user,
        "title": title,
        "message": message[:1024],
        "priority": priority,
        "sound": os.getenv("PUSHOVER_SOUND", "persistent"),
    }
    if url:
        payload["url"] = url
        payload["url_title"] = "Открыть чат"

    if priority == 2:
        payload["retry"] = int(os.getenv("PUSHOVER_RETRY_SECONDS", "60"))
        payload["expire"] = int(os.getenv("PUSHOVER_EXPIRE_SECONDS", "3600"))

    try:
        resp = requests.post(PUSHOVER_URL, data=payload, timeout=15)
        resp.raise_for_status()
        logger.info("Пуш отправлен: %s", title)
        return True
    except requests.RequestException:
        logger.exception("Не удалось отправить пуш через Pushover")
        return False
