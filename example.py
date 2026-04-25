"""End-to-end usage example for telegramlistener.

Run:
    uv run example.py

Prerequisites:
    Copy .env.example to .env and fill in your Telegram credentials, then:
    uv sync --extra examples
"""

from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv

from telegramlistener import (
    Channel,
    SessionManager,
    TelegramListener,
    TelegramStreamedMessage,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise OSError(f"Missing required environment variable: {name}")
    return value


async def consume(queue: asyncio.Queue[TelegramStreamedMessage]) -> None:
    while True:
        msg = await queue.get()
        print(msg)
        queue.task_done()


async def main() -> None:
    manager = SessionManager(
        api_id=int(_require("TELEGRAM_API_ID")),
        api_hash=_require("TELEGRAM_API_HASH"),
        phone=_require("TELEGRAM_PHONE"),
        session_name=os.getenv("TELEGRAM_SESSION_NAME", "telegram"),
    )

    if not await manager.is_operational():
        await manager.run_manual_login()

    async with TelegramListener(session_manager=manager) as listener:
        listener.set_channels([
            "me_observer_tg",
            Channel("ajanews", language="ar"),
        ])
        consumer = asyncio.create_task(consume(listener.queue))
        try:
            await listener.start()
        finally:
            consumer.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Stopped.")
