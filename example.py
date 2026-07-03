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
from contextlib import suppress

from dotenv import load_dotenv

from telegramlistener import (
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


def _detect_image_format(image_bytes: bytes) -> str:
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "JPEG"
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    return "unknown"


def _describe_images(msg: TelegramStreamedMessage) -> None:
    if not msg.image_bytes_list:
        return

    print(f"    Images: {len(msg.image_bytes_list)}")
    for index, image_bytes in enumerate(msg.image_bytes_list, start=1):
        image_format = _detect_image_format(image_bytes)
        print(f"    [{index}] Format: {image_format}, Size: {len(image_bytes)} bytes")


async def consume(queue: asyncio.Queue[TelegramStreamedMessage | None]) -> None:
    while True:
        msg = await queue.get()
        if msg is None:
            queue.task_done()
            break

        print(msg)
        _describe_images(msg)
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
            "testosint01"
        ])
        consumer = asyncio.create_task(consume(listener.queue))
        try:
            await listener.start()
        finally:
            consumer.cancel()
            with suppress(asyncio.CancelledError):
                await consumer


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Stopped.")
