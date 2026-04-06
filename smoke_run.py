"""Executable smoke runner for end-to-end Telegram listener verification."""

import asyncio
import logging
import os

from dotenv import load_dotenv

from telegramlistener.listener import TelegramListener
from telegramlistener.session_manager import SessionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("smoke_run")

load_dotenv()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE", "")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_NAME", "telegram")

CHANNELS = [
    {"channel": "me_observer_tg", "language": "en"},
    {"channel": "ajanews", "language": "ar"},
]


async def consume_messages(listener: TelegramListener) -> None:
    """
    Consume normalized messages from the listener queue and print them.

    Parameters
    ----------
    listener : TelegramListener
        Active listener instance exposing an async message queue.

    Returns
    -------
    None
        Runs continuously until the task is cancelled.
    """
    while True:
        msg = await listener.queue.get()
        print("=" * 80)
        print(msg)
        print("=" * 80)
        print()
        listener.queue.task_done()


async def main() -> None:
    """
    Run an end-to-end smoke flow for session validation and message streaming.

    Returns
    -------
    None
        Runs until interrupted and then performs graceful shutdown.

    Raises
    ------
    ValueError
        Raised when required Telegram environment variables are missing.
    """
    if not all([TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE]):
        raise ValueError("Missing TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_PHONE")

    manager = SessionManager(
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        phone=TELEGRAM_PHONE,
        session_name=SESSION_NAME,

    )

    # Manual login only when no valid session exists.
    if not await manager.is_operational():
        logger.warning("No valid session found. Starting manual login setup.")
        await manager.run_manual_login()

    listener = TelegramListener(session_manager=manager, timezone_name="UTC")
    listener.set_channels(CHANNELS)
    logger.info("Monitoring channels: %s", CHANNELS)

    consumer_task = asyncio.create_task(consume_messages(listener))
    listener_task = asyncio.create_task(listener.start())

    try:
        await asyncio.gather(listener_task, consumer_task)
    finally:
        listener.stop()
        if listener.client is not None and listener.client.is_connected():
            await listener.client.disconnect()
        consumer_task.cancel()
        listener_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user.")