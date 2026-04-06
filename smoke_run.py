"""Smoke test: simple real-time listener."""

import asyncio
import os

from dotenv import load_dotenv

from telegramlistener.listener import TelegramListener

load_dotenv()

TELEGRAM_API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH", "")
TELEGRAM_PHONE = os.getenv("TELEGRAM_PHONE", "")
CHANNELS = [
    {"channel": "me_observer_tg", "language": "en"},
    {"channel": "ajanews", "language": "ar"},
]

if not all([TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_PHONE]):
    raise ValueError("Missing TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_PHONE")


async def consume_messages(listener):
    """Print messages from queue."""
    while True:
        msg = await listener.queue.get()
        print("\n" + "=" * 60)
        print(msg)
        print("=" * 60)


async def main():
    """Run listener."""
    listener = TelegramListener(
        api_id=TELEGRAM_API_ID,
        api_hash=TELEGRAM_API_HASH,
        phone=TELEGRAM_PHONE,
        timezone_name="UTC",
    )
    await listener.connect()
    await listener.ingest(CHANNELS)
    print(f"Listening to: {CHANNELS}\n")

    consumer = asyncio.create_task(consume_messages(listener))
    runner = asyncio.create_task(listener.run())

    try:
        await asyncio.gather(consumer, runner)
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        await listener.disconnect()
        consumer.cancel()
        runner.cancel()


if __name__ == "__main__":
    asyncio.run(main())
