"""Telegram login and session management."""

import asyncio
from pathlib import Path

from telethon import TelegramClient

CACHE_DIR = Path.home() / ".cache" / "telegramlistener"
SESSION_PATH = CACHE_DIR / "telegram"


def _ensure_cache_dir() -> None:
    """Create the session cache directory."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def session_file_path() -> Path:
    """Return the Telethon session file path."""
    _ensure_cache_dir()
    return Path(f"{SESSION_PATH}.session")


async def telegram_login(api_id: int, api_hash: str, phone: str) -> None:
    """Authenticate and persist only the Telegram session."""
    _ensure_cache_dir()
    session_file = session_file_path()
    session_file.unlink(missing_ok=True)

    client = TelegramClient(str(SESSION_PATH), api_id, api_hash)
    await client.start(phone=phone)

    me = await client.get_me()
    print(f"Logged in as: {me.first_name} (@{me.username})")
    await client.disconnect()


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv

    load_dotenv()
    api_id = int(os.getenv("TELEGRAM_API_ID", "0"))
    api_hash = os.getenv("TELEGRAM_API_HASH", "")
    phone = os.getenv("TELEGRAM_PHONE", "")

    if not all([api_id, api_hash, phone]):
        raise ValueError("Missing TELEGRAM_API_ID, TELEGRAM_API_HASH, or TELEGRAM_PHONE")

    asyncio.run(telegram_login(api_id, api_hash, phone))

