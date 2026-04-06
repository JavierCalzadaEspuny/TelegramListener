# TelegramListener

TelegramListener is a lightweight Python package for consuming Telegram channel
messages in real time using a queue-based workflow.

## What It Does

- Creates and reuses a local Telethon session.
- Listens to one or more Telegram channels in real time.
- Normalizes incoming text with clean-text.
- Emits a simple `TelegramMessage` dataclass to an async queue.
- Supports optional per-channel language metadata.
- Stores runtime session files in `~/.cache_telegramlistener` by default.

## Install

Repository links:

- Web: https://github.com/JavierCalzadaEspuny/TelegramListener
- Git clone: https://github.com/JavierCalzadaEspuny/TelegramListener.git

Install directly from GitHub:

```bash
uv add git+https://github.com/JavierCalzadaEspuny/TelegramListener.git
```

Or with pip:

```bash
pip install git+https://github.com/JavierCalzadaEspuny/TelegramListener.git
```

For local development:

```bash
git clone https://github.com/JavierCalzadaEspuny/TelegramListener.git
cd TelegramListener
uv sync
```

## Basic Use

```python
import asyncio
from telegramlistener import TelegramListener


async def main() -> None:
    listener = TelegramListener(
        api_id=123456,
        api_hash="your_api_hash",
        timezone_name="UTC",
    )

    await listener.connect()
    await listener.ingest(["@channel_one", "@channel_two"])

    while True:
        message = await listener.queue.get()
        print(message.source, message.text)


asyncio.run(main())
```

## Login Flow (run once)

Before listening, create the local session file:

```python
import asyncio
from telegramlistener.login import TelegramLoginClient


async def main() -> None:
    client = TelegramLoginClient(
        api_id=123456,
        api_hash="your_api_hash",
        phone="+34123456789",
    )
    await client.run()


asyncio.run(main())
```

## Linear Workflow

1. Run `TelegramLoginClient` once and complete Telegram auth.
2. Create `TelegramListener` with the same `api_id` and `api_hash`.
3. Call `connect()`.
4. Call `ingest(...)` with channels.
5. Consume messages from `listener.queue`.

## Project Structure

```text
src/telegramlistener/
├── __init__.py       # Public API exports
├── main.py           # TelegramListener class
├── login.py          # Session bootstrap flow
├── models.py         # TelegramMessage + path helpers
└── telegram.py       # Backward-compatible aliases

pyproject.toml        # Package metadata and dependencies
.gitignore            # Python and runtime cache ignores
README.md             # Documentation
```

## Notes

- The package is intentionally small and explicit for easy maintenance.
- Runtime sessions are stored in `~/.cache_telegramlistener/.cache_telegram_sessions`.
- `telegram.py` remains as a compatibility layer for existing imports.
