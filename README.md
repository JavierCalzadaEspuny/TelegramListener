# TelegramListener

TelegramListener is a lightweight package to consume Telegram channel messages
in real time with a queue-based architecture.

It uses a dedicated `SessionManager` to keep login/session logic isolated from
the streaming `TelegramListener` runtime.

## Features

- Real-time consumption of Telegram channel messages.
- Queue-based output for easy async processing.
- Session health checks and manual login flow.
- Message normalization with `clean-text`.
- Stable message IDs with Unix timestamp + ULID.
- Structured logging ready for local and production runs.

## Install

Install from source (local development):

```bash
git clone https://github.com/JavierCalzadaEspuny/TelegramListener.git
cd TelegramListener
uv sync
```

Install directly from GitHub:

```bash
uv add git+https://github.com/JavierCalzadaEspuny/TelegramListener.git
```

Or with `pip`:

```bash
pip install git+https://github.com/JavierCalzadaEspuny/TelegramListener.git
```

## Configuration

Create a `.env` file (or copy `.env.example`) with:

```dotenv
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=your_api_hash_here
TELEGRAM_PHONE=+34123456789
TELEGRAM_SESSION_NAME=telegram
```

Variables:

- `TELEGRAM_API_ID`: Telegram API id.
- `TELEGRAM_API_HASH`: Telegram API hash.
- `TELEGRAM_PHONE`: Phone number for manual login.
- `TELEGRAM_SESSION_NAME`: Session file name prefix (optional, default `telegram`).

## Public API

```python
from telegramlistener import SessionManager, TelegramListener, TelegramStreamedMessage
```

## Quick Start

```python
import asyncio
import os

from telegramlistener import SessionManager, TelegramListener


async def consumer(listener: TelegramListener) -> None:
    while True:
        msg = await listener.queue.get()
        print(msg)
        listener.queue.task_done()


async def main() -> None:
    manager = SessionManager(
        api_id=int(os.environ["TELEGRAM_API_ID"]),
        api_hash=os.environ["TELEGRAM_API_HASH"],
        phone=os.environ["TELEGRAM_PHONE"],
        session_name=os.getenv("TELEGRAM_SESSION_NAME", "telegram"),
    )

    if not await manager.is_operational():
        await manager.run_manual_login()

    listener = TelegramListener(session_manager=manager, timezone_name="UTC")
    listener.set_channels([
        {"channel": "me_observer_tg", "language": "en"},
        {"channel": "ajanews", "language": "ar"},
    ])

    consumer_task = asyncio.create_task(consumer(listener))
    listener_task = asyncio.create_task(listener.start())

    try:
        await asyncio.gather(listener_task, consumer_task)
    finally:
        listener.stop()
        if listener.client is not None and listener.client.is_connected():
            await listener.client.disconnect()
        consumer_task.cancel()
        listener_task.cancel()


asyncio.run(main())
```

## Smoke Run

The repository includes a runnable smoke script:

```bash
uv run smoke_run.py
```

Expected behavior:

- If no valid session exists, it triggers manual login.
- Once authenticated, it listens to configured channels.
- Messages are printed and logs are written to `debug.log`.

## Project Structure

```text
src/telegramlistener/
├── __init__.py
├── listener.py
└── session_manager.py

smoke_run.py
pyproject.toml
README.md
```

## Notes

- Session files are stored in `~/.cache/telegramlistener`.
- This package does not auto-login silently on runtime reconnection.
- You control login explicitly through `SessionManager.run_manual_login()`.
