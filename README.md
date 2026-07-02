# TelegramListener

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

Stream Telegram channel messages to an asyncio queue. One coroutine produces; you consume.

---

## Install

```bash
pip install git+https://github.com/Cerval/TelegramListener.git
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add git+https://github.com/Cerval/TelegramListener.git
```

---

## Quick Start

```python
from telegramlistener import SessionManager, TelegramListener

manager = SessionManager(api_id=..., api_hash=..., phone="+34612345678")

if not await manager.is_operational():
    await manager.run_manual_login()          # interactive once, session persists

async with TelegramListener(session_manager=manager) as listener:
    listener.set_channels(["cnn", "ajanews"])
    await listener.start()                    # blocks; messages arrive on listener.queue
```

---

## Consuming Messages

`listener.queue` is a standard `asyncio.Queue[TelegramStreamedMessage]`. Run a consumer concurrently:

```python
async def consume(queue: asyncio.Queue) -> None:
    while True:
        msg = await queue.get()
        print(f"{msg.source}: {msg.text}")
        queue.task_done()

async with TelegramListener(session_manager=manager) as listener:
    listener.set_channels(["cnn", "ajanews"])
    consumer = asyncio.create_task(consume(listener.queue))
    try:
        await listener.start()
    finally:
        consumer.cancel()
```

---

## API Reference

### `SessionManager`

Manages Telethon session lifecycle: validation, interactive login, and cleanup.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_id` | `int` | — | Telegram API ID from [my.telegram.org](https://my.telegram.org) |
| `api_hash` | `str` | — | Telegram API hash |
| `phone` | `str` | — | Phone number in international format, e.g. `"+34612345678"` |
| `session_name` | `str` | `"telegram"` | Filename stem for the `.session` file |
| `session_dir` | `Path \| None` | `~/.cache/telegramlistener/` | Directory for session files |

| Method | Returns | Description |
|--------|---------|-------------|
| `await is_operational()` | `bool` | `True` if a valid session exists on disk |
| `await run_manual_login()` | `None` | Interactive terminal login; persists session |
| `await get_authorized_client()` | `TelegramClient` | Connected client; raises `SessionError` if no session |

---

### `TelegramListener`

Streams new messages from configured channels into `listener.queue`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_manager` | `SessionManager` | — | Authorized session manager |
| `timezone_name` | `str` | `"UTC"` | IANA timezone for message timestamps |

| Attribute / Method | Description |
|--------------------|-------------|
| `queue` | `asyncio.Queue[TelegramStreamedMessage]` — consume from here |
| `set_channels(channels)` | Configure channels before or between runs |
| `await start()` | Start and block. Reconnects automatically on failures |
| `await aclose()` | Graceful shutdown, disconnects client |
| `stop()` | Fire-and-forget shutdown (use `aclose()` when you can await) |
| `async with listener:` | Calls `aclose()` on exit automatically |

---

### `TelegramStreamedMessage`

Immutable message object. Every instance has a time-sortable ULID `id`.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Time-sortable ULID (26 chars), auto-generated |
| `timestamp` | `int` | Unix timestamp (UTC seconds) of the original message |
| `source` | `str` | Human-readable channel title |
| `source_id` | `int` | Numeric Telegram chat identifier |
| `text` | `str` | Sanitized text — unicode-fixed, emoji-stripped |

---

### Channels

`set_channels` accepts a list of channel usernames as plain strings (with or without
a leading `@`). Example:

```python
listener.set_channels(["cnn", "ajanews"])
```

---

### Exceptions

All library exceptions inherit from `TelegramListenerError`.

| Exception | When |
|-----------|------|
| `TelegramListenerError` | Base class — catch this to handle any library error |
| `SessionError` | Session missing, revoked, or login failed |
| `ConfigurationError` | `start()` called before `set_channels()` |

---

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_API_ID` | Yes | From [my.telegram.org](https://my.telegram.org) |
| `TELEGRAM_API_HASH` | Yes | From [my.telegram.org](https://my.telegram.org) |
| `TELEGRAM_PHONE` | Yes | International format, e.g. `+34612345678` |
| `TELEGRAM_SESSION_NAME` | No | Defaults to `telegram` |

Session files are stored at `~/.cache/telegramlistener/<session_name>.session`. The login flow runs once interactively; subsequent runs reuse the saved session automatically.

---

## Reconnection

The listener reconnects on transient failures using exponential backoff: 2 s → 4 s → 8 s → … capped at 60 s, with ±1 s jitter. It stops permanently only if:

- The Telegram session is revoked or the account is deactivated, or
- `stop()` / `aclose()` is called explicitly.

---

## Logging

The library is silent by default (uses `NullHandler`). Enable logging at any level:

```python
import logging
logging.getLogger("telegramlistener").setLevel(logging.DEBUG)
```

---

## Running the Example

```bash
uv sync --extra examples
cp .env.example .env   # fill in your credentials
uv run example.py
```

---

## Contributing

1. Fork, create a branch.
2. `ruff check src/ example.py` — must pass.
3. Open a pull request.

---

## License

[MIT](LICENSE)
