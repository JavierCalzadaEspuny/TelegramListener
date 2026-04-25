# TelegramListener — Architecture Reference

This document is a complete reference for the `telegramlistener` library. It is intended to give a future agent (or developer) a full understanding of how the codebase works without needing to read the source first.

---

## Purpose

`telegramlistener` is a Python library that listens to public Telegram channels in real time and streams their messages into an `asyncio.Queue`. Consumers read `TelegramStreamedMessage` objects from the queue at their own pace. The library handles session authentication, reconnection with exponential backoff, text sanitization, and backpressure.

---

## Repository layout

```
TelegramListener/
├── src/telegramlistener/
│   ├── __init__.py        # Public API surface
│   ├── _listener.py       # TelegramListener — core streaming class
│   ├── _session.py        # SessionManager — auth lifecycle
│   ├── _models.py         # Channel, TelegramStreamedMessage
│   └── _exceptions.py     # TelegramListenerError, SessionError, ConfigurationError
├── example.py             # End-to-end usage script
├── pyproject.toml         # Package metadata, deps, tool config
└── .env.example           # Required environment variables
```

All public symbols are re-exported from `__init__.py`:
`Channel`, `SessionManager`, `TelegramListener`, `TelegramStreamedMessage`, `TelegramListenerError`, `SessionError`, `ConfigurationError`.

---

## Data models (`_models.py`)

### `Channel`

A frozen dataclass representing one Telegram channel to monitor.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | `str` | required | Channel username. Leading `@` is stripped automatically. Always stored lowercase. |
| `language` | `str` | `"unknown"` | BCP-47 language tag for downstream routing (e.g. `"ar"`, `"en"`). |

`Channel("@AJANews", language="AR")` stores `name="ajanews"`, `language="ar"`.

Plain strings passed to `set_channels()` are auto-converted to `Channel(name=s, language="unknown")`.

---

### `TelegramStreamedMessage`

A frozen dataclass produced by `TelegramListener` for every incoming message.

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | `int` | Unix timestamp (UTC seconds) of the original Telegram message. |
| `source` | `str` | Human-readable channel title (e.g. `"Al Jazeera"`). |
| `source_id` | `int` | Numeric Telegram chat identifier. Negative for channels/supergroups. |
| `text` | `str` | Sanitized text: unicode-fixed (`ftfy`) and emoji-stripped (`emoji`). |
| `language` | `str` | Taken from the matching `Channel.language`, or `"unknown"`. |
| `id` | `str` | Auto-generated time-sortable ULID (26 chars). Not set via `__init__`. |

`id` is created in `__post_init__` via `ulid.ULID()`. Because ULIDs embed a millisecond timestamp, messages can be sorted by `id` to recover arrival order.

The `__repr__` truncates `text` to 50 characters for readability.

---

## Exceptions (`_exceptions.py`)

```
TelegramListenerError          ← catch-all base
├── SessionError               ← missing/revoked session; call run_manual_login()
└── ConfigurationError         ← misconfigured listener; e.g. start() before set_channels()
```

---

## Session management (`_session.py`)

### `SessionManager`

Owns all authentication logic. `TelegramListener` delegates auth entirely to this class.

**Constructor parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `api_id` | `int` | required | From https://my.telegram.org |
| `api_hash` | `str` | required | From https://my.telegram.org |
| `phone` | `str` | required | International format, e.g. `"+34612345678"` |
| `session_name` | `str` | `"telegram"` | Stem of the `.session` file |
| `session_dir` | `Path \| None` | `~/.cache/telegramlistener/` | Directory for session files |

Session files are Telethon SQLite databases. The full path on disk is `{session_dir}/{session_name}.session`. The directory is created automatically if missing.

**Key methods:**

#### `is_operational() -> bool` (async)

1. Returns `False` immediately if the `.session` file does not exist.
2. Connects a fresh `TelegramClient` and calls `is_user_authorized()`.
3. If authorization returns `False`, or a fatal error (`AuthKeyDuplicatedError`, `AuthKeyUnregisteredError`, `UserDeactivatedError`) is raised, the session file is deleted (`_cleanup()`) and `False` is returned.
4. Any other transient exception returns `False` without deleting the file.
5. Returns `True` only when the session is confirmed valid.

#### `run_manual_login()` (async)

Interactive terminal login flow. Wraps `TelegramClient.start(phone=...)`, which handles SMS code and optional 2FA password prompts. Call this **once** before the first `TelegramListener.start()`. If the session is already valid, it is a no-op. On failure, the session file is cleaned up and `SessionError` is raised.

#### `get_authorized_client() -> TelegramClient` (async)

Returns a **connected** `TelegramClient` for the existing session. Raises `SessionError` if no valid session exists. The caller is responsible for calling `client.disconnect()` when done.

#### `session_file -> Path` (property)

Read-only path to the `.session` file on disk.

---

## Core listener (`_listener.py`)

### `TelegramListener`

Registers a Telethon event handler, feeds messages into an `asyncio.Queue`, and manages the connection lifecycle.

**Constructor parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_manager` | `SessionManager` | required | An authorized session manager |
| `timezone_name` | `str` | `"UTC"` | IANA timezone for normalizing timestamps |
| `queue_maxsize` | `int` | `0` (unbounded) | Maximum messages buffered in `queue` |

**Public attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `queue` | `asyncio.Queue[TelegramStreamedMessage \| None]` | Consumers read from this. `None` is the shutdown sentinel. |

---

### Lifecycle

```
set_channels(...)          ← configure which channels to monitor
        │
        ▼
    start()                ← blocks; registers handler, runs reconnect loop
        │
   (running)
        │
  stop() / aclose()        ← graceful shutdown
```

**`set_channels(channels: list[str | Channel])`**

Must be called before `start()`. Stores the channel list; strings are promoted to `Channel` objects. Can be called multiple times to reconfigure before starting.

**`start()` (async, blocking)**

1. Raises `ConfigurationError` if no channels were set.
2. Raises `SessionError` if `SessionManager.is_operational()` returns `False`.
3. Obtains an authorized client via `SessionManager.get_authorized_client()`.
4. Registers `_on_message` as a Telethon `NewMessage` event handler, scoped to the configured channel usernames.
5. Enters the reconnect loop:
   - Calls `client.run_until_disconnected()`.
   - On **fatal errors** (`AuthKeyDuplicatedError`, `AuthKeyUnregisteredError`, `UserDeactivatedError`): logs and exits immediately.
   - On **any other exception**: computes backoff delay = `min(60, 2^attempt) + jitter(0–1 s)`, waits, then checks `is_operational()` again before reconnecting.
   - Exits the loop when `stop()` / `aclose()` sets `_stop_event`.
6. Puts `None` (the sentinel) on the queue so consumers know no more messages will arrive.

**Reconnect backoff:**

| Attempt | Base delay | Cap |
|---------|-----------|-----|
| 0 | 1 s | — |
| 1 | 2 s | — |
| 2 | 4 s | — |
| … | 2^n s | 60 s |

Each delay also has `random.uniform(0, 1)` seconds of jitter to avoid thundering herds.

**`stop()`** (sync)

Sets `_stop_event` and fires `client.disconnect()` as a background task via `asyncio.ensure_future`. Returns immediately — shutdown is asynchronous. Use when you cannot `await`.

**`aclose()`** (async)

Sets `_stop_event` and `await`s `client.disconnect()`. Guarantees the client is disconnected before returning. Prefer this over `stop()`. Automatically called by the `async with` context manager.

**Context manager:**

```python
async with TelegramListener(session_manager=manager) as listener:
    ...
# aclose() is called automatically on exit
```

---

### Message handler (`_on_message`)

Called by Telethon for every new message in the monitored channels.

1. Strips and sanitizes text via `_sanitize()` (unicode fix + emoji removal). Empty results are discarded.
2. Looks up channel metadata (`title`, `language`) in `_chat_meta` (a `dict[int, tuple[str, str]]` keyed by `chat_id`). On first message from a chat, fetches the chat object from Telegram and caches it.
3. Language is resolved by matching the channel's `username` against the configured `Channel` list. Defaults to `"unknown"` if no match.
4. Constructs a `TelegramStreamedMessage` and calls `queue.put_nowait(msg)`.
5. If the queue is full (`asyncio.QueueFull`), the message is **dropped** (not blocked) and a warning is logged. This preserves the Telethon event loop.
6. Any unhandled exception is caught and logged; the handler never raises.

---

### Text sanitization

```python
def _sanitize(text: str) -> str:
    return emoji.replace_emoji(ftfy.fix_text(text), replace="").strip()
```

- `ftfy.fix_text`: fixes mojibake, wrong encoding, bad Unicode.
- `emoji.replace_emoji(..., replace="")`: removes all emoji characters.
- `.strip()`: trims surrounding whitespace.

---

## Public API surface

```python
from telegramlistener import (
    Channel,
    SessionManager,
    TelegramListener,
    TelegramStreamedMessage,
    TelegramListenerError,
    SessionError,
    ConfigurationError,
)
```

---

## Typical usage pattern

```python
import asyncio
from telegramlistener import Channel, SessionManager, TelegramListener

async def consume(queue):
    while True:
        msg = await queue.get()
        if msg is None:          # shutdown sentinel
            break
        print(msg)
        queue.task_done()

async def main():
    manager = SessionManager(
        api_id=12345,
        api_hash="abc...",
        phone="+34612345678",
    )

    # First run only: interactive SMS/2FA login
    if not await manager.is_operational():
        await manager.run_manual_login()

    async with TelegramListener(session_manager=manager, queue_maxsize=1000) as listener:
        listener.set_channels([
            "cnn",
            Channel("ajanews", language="ar"),
        ])
        consumer = asyncio.create_task(consume(listener.queue))
        try:
            await listener.start()   # blocks until stopped or session invalid
        finally:
            consumer.cancel()

asyncio.run(main())
```

---

## Environment variables (used by `example.py`)

| Variable | Required | Description |
|----------|----------|-------------|
| `TELEGRAM_API_ID` | yes | Numeric API ID from my.telegram.org |
| `TELEGRAM_API_HASH` | yes | API hash from my.telegram.org |
| `TELEGRAM_PHONE` | yes | Phone number in international format |
| `TELEGRAM_SESSION_NAME` | no | Session file stem (default: `telegram`) |

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `telethon >= 1.36` | Telegram MTProto client |
| `ftfy >= 6.0` | Unicode text repair |
| `emoji >= 2.1` | Emoji detection and removal |
| `python-ulid >= 3.1` | Time-sortable unique IDs for messages |
| `python-dotenv` | (optional, examples only) Load `.env` files |

Python 3.10+ required. Tested on 3.10, 3.11, 3.12.

---

## Key design decisions

**Session isolation.** `SessionManager` owns all auth state. `TelegramListener` never touches credentials directly — it only calls `is_operational()` and `get_authorized_client()`. This means session handling can be replaced or mocked without touching the listener.

**Queue-based output.** Using `asyncio.Queue` decouples message production from consumption. Consumers can be slow, concurrent, or replaceable at runtime. The `queue_maxsize=0` default is unbounded, so callers must set a bound if they cannot guarantee keeping up.

**Drop-on-full backpressure.** `put_nowait` + warning is chosen over `await queue.put()` to avoid blocking the Telethon event loop. Messages are lost rather than stalling the connection.

**Sentinel for shutdown.** `None` is placed on the queue when the listener stops. Consumers should check for `None` to detect end-of-stream cleanly.

**Automatic reconnection.** Transient connection errors trigger exponential backoff (2^n seconds, capped at 60 s, plus uniform jitter). Fatal auth errors bypass the retry loop entirely and stop the listener.

**Lazy chat metadata.** `_chat_meta` is populated on first message per `chat_id`. This avoids upfront API calls for all channels at startup and keeps `start()` fast.

**Text normalization.** All messages are unicode-repaired and emoji-stripped before reaching the queue. The output `text` field is always clean ASCII-compatible text.
