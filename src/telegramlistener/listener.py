"""Telegram real-time listener with queue-based output."""

import asyncio
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import ulid
from cleantext import clean
from telethon import TelegramClient as TelethonClient, events
from telethon.errors import AuthKeyDuplicatedError, AuthKeyUnregisteredError
from telethon.tl.types import Message as TelethonMessage

from .login import SESSION_PATH, session_file_path, telegram_login


def _default_session_dir() -> Path:
    """Build and create the Telegram session directory."""
    session_dir = Path.home() / ".cache" / "telegramlistener"
    session_dir.mkdir(parents=True, exist_ok=True)
    return session_dir


def _sanitize_text(text: str) -> str:
    """Clean text using cleantext library."""
    return clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        lower=False,
        no_line_breaks=False,
        no_urls=False,
        no_emails=False,
        no_phone_numbers=False,
        no_numbers=False,
        no_digits=False,
        no_currency_symbols=False,
        no_punct=False,
        no_emoji=True,
        lang="en",
    ).strip()


def _build_unix_ulid(timestamp: int) -> str:
    """Build stable ID: <unix_timestamp>_<ulid>."""
    return f"{timestamp}_{ulid.new()}"


def _normalize_channel_key(channel: str) -> str:
    """Normalize channel name to @-prefixed format."""
    c = str(channel or "").strip().lower()
    if not c:
        return ""
    if c.startswith("@"):
        return c
    return f"@{c}"


def _extract_lang(spec) -> str:
    """Extract language from dict or string spec."""
    if isinstance(spec, str):
        return spec.strip().lower() or "unknown"
    if isinstance(spec, dict):
        lang = str(
            spec.get("language")
            or spec.get("lang")
            or spec.get("language_code")
            or ""
        ).strip().lower()
        return lang or "unknown"
    return "unknown"


@dataclass(frozen=True)
class TelegramStreamedMessage:
    """Telegram-specific message container produced by the listener."""

    timestamp: int
    source: str
    source_id: int
    text: str
    language: str = "unknown"
    global_id: str = field(init=False)

    def __post_init__(self) -> None:
        """Generate stable global ID on initialization."""
        object.__setattr__(self, "global_id", _build_unix_ulid(self.timestamp))

    def __str__(self) -> str:
        """Return formatted message representation."""
        return (
            f"  global_id={self.global_id}\n"
            f"  timestamp={self.timestamp}\n"
            f"  source={self.source}\n"
            f"  source_id={self.source_id}\n"
            f"  language={self.language}\n"
            f"  text={self.text}\n"
        )


class TelegramListener:
    """
    Real-time Telegram message listener with queue-based output.

    Requires a pre-authenticated session file under .cache/telegramlistener.
    Incoming messages are converted to TelegramStreamedMessage and queued for
    downstream processing.
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        phone: str,
        session_path: str | Path | None = None,
        timezone_name: str = "UTC",
        channel_languages: list[str] | list[dict[str, str]] | None = None,
    ) -> None:
        """Initialize Telegram stream client.

        Parameters
        ----------
        api_id : int
            Telegram API ID.
        api_hash : str
            Telegram API hash.
        phone : str
            Phone number for login when no valid session exists.
        session_path : str | Path | None
            Optional base path for the Telethon session.
        timezone_name : str
            IANA timezone for message timestamp normalization.
        channel_languages : list[str] | list[dict[str, str]] | None
            Optional list of channels or channel dictionaries with language.
        """
        if not api_id or not api_hash:
            raise ValueError("api_id and api_hash are required")

        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self._tz = ZoneInfo(timezone_name)

        base_dir = Path(session_path) if session_path is not None else _default_session_dir()
        base_dir.mkdir(parents=True, exist_ok=True)
        self.session_path = base_dir / "telegram"
        self._client = TelethonClient(str(self.session_path), api_id, api_hash)

        self.queue: asyncio.Queue[TelegramStreamedMessage] = asyncio.Queue()
        self._stop_requested = False
        self._stream_registered = False
        self._channels: list[str] = []
        self._chat_cache: dict[int, str] = {}
        self._chat_lang_key_cache: dict[int, str] = {}
        self._channel_languages: dict[str, str] = {}

        if channel_languages:
            self.set_channel_languages(channel_languages)

    @property
    def lag(self) -> timedelta:
        """Return current queue lag."""
        if self.queue.empty():
            return timedelta(0)
        oldest_ts = self.queue._queue[0].timestamp
        now_ts = int(datetime.now(self._tz).timestamp())
        return timedelta(seconds=max(0, now_ts - oldest_ts))

    @property
    def queue_size(self) -> int:
        """Return number of pending messages."""
        return self.queue.qsize()

    def set_channel_languages(self, channel_languages: list[str] | list[dict[str, str]]) -> None:
        """Update channel list and per-channel language mapping."""
        self._channels = []
        self._channel_languages = {}

        if all(isinstance(item, str) for item in channel_languages):
            for channel in channel_languages:
                channel_name = str(channel).strip()
                if not channel_name:
                    continue
                self._channels.append(channel_name)
                self._channel_languages[_normalize_channel_key(channel_name)] = "unknown"
            return

        if all(isinstance(item, dict) for item in channel_languages):
            for item in channel_languages:
                channel_name = str(item.get("channel", item.get("name", ""))).strip()
                if not channel_name:
                    continue
                self._channels.append(channel_name)
                self._channel_languages[_normalize_channel_key(channel_name)] = _extract_lang(item)
            return

        raise TypeError("channels must be list[str] or list[dict[channel, language]]")

    async def connect(self) -> None:
        """Establish connection using the cached session or create a new one."""
        session_file = session_file_path()

        if session_file.exists():
            try:
                await self._client.connect()
                if await self._client.is_user_authorized():
                    return
            except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, sqlite3.OperationalError):
                pass
            session_file.unlink(missing_ok=True)
            self._client = TelethonClient(str(self.session_path), self.api_id, self.api_hash)

        if not self.phone:
            raise ValueError("Phone required to login. No session found.")

        await telegram_login(self.api_id, self.api_hash, self.phone)
        self._client = TelethonClient(str(self.session_path), self.api_id, self.api_hash)
        await self._client.connect()
        if not await self._client.is_user_authorized():
            raise RuntimeError("Session was created but not authorized.")

    async def disconnect(self) -> None:
        """Disconnect from Telegram."""
        self._stop_requested = True
        await self._client.disconnect()

    async def _build_message(self, msg: TelethonMessage) -> TelegramStreamedMessage | None:
        """Convert Telethon message to TelegramStreamedMessage."""
        text = _sanitize_text((msg.message or "").strip())
        if not text:
            return None

        chat_id = msg.chat_id
        if chat_id not in self._chat_cache:
            chat = await msg.get_chat()
            self._chat_cache[chat_id] = getattr(chat, "title", str(chat.id))
            username = getattr(chat, "username", None)
            key_source = username if username else self._chat_cache[chat_id]
            self._chat_lang_key_cache[chat_id] = _normalize_channel_key(key_source)

        timestamp = int(msg.date.astimezone(self._tz).timestamp())
        lang_key = self._chat_lang_key_cache.get(chat_id, "")
        language = self._channel_languages.get(lang_key, "unknown")

        return TelegramStreamedMessage(
            timestamp=timestamp,
            source=self._chat_cache[chat_id],
            source_id=chat_id,
            text=text,
            language=language,
        )

    def _register_stream(self) -> None:
        """Register a message handler once for the current channel list."""
        if self._stream_registered:
            return
        if not self._channels:
            raise ValueError("No channels configured. Pass channels to ingest().")

        self._stream_registered = True

        @self._client.on(events.NewMessage(chats=self._channels))
        async def _handler(event: events.NewMessage.Event) -> None:
            parsed = await self._build_message(event.message)
            if parsed is not None:
                await self.queue.put(parsed)

    async def ingest(self, channels: list[str] | list[dict[str, str]]) -> None:
        """Configure channels and attach real-time handlers."""
        self.set_channel_languages(channels)
        self._register_stream()

    async def run(self) -> None:
        """Keep the listener alive."""
        self._stop_requested = False
        while not self._stop_requested:
            try:
                await self._client.run_until_disconnected()
            except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, sqlite3.OperationalError):
                if self._stop_requested:
                    break
                session_file = session_file_path()
                session_file.unlink(missing_ok=True)
                self._client = TelethonClient(str(self.session_path), self.api_id, self.api_hash)
                if self.phone:
                    await telegram_login(self.api_id, self.api_hash, self.phone)
                    self._client = TelethonClient(str(self.session_path), self.api_id, self.api_hash)
                    await self._client.connect()
                else:
                    raise

