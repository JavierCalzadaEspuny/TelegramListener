"""Real-time Telegram listener and normalized streamed message models."""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import ulid
from cleantext import clean
from telethon import events
from telethon.tl.types import Message as TelethonMessage

from .session_manager import SessionManager

logger = logging.getLogger("telegram_listener")

# --- Passive Utilities ---

def _sanitize_text(text: str) -> str:
    """Normalize Telegram message text for downstream processing."""
    return clean(
        text,
        fix_unicode=True,
        to_ascii=False,
        lower=False,
        no_emoji=True,
    ).strip()

def _normalize_channel_key(channel: str) -> str:
    """Normalize channel keys to lowercase @-prefixed values."""
    c = str(channel or "").strip().lower()
    if not c: return ""
    return f"@{c.lstrip('@')}"

@dataclass(frozen=True)
class TelegramStreamedMessage:
    """
    Immutable container for normalized streamed Telegram messages.

    The object represents the minimal, transport-friendly payload produced by
    the listener and consumed by downstream processors.

    Attributes
    ----------
    timestamp : int
        Unix timestamp normalized to the listener timezone.
    source : str
        Human-readable channel title.
    source_id : int
        Numeric Telegram chat identifier.
    text : str
        Sanitized message content.
    language : str
        Language tag associated with the channel.
    id : str
        Stable identifier.

    Methods
    -------
    __str__():
        Return a compact, human-readable representation.

    Example
    -------
    >>> m = TelegramStreamedMessage(1700000000, "news", 123, "hello")
    >>> m.id.startswith("1700000000_")
    True
    """
    timestamp: int
    source: str
    source_id: int
    text: str
    language: str = "unknown"
    id: str = field(init=False)

    def __post_init__(self) -> None:
        """Populate the identifier after initialization."""
        object.__setattr__(self, "id", str(ulid.ULID()))
    
    def __str__(self) -> str:
        """Render a compact multi-line preview of the streamed message."""
        return (f"StreamedMessage(\n"
                f"      timestamp={self.timestamp},\n"
                f"      id={self.id},\n"
                f"      source={self.source},\n"
                f"      source_id={self.source_id},\n"
                f"      language={self.language},\n"
                f"      text={self.text[:30]}...)"
                )
    
# --- The Modern Listener ---

class TelegramListener:
    """
    Example
    -------
    >>> # 1. Dependency Injection: Pass the manager to the listener
    >>> listener = TelegramListener(session_manager=manager)
    >>> listener.set_channels([{"channel": "ajanews", "language": "ar"}])
    
    >>> # 2. Run the listener and a consumer concurrently
    >>> async def simple_consumer(q):
    ...     while True:
    ...         msg = await q.get()
    ...         print(f"New Message: {msg}")
    ...         q.task_done()
    
    >>> # 3. Start the loop (this will block until stopped)
    >>> try:
    ...     await asyncio.gather(
    ...         listener.start(),
    ...         simple_consumer(listener.queue)
    ...     )
    ... except KeyboardInterrupt:
    ...     listener.stop()
    """

    def __init__(
        self,
        session_manager: SessionManager,
        timezone_name: str = "UTC"
    ) -> None:
        """
        Initialize a listener bound to a session manager and timezone.

        Parameters
        ----------
        session_manager : SessionManager
            Session manager responsible for authorization and client creation.
        timezone_name : str
            IANA timezone used to normalize output timestamps.

        Raises
        ------
        ZoneInfoNotFoundError
            Raised when `timezone_name` is not a valid IANA timezone.
        """
        # Dependency Injection
        self.manager = session_manager
        self._tz = ZoneInfo(timezone_name)
        
        # Telethon Client (State)
        self.client = None
        self.queue: asyncio.Queue[TelegramStreamedMessage] = asyncio.Queue()
        
        # Internal configuration
        self._channels: list[str] = []
        self._channel_languages: dict[str, str] = {}
        
        # Caches for performance
        self._chat_cache: dict[int, str] = {}
        self._chat_lang_key_cache: dict[int, str] = {}
        
        self._stop_requested = False

    def set_channels(self, channel_data: list[str] | list[dict]) -> None:
        """
        Configure channels to monitor and optional language metadata.

        Parameters
        ----------
        channel_data : list[str] | list[dict]
            Channel configuration as either usernames or dictionaries with
            keys such as `channel`/`name` and `language`/`lang`.

        Returns
        -------
        None
            Updates internal channel and language mappings in place.
        """
        self._channels = []
        self._channel_languages = {}

        for item in channel_data:
            if isinstance(item, str):
                name, lang = item, "unknown"
            else:
                name = item.get("channel") or item.get("name") or item.get("source", "")
                lang = item.get("language") or item.get("lang") or "unknown"
            
            if name:
                clean_name = name.strip()
                self._channels.append(clean_name)
                self._channel_languages[_normalize_channel_key(clean_name)] = lang.lower()

    async def _message_handler(self, event: events.NewMessage.Event) -> None:
        """Transform a raw Telethon event into a queued streamed message."""
        msg: TelethonMessage = event.message
        text = _sanitize_text((msg.message or "").strip())
        
        if not text:
            return

        chat_id = msg.chat_id
        
        # Resolve source name and language mapping
        if chat_id not in self._chat_cache:
            chat = await msg.get_chat()
            self._chat_cache[chat_id] = getattr(chat, "title", str(chat_id))
            username = getattr(chat, "username", None)
            key_source = username if username else self._chat_cache[chat_id]
            self._chat_lang_key_cache[chat_id] = _normalize_channel_key(key_source)

        timestamp = int(msg.date.astimezone(self._tz).timestamp())
        lang_key = self._chat_lang_key_cache.get(chat_id, "")
        language = self._channel_languages.get(lang_key, "unknown")

        # Build and Queue
        streamed_msg = TelegramStreamedMessage(
            timestamp=timestamp,
            source=self._chat_cache[chat_id],
            source_id=chat_id,
            text=text,
            language=language,
        )
        await self.queue.put(streamed_msg)

    async def start(self) -> None:
        """
        Start the listener loop and stream incoming messages to the queue.

        Returns
        -------
        None
            Runs until `stop()` is requested or the client disconnects.

        Raises
        ------
        ValueError
            Raised when no channels were configured before startup.
        RuntimeError
            Raised when the session manager reports a non-operational session.
        """
        if not self._channels:
            raise ValueError("No channels configured. Call set_channels() first.")

        # 1. Operational Check (The Fail-Safe)
        if not await self.manager.is_operational():
            raise RuntimeError("❌ Session is not operational. Run manual login first.")

        # 2. Get the authorized client
        self.client = await self.manager.get_authorized_client()
        
        # 3. Register the event handler
        self.client.on(events.NewMessage(chats=self._channels))(self._message_handler)
        
        logger.info("Listener online. Monitoring %s sources.", len(self._channels))
        
        self._stop_requested = False
        while not self._stop_requested:
            try:
                await self.client.run_until_disconnected()
            except Exception as e:
                if self._stop_requested:
                    break
                
                logger.warning("⚠️ Connection lost: %s. Re-verifying session in 15s...", e)
                await asyncio.sleep(15)
                
                # Double-check if the session survived the crash
                if await self.manager.is_operational():
                    await self.client.connect()
                else:
                    logger.error("❌ Session revoked or corrupted. Stopping listener.")
                    self._stop_requested = True
                    break

    def stop(self) -> None:
        """
        Request a graceful shutdown of the listener loop.

        Returns
        -------
        None
            Sets the internal stop flag consumed by `start()`.
        """
        logger.info("🟥 Stop requested.")
        self._stop_requested = True