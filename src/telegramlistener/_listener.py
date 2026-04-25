"""Real-time Telegram message listener with asyncio queue output."""

from __future__ import annotations

import asyncio
import logging
import random
from zoneinfo import ZoneInfo

import emoji
import ftfy
from telethon import events
from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    UserDeactivatedError,
)

from ._exceptions import ConfigurationError, SessionError
from ._models import Channel, TelegramStreamedMessage
from ._session import SessionManager

logger = logging.getLogger(__name__)

_BACKOFF_BASE = 2.0
_BACKOFF_CAP = 60.0
_FATAL_ERRORS = (AuthKeyDuplicatedError, AuthKeyUnregisteredError, UserDeactivatedError)

# Sentinel placed on the queue by aclose() so consumers can detect shutdown.
_SENTINEL: TelegramStreamedMessage | None = None


def _sanitize(text: str) -> str:
    return emoji.replace_emoji(ftfy.fix_text(text), replace="").strip()


def _to_channel(item: str | Channel) -> Channel:
    return item if isinstance(item, Channel) else Channel(name=item)


class TelegramListener:
    """Streams new messages from configured Telegram channels into an asyncio queue.

    Typical usage:

    1. Create a :class:`SessionManager` and ensure it is operational.
    2. Create a ``TelegramListener``, configure channels, then call :meth:`start`.
    3. Consume :class:`~telegramlistener.TelegramStreamedMessage` objects from
       :attr:`queue`.

    The listener reconnects automatically on transient failures using exponential
    backoff (capped at 60 s). It stops only when :meth:`aclose` / :meth:`stop` is
    called, or when the Telegram session is permanently invalidated.

    **Backpressure**: pass ``maxsize`` to cap queue depth. When the queue is full,
    ``_on_message`` drops the incoming message and emits a warning rather than
    blocking the Telethon event loop.

    Args:
        session_manager: An authorized :class:`SessionManager` instance.
        timezone_name: IANA timezone for normalizing message timestamps.
            Default ``"UTC"``.
        queue_maxsize: Maximum number of messages buffered in :attr:`queue`.
            ``0`` (default) means unbounded — only use this if your consumer
            is guaranteed to keep up.

    Raises:
        zoneinfo.ZoneInfoNotFoundError: If ``timezone_name`` is not a valid IANA
            timezone.

    Example:
        >>> listener = TelegramListener(session_manager=manager, queue_maxsize=1000)
        >>> listener.set_channels(["cnn", Channel("ajanews", language="ar")])
        >>> async with listener:
        ...     consumer = asyncio.create_task(consume(listener.queue))
        ...     await listener.start()
        ...     consumer.cancel()
    """

    def __init__(
        self,
        session_manager: SessionManager,
        timezone_name: str = "UTC",
        queue_maxsize: int = 0,
    ) -> None:
        self._manager = session_manager
        self._tz = ZoneInfo(timezone_name)
        self._channels: list[Channel] = []
        self._client = None
        self._stop_event = asyncio.Event()
        self._disconnect_task: asyncio.Task[None] | None = None
        self.queue: asyncio.Queue[TelegramStreamedMessage | None] = asyncio.Queue(
            maxsize=queue_maxsize
        )
        # chat_id -> (title, language), populated lazily on first message per chat
        self._chat_meta: dict[int, tuple[str, str]] = {}

    def set_channels(self, channels: list[str | Channel]) -> None:
        """Configure the channels to monitor.

        Args:
            channels: Channel usernames (``str``) or :class:`Channel` instances.
                Strings are equivalent to ``Channel(name=s, language="unknown")``.

        Example:
            >>> listener.set_channels([
            ...     "cnn",
            ...     Channel("ajanews", language="ar"),
            ... ])
        """
        self._channels = [_to_channel(item) for item in channels]

    async def start(self) -> None:
        """Start the listener and block until stopped or the session is invalidated.

        Raises:
            ConfigurationError: If :meth:`set_channels` was not called before start.
            SessionError: If the session is not operational at startup.
        """
        if not self._channels:
            raise ConfigurationError(
                "No channels configured. Call set_channels() before start()."
            )

        if not await self._manager.is_operational():
            raise SessionError(
                "Session is not operational. Call run_manual_login() first."
            )

        self._client = await self._manager.get_authorized_client()
        self._client.add_event_handler(
            self._on_message,
            events.NewMessage(chats=[c.name for c in self._channels]),
        )
        logger.info("Listener started. Monitoring %d channel(s).", len(self._channels))

        attempt = 0
        while not self._stop_event.is_set():
            try:
                await self._client.run_until_disconnected()
                break
            except _FATAL_ERRORS as exc:
                logger.error(
                    "Session permanently invalidated (%s). Stopping.",
                    type(exc).__name__,
                )
                break
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                delay = min(_BACKOFF_CAP, _BACKOFF_BASE**attempt) + random.uniform(0, 1)
                logger.warning(
                    "Connection lost (%s). Reconnecting in %.1f s (attempt %d).",
                    exc,
                    delay,
                    attempt + 1,
                )
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._stop_event.wait()),
                        timeout=delay,
                    )
                    break  # stop was requested during the backoff window
                except asyncio.TimeoutError:
                    pass

                if not await self._manager.is_operational():
                    logger.error(
                        "Session is no longer valid after reconnect attempt. Stopping."
                    )
                    break

                await self._client.connect()
                attempt += 1

        # Signal consumers that no more messages will arrive.
        await self.queue.put(_SENTINEL)

    def stop(self) -> None:
        """Request a graceful shutdown. Returns immediately; shutdown is asynchronous.

        Prefer :meth:`aclose` when you can ``await`` — it guarantees the client
        is disconnected before returning.
        """
        self._stop_event.set()
        if self._client is not None and self._client.is_connected():
            self._disconnect_task = asyncio.ensure_future(
                self._client.disconnect()
            )

    async def aclose(self) -> None:
        """Shut down the listener and disconnect the Telegram client.

        Awaiting this guarantees the client is disconnected before returning.
        Use in ``finally`` blocks or as part of an ``async with`` statement.
        """
        self._stop_event.set()
        if self._client is not None and self._client.is_connected():
            await self._client.disconnect()
        if self._disconnect_task is not None:
            # If stop() fired a disconnect task, wait for it to finish.
            await asyncio.shield(self._disconnect_task)

    async def __aenter__(self) -> TelegramListener:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def _on_message(self, event: events.NewMessage.Event) -> None:
        try:
            text = _sanitize((event.message.message or "").strip())
            if not text:
                return

            chat_id = event.message.chat_id
            if chat_id not in self._chat_meta:
                chat = await event.message.get_chat()
                title = getattr(chat, "title", str(chat_id))
                raw = getattr(chat, "username", None) or title
                username = raw.strip().lstrip("@").lower()
                language = next(
                    (c.language for c in self._channels if c.name.lower() == username),
                    "unknown",
                )
                self._chat_meta[chat_id] = (title, language)

            title, language = self._chat_meta[chat_id]
            msg = TelegramStreamedMessage(
                timestamp=int(event.message.date.astimezone(self._tz).timestamp()),
                source=title,
                source_id=chat_id,
                text=text,
                language=language,
            )

            try:
                self.queue.put_nowait(msg)
            except asyncio.QueueFull:
                logger.warning(
                    "Queue full (maxsize=%d) — dropping message from %r.",
                    self.queue.maxsize,
                    title,
                )
        except Exception:
            logger.exception(
                "Unhandled error processing message from chat_id=%s.",
                event.message.chat_id,
            )
