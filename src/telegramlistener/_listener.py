"""Real-time Telegram message listener with asyncio queue output."""

from __future__ import annotations

import asyncio
import logging
import random
from zoneinfo import ZoneInfo

import emoji
import ftfy
from telethon import TelegramClient, events
from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    UserDeactivatedError,
)
from telethon.tl.custom.message import Message

from ._exceptions import ConfigurationError, SessionError
from ._models import TelegramStreamedMessage
from ._session import SessionManager

logger = logging.getLogger(__name__)

_BACKOFF_BASE = 2.0
_BACKOFF_CAP = 60.0
_FATAL_ERRORS = (AuthKeyDuplicatedError, AuthKeyUnregisteredError, UserDeactivatedError)

# Sentinel placed on the queue by aclose() so consumers can detect shutdown.
_SENTINEL: TelegramStreamedMessage | None = None


def _sanitize(text: str | None) -> str | None:
    """
    Removes emojis and fixes broken unicode text.
    Returns None when the input is empty or sanitizes down to an empty string.
    """
    if text is None:
        return None
    sanitized = emoji.replace_emoji(ftfy.fix_text(text), replace="").strip()
    return sanitized or None


async def _download_image_bytes(message: Message) -> bytes | None:
    """Safely attempts to download media from a message as bytes."""
    if getattr(message, "photo", None) is None:
        return None

    payload = await message.download_media(file=bytes)
    return payload if isinstance(payload, bytes) else None


async def _resolve_chat_meta(
    message: Message,
    chat_meta: dict[int, str],
) -> tuple[int, str]:
    """Resolves and caches chat title by ID to minimize API calls."""
    chat_id = message.chat_id
    if chat_id not in chat_meta:
        chat = await message.get_chat()
        chat_meta[chat_id] = getattr(chat, "title", str(chat_id))

    return chat_id, chat_meta[chat_id]


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
    incoming messages are dropped and a warning is emitted to prevent blocking
    the Telethon event loop.

    Args:
        session_manager: An authorized :class:`SessionManager` instance.
        timezone_name: IANA timezone validated at initialization.
        queue_maxsize: Maximum number of messages buffered in :attr:`queue`.
            ``0`` (default) means unbounded.
    """

    def __init__(
        self,
        session_manager: SessionManager,
        timezone_name: str = "UTC",
        queue_maxsize: int = 0,
    ) -> None:
        self._manager = session_manager
        ZoneInfo(timezone_name)
        self._channels: list[str] = []
        self._client: TelegramClient | None = None
        self._stop_event = asyncio.Event()
        self._disconnect_task: asyncio.Task[None] | None = None
        self.queue: asyncio.Queue[TelegramStreamedMessage | None] = asyncio.Queue(
            maxsize=queue_maxsize
        )
        self._chat_meta: dict[int, str] = {}

    def set_channels(self, channels: list[str]) -> None:
        """Configure the channels to monitor."""
        self._channels = [
            channel.strip().lstrip("@").lower()
            for channel in channels
        ]

    async def start(self) -> None:
        """Start the listener and block until stopped or invalidated."""
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
            self._on_new_message,
            events.NewMessage(chats=self._channels),
        )
        self._client.add_event_handler(
            self._on_album,
            events.Album(chats=self._channels),
        )
        logger.info("Listener started. Monitoring %d channel(s).", len(self._channels))

        attempt = 0
        while not self._stop_event.is_set():
            try:
                await self._client.run_until_disconnected()
                break
            except _FATAL_ERRORS as exc:
                logger.error(
                    "Session permanently invalidated (%s). Stopping loop.",
                    type(exc).__name__,
                )
                self._safely_cleanup_session()
                break
            except Exception as exc:
                if self._stop_event.is_set():
                    break

                attempt += 1
                delay = min(_BACKOFF_CAP, _BACKOFF_BASE ** attempt) + random.uniform(0, 1)
                logger.warning(
                    "Unexpected error (%s). Reconnecting in %.1f s (attempt %d).",
                    exc,
                    delay,
                    attempt,
                )

                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._stop_event.wait()), timeout=delay
                    )
                    break
                except asyncio.TimeoutError:
                    pass

                try:
                    if not self._client.is_connected():
                        await self._client.connect()
                    attempt = 0
                except _FATAL_ERRORS as fatal_exc:
                    logger.error("Session revoked during reconnect: %s", fatal_exc)
                    self._safely_cleanup_session()
                    break
                except Exception as reconnect_exc:
                    logger.error("Reconnection failed: %s. Retrying.", reconnect_exc)

        await self.queue.put(_SENTINEL)

    def _safely_cleanup_session(self) -> None:
        """Attempts best-effort cleanup of the session file."""
        try:
            self._manager._cleanup()
        except Exception:
            logger.debug("Session cleanup failed or is unavailable.")

    def stop(self) -> None:
        """Request a graceful shutdown non-blockingly."""
        self._stop_event.set()
        if self._client and self._client.is_connected():
            self._disconnect_task = asyncio.create_task(self._client.disconnect())

    async def aclose(self) -> None:
        """Shut down the listener and disconnect the Telegram client robustly."""
        self._stop_event.set()
        if self._client and self._client.is_connected():
            await self._client.disconnect()
        if self._disconnect_task is not None:
            await asyncio.shield(self._disconnect_task)

    async def __aenter__(self) -> TelegramListener:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def _enqueue_message(
        self, base_message: Message, text: str | None, images: list[bytes]
    ) -> None:
        """DRY helper to construct and safely enqueue the streamed message."""
        chat_id, title = await _resolve_chat_meta(base_message, self._chat_meta)
        streamed_message = TelegramStreamedMessage(
            timestamp=int(base_message.date.timestamp()),
            source=title,
            source_id=chat_id,
            text=text,
            images=images,
        )

        try:
            self.queue.put_nowait(streamed_message)
        except asyncio.QueueFull:
            logger.warning(
                "Queue full (maxsize=%d) — dropping message from %r.",
                self.queue.maxsize, title
            )

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        try:
            if event.message.grouped_id is not None:
                return

            message = event.message
            text = _sanitize((message.text or "").strip())

            image_bytes = await _download_image_bytes(message)
            images = [image_bytes] if image_bytes is not None else []

            if not text and not images:
                return

            await self._enqueue_message(message, text, images)

        except Exception:
            logger.exception(
                "Unhandled error processing message from chat_id=%s.",
                event.message.chat_id,
            )

    async def _on_album(self, event: events.Album.Event) -> None:
        try:
            messages = event.messages
            if not messages:
                return

            text = _sanitize((messages[0].text or "").strip())

            images: list[bytes] = []
            for message in messages:
                image_bytes = await _download_image_bytes(message)
                if image_bytes is not None:
                    images.append(image_bytes)

            if not images and not text:
                return

            await self._enqueue_message(messages[0], text, images)

        except Exception:
            logger.exception(
                "Unhandled error processing album from chat_id=%s.",
                event.messages[0].chat_id if event.messages else "unknown",
            )