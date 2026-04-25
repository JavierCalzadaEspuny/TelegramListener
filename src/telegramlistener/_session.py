"""Telegram session lifecycle: authentication and validation."""

from __future__ import annotations

import logging
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    UserDeactivatedError,
)

from ._exceptions import SessionError

logger = logging.getLogger(__name__)

_DEFAULT_SESSION_DIR = Path.home() / ".cache" / "telegramlistener"
_FATAL_SESSION_ERRORS = (
    AuthKeyDuplicatedError,
    AuthKeyUnregisteredError,
    UserDeactivatedError,
)


class SessionManager:
    """Manages the Telethon session lifecycle: validation, login, and cleanup.

    SessionManager isolates all credential and authentication logic so
    :class:`TelegramListener` can operate without handling auth directly.

    Session files are SQLite databases stored under ``session_dir``
    (default: ``~/.cache/telegramlistener/``). Telethon identifies them by
    the path prefix — the ``.session`` extension is appended automatically.

    Args:
        api_id: Telegram API identifier from https://my.telegram.org.
        api_hash: Telegram API hash paired with ``api_id``.
        phone: Phone number in international format, e.g. ``"+34612345678"``.
        session_name: Filename stem for the ``.session`` file. Default ``"telegram"``.
        session_dir: Directory for session files.
            Default ``~/.cache/telegramlistener/``.

    Example:
        >>> manager = SessionManager(
        ...     api_id=12345,
        ...     api_hash="abc123",
        ...     phone="+34612345678",
        ... )
        >>> if not await manager.is_operational():
        ...     await manager.run_manual_login()
        >>> client = await manager.get_authorized_client()
        >>> try:
        ...     me = await client.get_me()
        ... finally:
        ...     await client.disconnect()
    """

    def __init__(
        self,
        api_id: int,
        api_hash: str,
        phone: str,
        session_name: str = "telegram",
        session_dir: Path | None = None,
    ) -> None:
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.session_name = session_name

        _dir = session_dir or _DEFAULT_SESSION_DIR
        _dir.mkdir(parents=True, exist_ok=True)

        self._session_path = _dir / session_name
        self._session_file = self._session_path.with_suffix(".session")

    @property
    def session_file(self) -> Path:
        """Path to the ``.session`` file on disk."""
        return self._session_file

    async def is_operational(self) -> bool:
        """Return ``True`` if a valid, authorized session exists on disk."""
        if not self._session_file.exists():
            return False

        client = TelegramClient(str(self._session_path), self.api_id, self.api_hash)
        try:
            await client.connect()
            authorized = await client.is_user_authorized()
        except _FATAL_SESSION_ERRORS:
            logger.warning(
                "Session %r is revoked or corrupted — cleaning up.", self.session_name
            )
            self._cleanup()
            return False
        except Exception as exc:
            logger.warning(
                "Session health check failed (%s) — treating as inoperational.", exc
            )
            return False
        else:
            if not authorized:
                logger.warning(
                    "Session %r exists but is no longer authorized — cleaning up.",
                    self.session_name,
                )
                self._cleanup()
                return False
            return True
        finally:
            await client.disconnect()

    async def run_manual_login(self) -> None:
        """Run the interactive terminal login flow and persist the session.

        Telethon handles the full flow: it prompts for the SMS code and,
        if needed, the 2FA password. Call this once before the first
        :meth:`TelegramListener.start`.

        Raises:
            SessionError: If login fails for any reason.
        """
        if await self.is_operational():
            logger.info(
                "Session %r is already operational — skipping login.", self.session_name
            )
            return

        client = TelegramClient(str(self._session_path), self.api_id, self.api_hash)
        try:
            await client.start(phone=self.phone)
            me = await client.get_me()
            logger.info("Login successful: %s (@%s).", me.first_name, me.username)
        except Exception as exc:
            self._cleanup()
            raise SessionError(f"Login failed: {exc}") from exc
        finally:
            await client.disconnect()

    async def get_authorized_client(self) -> TelegramClient:
        """Return a connected :class:`~telethon.TelegramClient` for the current session.

        Raises:
            SessionError: If no valid session exists. Call :meth:`run_manual_login`
                first.
        """
        if not await self.is_operational():
            raise SessionError(
                f"No valid session at {self._session_file}. "
                "Call run_manual_login() first."
            )

        client = TelegramClient(str(self._session_path), self.api_id, self.api_hash)
        await client.connect()
        return client

    def _cleanup(self) -> None:
        try:
            self._session_file.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning(
                "Could not delete session file %s: %s.", self._session_file, exc
            )
