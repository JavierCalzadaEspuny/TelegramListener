"""Session lifecycle management for Telethon authentication and validation."""

import asyncio
import logging
from pathlib import Path
from telethon import TelegramClient
from telethon.errors import (
    AuthKeyDuplicatedError, 
    AuthKeyUnregisteredError, 
    UserDeactivatedError
)

logger = logging.getLogger("session_manager")

class SessionManager:
    """
    Session manager for validating, creating, and cleaning Telegram sessions.

    The class centralizes session health checks and manual login flow so
    runtime listeners can operate without handling credential logic directly.

    Attributes
    ----------
    api_id : int
        Telegram API identifier.
    api_hash : str
        Telegram API hash associated with `api_id`.
    phone : str
        Phone number used during interactive login.
    session_name : str
        Logical name used to build the `.session` file path.
    base_cache_dir : Path
        Base cache directory where session files are stored.
    session_path : Path
        Path prefix used by Telethon for the session database.
    session_file : Path
        Physical `.session` file path.

    Methods
    -------
    is_operational():
        Validate that a session exists and is still authorized.
    run_manual_login():
        Execute interactive login flow when session is missing or invalid.
    get_authorized_client():
        Return a connected Telethon client bound to a valid session.
    cleanup():
        Remove invalid session artifacts from disk.

    Example
    -------
    >>> # 1. Initialize the session identity
    >>> manager = SessionManager(api_id=12345, api_hash="your_hash", phone="+100000000")
    
    >>> # 2. Ensure session is active (triggers SMS flow only if needed)
    >>> await manager.run_manual_login()
    
    >>> # 3. Get the client and perform operations
    >>> client = await manager.get_authorized_client()
    >>> try:
    ...     me = await client.get_me()
    ...     print(f"Logged in as: {me.username}")
    ... finally:
        # Always disconnect to release the .session file lock
    ...     await client.disconnect()
    """
    def __init__(self, api_id: int, api_hash: str, phone: str, session_name: str = "telegram"):
        """
        Initialize session manager paths and Telegram credentials.

        Parameters
        ----------
        api_id : int
            Telegram API identifier.
        api_hash : str
            Telegram API hash associated with `api_id`.
        phone : str
            Phone number used for interactive login.
        session_name : str
            Name used to derive session file paths.

        Returns
        -------
        None
            Initializes session state and creates cache directory if needed.
        """
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.session_name = session_name
        
        # Centralized Path: ~/.cache/telegramlistener/
        self.base_cache_dir = Path.home() / ".cache" / "telegramlistener"
        self.base_cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Telethon needs the path WITHOUT the .session extension for the constructor
        self.session_path = self.base_cache_dir / self.session_name
        self.session_file = self.session_path.with_suffix(".session")

    async def is_operational(self) -> bool:
        """
        Check whether the current session exists and remains authorized.

        Returns
        -------
        bool
            True when the session file exists and Telegram accepts it.
        """
        if not self.session_file.exists():
            return False

        client = TelegramClient(str(self.session_path), self.api_id, self.api_hash)
        try:
            await client.connect()
            is_authorized = await client.is_user_authorized()
            await client.disconnect()
            
            if not is_authorized:
                logger.warning("🚫 Session %s is no longer authorized. Cleaning up...", self.session_name)
                self.cleanup()
                return False
            return True

        except (AuthKeyUnregisteredError, AuthKeyDuplicatedError, UserDeactivatedError):
            logger.error("🚫 Session %s is corrupted or revoked. Removing file...", self.session_name)
            self.cleanup()
            return False
        except Exception as e:
            logger.warning("Connectivity error during health check: %s", e)
            return False

    async def run_manual_login(self):
        """
        Run the interactive login flow to create a valid session.

        Returns
        -------
        None
            Persists a new authorized session when login succeeds.

        Raises
        ------
        Exception
            Re-raises any Telethon login error after cleanup.
        """
        if await self.is_operational():
            logger.info("✅ Session '%s' is already operational. Skipping login.", self.session_name)
            return

        logger.info("Starting interactive login for %s...", self.phone)
        client = TelegramClient(str(self.session_path), self.api_id, self.api_hash)
        try:
            # Handles phone, SMS code, and 2FA password via terminal
            await client.start(phone=self.phone)
            me = await client.get_me()
            logger.info("✅ Login successful. Logged in as: %s (@%s)", me.first_name, me.username)
            await client.disconnect()
        except Exception as e:
            logger.error("❌ Login failed: %s", e)
            self.cleanup()
            raise

    async def get_authorized_client(self) -> TelegramClient:
        """
        Return a connected Telethon client backed by a valid session.

        Returns
        -------
        TelegramClient
            Connected client authorized for Telegram API operations.

        Raises
        ------
        RuntimeError
            Raised when the session file is missing or no longer authorized.
        """
        if not await self.is_operational():
            raise RuntimeError(
                f"No valid session found at {self.session_file}. "
                "Please run 'run_manual_login()' first."
            )
        
        client = TelegramClient(str(self.session_path), self.api_id, self.api_hash)
        await client.connect()
        return client

    def cleanup(self):
        """
        Remove the session file from disk when it is invalid.

        Returns
        -------
        None
            Deletes the session file when present.
        """
        if self.session_file.exists():
            try:
                self.session_file.unlink(missing_ok=True)
                logger.warning("Deleted invalid session: %s", self.session_file)
            except OSError as e:
                logger.warning("Could not delete session file: %s", e)