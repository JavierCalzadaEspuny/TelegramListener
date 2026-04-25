"""telegramlistener — stream Telegram channel messages to an asyncio queue."""

from __future__ import annotations

import logging

from ._exceptions import ConfigurationError, SessionError, TelegramListenerError
from ._listener import TelegramListener
from ._models import Channel, TelegramStreamedMessage
from ._session import SessionManager

logging.getLogger(__name__).addHandler(logging.NullHandler())

__all__ = [
    "Channel",
    "ConfigurationError",
    "SessionError",
    "SessionManager",
    "TelegramListener",
    "TelegramListenerError",
    "TelegramStreamedMessage",
]
