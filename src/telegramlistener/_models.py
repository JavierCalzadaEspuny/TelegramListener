"""Data models."""

from __future__ import annotations

from dataclasses import dataclass, field

import ulid


@dataclass(frozen=True)
class Channel:
    """A Telegram channel to monitor.

    Args:
        name: Channel username, with or without a leading ``@``.
        language: BCP-47 language tag for downstream routing. Defaults to ``"unknown"``.

    Example:
        >>> Channel("ajanews", language="ar")
        Channel(name='ajanews', language='ar')
    """

    name: str
    language: str = "unknown"

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip().lstrip("@"))
        object.__setattr__(self, "language", self.language.lower())


@dataclass(frozen=True)
class TelegramStreamedMessage:
    """An immutable, normalized message received from a monitored channel.

    Produced by :class:`TelegramListener` and placed on its ``queue``.

    Attributes:
        timestamp: Unix timestamp (UTC seconds) of the original Telegram message.
        source: Human-readable channel title.
        source_id: Numeric Telegram chat identifier.
        text: Sanitized message text — unicode-fixed, emoji-stripped.
        language: Language tag from channel configuration, or ``"unknown"``.
        id: Time-sortable ULID string (26 characters), unique per instance.

    Example:
        >>> msg = TelegramStreamedMessage(
        ...     timestamp=1700000000,
        ...     source="Al Jazeera",
        ...     source_id=-1001234567890,
        ...     text="Breaking news...",
        ...     language="ar",
        ... )
        >>> len(msg.id)
        26
    """

    timestamp: int
    source: str
    source_id: int
    text: str
    language: str = "unknown"
    id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", str(ulid.ULID()))

    def __repr__(self) -> str:
        preview = self.text[:50] + "..." if len(self.text) > 50 else self.text
        return (
            f"TelegramStreamedMessage("
            f"id={self.id!r}, "
            f"source={self.source!r}, "
            f"language={self.language!r}, "
            f"timestamp={self.timestamp}, "
            f"text={preview!r})"
        )
