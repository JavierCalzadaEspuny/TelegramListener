"""Data models."""

from __future__ import annotations

from dataclasses import dataclass, field

import ulid


@dataclass(frozen=True)
class TelegramStreamedMessage:
    """An immutable, normalized message received from a monitored channel.

    Produced by :class:`TelegramListener` and placed on its ``queue``.

    Attributes:
        timestamp: Unix timestamp (UTC seconds) of the original Telegram message.
        source: Human-readable channel title.
        source_id: Numeric Telegram chat identifier.
        text: Sanitized message text — unicode-fixed, emoji-stripped, or None when
            the message has no text/caption.
        images: Always a list of in-memory binary payloads for attached photos.
            The list may be empty when the message has no images.
        id: Time-sortable ULID string (26 characters), unique per instance.

    Example:
        >>> msg = TelegramStreamedMessage(
        ...     timestamp=1700000000,
        ...     source="Al Jazeera",
        ...     source_id=-1001234567890,
        ...     text="Breaking news...",
        ...     images=[],
        ... )
        >>> len(msg.id)
        26
    """

    timestamp: int
    source: str
    source_id: int
    text: str | None
    id: str = field(init=False)
    images: list[bytes] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", str(ulid.ULID()))

    def __repr__(self) -> str:
        preview = None
        if self.text is not None:
            preview = self.text[:50] + "..." if len(self.text) > 50 else self.text
        return (
            f"TelegramStreamedMessage("
            f"id={self.id!r}, "
            f"source={self.source!r}, "
            f"timestamp={self.timestamp}, "
            f"text={preview!r}, "
            f"images={len(self.images)})"
        )
