"""Application-level event-broker contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class BrokerMessage:
    """One message read from an event broker."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes


@runtime_checkable
class EventBroker(Protocol):
    """Minimal publish/consume boundary used by the local platform."""

    def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        """Publish one message and wait for delivery confirmation."""

    def consume_one(
        self,
        topic: str,
        group_id: str,
        timeout_seconds: float = 10.0,
    ) -> BrokerMessage | None:
        """Return one message or ``None`` when the timeout expires."""

    def acknowledge(self, message: BrokerMessage) -> None:
        """Commit the consumed message position after durable processing succeeds."""

    def close(self) -> None:
        """Flush producer state and release consumer resources."""
