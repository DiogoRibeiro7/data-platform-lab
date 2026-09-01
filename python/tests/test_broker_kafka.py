"""Tests for the Kafka event-broker adapter."""

from __future__ import annotations

from typing import Any

import pytest

from data_platform_lab.broker import BrokerMessage, EventBroker, KafkaEventBroker


class FakeMessage:
    def __init__(self, value: bytes = b"payload", key: bytes | None = b"key") -> None:
        self._value = value
        self._key = key

    def error(self) -> None:
        return None

    def topic(self) -> str:
        return "events"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 7

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes:
        return self._value


class FakeProducer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bytes, bytes | None]] = []

    def produce(self, *, topic: str, value: bytes, key: bytes | None, callback: Any) -> None:
        self.calls.append((topic, value, key))
        callback(None, FakeMessage(value=value, key=key))

    def flush(self, _timeout: float) -> int:
        return 0


class FakeConsumer:
    def __init__(self, message: FakeMessage | None = None) -> None:
        self.message = message or FakeMessage()
        self.subscriptions: list[list[str]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, _timeout: float) -> FakeMessage:
        return self.message

    def close(self) -> None:
        self.closed = True


def test_event_broker_contract_is_runtime_checkable() -> None:
    class DummyBroker:
        def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None:
            del topic, value, key

        def consume_one(
            self, topic: str, group_id: str, timeout_seconds: float = 10.0
        ) -> BrokerMessage | None:
            del topic, group_id, timeout_seconds
            return None

        def close(self) -> None:
            return None

    assert isinstance(DummyBroker(), EventBroker)


def test_kafka_adapter_validates_inputs_without_connecting() -> None:
    broker = object.__new__(KafkaEventBroker)

    with pytest.raises(ValueError, match="topic"):
        KafkaEventBroker.publish(broker, "", b"payload")

    with pytest.raises(TypeError, match="bytes"):
        KafkaEventBroker.publish(broker, "events", "payload")  # type: ignore[arg-type]


def test_broker_message_is_immutable_value_object() -> None:
    message = BrokerMessage("events", 0, 7, b"key", b"payload")

    assert message.topic == "events"
    assert message.partition == 0
    assert message.offset == 7
    assert message.key == b"key"
    assert message.value == b"payload"
