"""Tests for the Kafka event-broker adapter."""

from __future__ import annotations

from typing import Any

import pytest
from dataexcept import OperationTimeoutError, ServiceConnectionError

from data_platform_lab.broker import BrokerMessage, EventBroker, KafkaEventBroker


class FakeMessage:
    def __init__(
        self,
        value: bytes = b"payload",
        key: bytes | None = b"key",
        error: object | None = None,
    ) -> None:
        self._value = value
        self._key = key
        self._error = error

    def error(self) -> object | None:
        return self._error

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
    def __init__(self, *, pending: int = 0, failure: Exception | None = None) -> None:
        self.calls: list[tuple[str, bytes, bytes | None]] = []
        self.pending = pending
        self.failure = failure

    def produce(self, *, topic: str, value: bytes, key: bytes | None, callback: Any) -> None:
        if self.failure is not None:
            raise self.failure
        self.calls.append((topic, value, key))
        callback(None, FakeMessage(value=value, key=key))

    def flush(self, _timeout: float) -> int:
        return self.pending


class FakeConsumer:
    def __init__(self, message: FakeMessage | None = None) -> None:
        self.message = message or FakeMessage()
        self.subscriptions: list[list[str]] = []
        self.commits: list[tuple[list[Any], bool]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, _timeout: float) -> FakeMessage:
        return self.message

    def commit(self, *, offsets: list[Any], asynchronous: bool) -> None:
        self.commits.append((offsets, asynchronous))

    def close(self) -> None:
        self.closed = True


def _bare_broker(producer: FakeProducer | None = None) -> KafkaEventBroker:
    broker = object.__new__(KafkaEventBroker)
    broker._producer = producer or FakeProducer()
    broker._consumer = None
    broker._bootstrap_servers = "localhost:9092"
    broker._consumer_factory = lambda _config: FakeConsumer()
    broker._topic_partition_factory = lambda topic, partition, offset: (topic, partition, offset)
    return broker


def test_event_broker_contract_is_runtime_checkable() -> None:
    class DummyBroker:
        def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None:
            del topic, value, key

        def consume_one(
            self, topic: str, group_id: str, timeout_seconds: float = 10.0
        ) -> BrokerMessage | None:
            del topic, group_id, timeout_seconds
            return None

        def acknowledge(self, message: BrokerMessage) -> None:
            del message

        def close(self) -> None:
            return None

    assert isinstance(DummyBroker(), EventBroker)


def test_kafka_adapter_validates_inputs_without_connecting() -> None:
    broker = object.__new__(KafkaEventBroker)
    with pytest.raises(ValueError, match="topic"):
        KafkaEventBroker.publish(broker, "", b"payload")
    with pytest.raises(TypeError, match="bytes"):
        KafkaEventBroker.publish(broker, "events", "payload")  # type: ignore[arg-type]


def test_kafka_publish_classifies_client_failure() -> None:
    cause = OSError("broker unavailable")
    broker = _bare_broker(FakeProducer(failure=cause))

    with pytest.raises(ServiceConnectionError, match="Kafka") as error:
        broker.publish("events", b"payload")

    assert error.value.__cause__ is cause
    assert error.value.original_exception is cause


def test_kafka_publish_classifies_delivery_timeout() -> None:
    broker = _bare_broker(FakeProducer(pending=1))

    with pytest.raises(OperationTimeoutError, match="Kafka publish") as error:
        broker.publish("events", b"payload")

    assert error.value.timeout == 10.0


def test_kafka_consume_classifies_broker_error() -> None:
    broker = _bare_broker()
    broker._consumer_factory = lambda _config: FakeConsumer(FakeMessage(error="transport failure"))

    with pytest.raises(ServiceConnectionError, match="transport failure") as error:
        broker.consume_one("events", "group")

    assert isinstance(error.value.__cause__, RuntimeError)


def test_kafka_acknowledgement_commits_next_offset() -> None:
    class TopicPartition:
        def __init__(self, topic: str, partition: int, offset: int) -> None:
            self.topic = topic
            self.partition = partition
            self.offset = offset

    broker = _bare_broker()
    consumer = FakeConsumer()
    broker._consumer = consumer
    broker._topic_partition_factory = TopicPartition
    broker.acknowledge(BrokerMessage("events", 2, 7, b"key", b"payload"))
    offsets, asynchronous = consumer.commits[0]
    assert asynchronous is False
    assert offsets[0].topic == "events"
    assert offsets[0].partition == 2
    assert offsets[0].offset == 8


def test_broker_message_is_immutable_value_object() -> None:
    message = BrokerMessage("events", 0, 7, b"key", b"payload")
    assert message.topic == "events"
    assert message.partition == 0
    assert message.offset == 7
    assert message.key == b"key"
    assert message.value == b"payload"
