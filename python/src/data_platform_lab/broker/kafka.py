"""Kafka implementation of the event-broker boundary."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from data_platform_lab.broker.store import BrokerMessage


class KafkaEventBroker:
    """Publish and consume messages through Apache Kafka."""

    def __init__(self, bootstrap_servers: str) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")

        try:
            kafka = import_module("confluent_kafka")
        except ModuleNotFoundError as exc:
            raise RuntimeError("confluent-kafka is required for Kafka broker support") from exc

        producer_factory = getattr(kafka, "Producer", None)
        consumer_factory = getattr(kafka, "Consumer", None)
        if not callable(producer_factory) or not callable(consumer_factory):
            raise RuntimeError("confluent-kafka does not expose Producer and Consumer")

        self._producer = producer_factory({"bootstrap.servers": bootstrap_servers})
        self._consumer_factory = consumer_factory
        self._bootstrap_servers = bootstrap_servers
        self._consumer: Any | None = None

    def publish(self, topic: str, value: bytes, key: bytes | None = None) -> None:
        """Publish one message and synchronously confirm broker delivery."""
        if not topic.strip():
            raise ValueError("topic must not be empty")
        if not isinstance(value, bytes):
            raise TypeError("value must be bytes")

        delivery_error: list[Exception] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                delivery_error.append(RuntimeError(str(error)))

        self._producer.produce(topic=topic, value=value, key=key, callback=delivered)
        remaining = self._producer.flush(10.0)
        if remaining:
            raise TimeoutError(f"Kafka delivery timed out with {remaining} message(s) pending")
        if delivery_error:
            raise delivery_error[0]

    def consume_one(
        self,
        topic: str,
        group_id: str,
        timeout_seconds: float = 10.0,
    ) -> BrokerMessage | None:
        """Consume one message from *topic* using a fresh group subscription."""
        if not topic.strip():
            raise ValueError("topic must not be empty")
        if not group_id.strip():
            raise ValueError("group_id must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._close_consumer()
        consumer = self._consumer_factory(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self._consumer = consumer
        consumer.subscribe([topic])
        message = consumer.poll(timeout_seconds)
        if message is None:
            return None
        error = message.error()
        if error is not None:
            raise RuntimeError(f"Kafka consume failed: {error}")

        value = message.value()
        if value is None:
            raise RuntimeError("Kafka message payload is unexpectedly null")
        return BrokerMessage(
            topic=str(message.topic()),
            partition=int(message.partition()),
            offset=int(message.offset()),
            key=message.key(),
            value=bytes(value),
        )

    def close(self) -> None:
        """Flush producer state and close any active consumer."""
        self._producer.flush(10.0)
        self._close_consumer()

    def _close_consumer(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
