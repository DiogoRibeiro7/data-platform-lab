"""Kafka implementation of the event-broker boundary."""

from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from typing import Any, cast

from dataexcept import DependencyError, OperationTimeoutError, ServiceConnectionError

from data_platform_lab.broker.store import BrokerMessage

Factory = Callable[..., Any]


class KafkaEventBroker:
    """Publish and consume messages through Apache Kafka."""

    def __init__(self, bootstrap_servers: str) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers must not be empty")
        try:
            kafka = import_module("confluent_kafka")
        except ModuleNotFoundError as exc:
            raise DependencyError(
                "confluent-kafka",
                "confluent-kafka is required for Kafka broker support",
            ) from exc

        producer_candidate = getattr(kafka, "Producer", None)
        consumer_candidate = getattr(kafka, "Consumer", None)
        topic_partition_candidate = getattr(kafka, "TopicPartition", None)
        if not all(
            callable(factory)
            for factory in (producer_candidate, consumer_candidate, topic_partition_candidate)
        ):
            raise DependencyError(
                "confluent-kafka",
                "confluent-kafka does not expose Producer, Consumer, and TopicPartition",
            )

        producer_factory = cast(Factory, producer_candidate)
        self._consumer_factory = cast(Factory, consumer_candidate)
        self._topic_partition_factory = cast(Factory, topic_partition_candidate)
        try:
            self._producer = producer_factory({"bootstrap.servers": bootstrap_servers})
        except Exception as exc:
            raise ServiceConnectionError("Kafka", exc) from exc
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

        try:
            self._producer.produce(topic=topic, value=value, key=key, callback=delivered)
            remaining = self._producer.flush(10.0)
        except Exception as exc:
            raise ServiceConnectionError("Kafka", exc) from exc
        if remaining:
            raise OperationTimeoutError("Kafka publish", 10.0)
        if delivery_error:
            error = delivery_error[0]
            raise ServiceConnectionError("Kafka", error) from error

    def consume_one(
        self,
        topic: str,
        group_id: str,
        timeout_seconds: float = 10.0,
    ) -> BrokerMessage | None:
        """Consume one message from *topic* without committing its offset."""
        if not topic.strip():
            raise ValueError("topic must not be empty")
        if not group_id.strip():
            raise ValueError("group_id must not be empty")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._close_consumer()
        try:
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
        except Exception as exc:
            raise ServiceConnectionError("Kafka", exc) from exc
        if message is None:
            return None
        error = message.error()
        if error is not None:
            broker_error = RuntimeError(str(error))
            raise ServiceConnectionError("Kafka", broker_error) from broker_error

        value = message.value()
        if value is None:
            raise ServiceConnectionError(
                "Kafka",
                RuntimeError("Kafka message payload is unexpectedly null"),
            )
        return BrokerMessage(
            topic=str(message.topic()),
            partition=int(message.partition()),
            offset=int(message.offset()),
            key=message.key(),
            value=bytes(value),
        )

    def acknowledge(self, message: BrokerMessage) -> None:
        """Synchronously commit the position immediately after *message*."""
        if self._consumer is None:
            raise RuntimeError("cannot acknowledge without an active Kafka consumer")
        if not isinstance(message, BrokerMessage):
            raise TypeError("message must be a BrokerMessage")

        next_offset = message.offset + 1
        position = self._topic_partition_factory(message.topic, message.partition, next_offset)
        try:
            self._consumer.commit(offsets=[position], asynchronous=False)
        except Exception as exc:
            raise ServiceConnectionError("Kafka", exc) from exc

    def close(self) -> None:
        """Flush producer state and close any active consumer."""
        try:
            self._producer.flush(10.0)
        except Exception as exc:
            raise ServiceConnectionError("Kafka", exc) from exc
        finally:
            self._close_consumer()

    def _close_consumer(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as exc:
                raise ServiceConnectionError("Kafka", exc) from exc
            finally:
                self._consumer = None
