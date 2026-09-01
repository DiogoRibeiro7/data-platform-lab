"""Wire-level smoke command for the local Kafka broker."""

from __future__ import annotations

import argparse
import json
import os
from uuid import uuid4

from data_platform_lab.broker import KafkaEventBroker


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify Kafka publish/consume round trip.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("DPL_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default="data-platform-lab-smoke")
    return parser


def main(argv: list[str] | None = None) -> None:
    """Publish and consume one deterministic JSON message through Kafka."""
    args = _build_parser().parse_args(argv)
    group_id = f"data-platform-lab-smoke-{uuid4()}"
    payload = json.dumps(
        {"event_id": "broker-smoke", "value": 1.0},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    broker = KafkaEventBroker(args.bootstrap_servers)
    try:
        broker.publish(args.topic, payload, key=b"broker-smoke")
        message = broker.consume_one(args.topic, group_id, timeout_seconds=15.0)
        if message is None:
            raise RuntimeError("Kafka smoke check timed out waiting for the published message")
        if message.value != payload or message.key != b"broker-smoke":
            raise RuntimeError("Kafka smoke check returned an unexpected message")

        print("=== Kafka Broker Smoke Check ===")
        print(f"Topic     : {message.topic}")
        print(f"Partition : {message.partition}")
        print(f"Offset    : {message.offset}")
        print("Round trip: ok")
    finally:
        broker.close()


if __name__ == "__main__":
    main()
