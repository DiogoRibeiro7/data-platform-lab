"""End-to-end local platform failure/recovery smoke command."""

from __future__ import annotations

import argparse
import json
import os

from data_platform_lab.broker import KafkaEventBroker
from data_platform_lab.iceberg import IcebergCatalogConfig, IcebergTableStore, build_catalog
from data_platform_lab.metadata import PostgresRunStore
from data_platform_lab.recovery import RecoverableIngestionPipeline
from data_platform_lab.storage import S3BlobStore

_DEFAULT_DSN = "postgresql://data_platform_lab:data_platform_lab@localhost:5432/data_platform_lab"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prove broker-to-Iceberg recovery after an injected post-commit failure."
    )
    parser.add_argument("--topic", default="data-platform-lab-recovery")
    parser.add_argument("--group-id", default="data-platform-lab-recovery-smoke")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--postgres-dsn", default=os.getenv("DPL_POSTGRES_DSN", _DEFAULT_DSN))
    parser.add_argument(
        "--warehouse",
        default=os.getenv("DPL_ICEBERG_WAREHOUSE", "s3://data-platform-lab/iceberg"),
    )
    parser.add_argument(
        "--s3-endpoint",
        default=os.getenv("DPL_S3_ENDPOINT_URL", "http://localhost:3900"),
    )
    parser.add_argument("--table", default="analytics.sensor_events")
    return parser


def main(argv: list[str] | None = None) -> None:
    """Run one deliberate failure and prove replay completes without duplication."""
    args = _build_parser().parse_args(argv)
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")

    broker = KafkaEventBroker(args.bootstrap_servers)
    run_store = PostgresRunStore.connect(args.postgres_dsn)
    blob_store = S3BlobStore.from_boto3(
        bucket="data-platform-lab",
        endpoint_url=args.s3_endpoint,
        region_name=os.getenv("AWS_DEFAULT_REGION", "garage"),
        access_key_id=access_key,
        secret_access_key=secret_key,
        key_prefix="recovery",
    )
    iceberg_store = IcebergTableStore(
        build_catalog(
            IcebergCatalogConfig(
                catalog_name="recovery",
                postgres_dsn=args.postgres_dsn,
                warehouse=args.warehouse,
                s3_endpoint=args.s3_endpoint,
                s3_access_key_id=access_key,
                s3_secret_access_key=secret_key,
                s3_region=os.getenv("AWS_DEFAULT_REGION", "garage"),
            )
        )
    )
    pipeline = RecoverableIngestionPipeline(
        broker=broker,
        run_store=run_store,
        blob_store=blob_store,
        iceberg_store=iceberg_store,
        table_identifier=args.table,
    )

    payload = json.dumps(
        {
            "sensor_id": "recovery-sensor",
            "type": "temperature",
            "value": 22.25,
            "unit": "C",
            "location": "platform-lab",
            "timestamp": "2026-09-01T12:00:00+00:00",
        },
        separators=(",", ":"),
    ).encode()

    try:
        broker.publish(args.topic, payload, key=b"recovery-sensor")
        first = broker.consume_one(args.topic, args.group_id)
        if first is None:
            raise RuntimeError("recovery smoke could not consume the published message")

        try:
            pipeline.process(first, fail_after_iceberg=True)
        except RuntimeError as exc:
            if "injected failure after Iceberg commit" not in str(exc):
                raise
        else:
            raise RuntimeError("recovery smoke expected the injected failure")

        replay = broker.consume_one(args.topic, args.group_id)
        if replay is None:
            raise RuntimeError("unacknowledged Kafka message was not replayed")
        if (replay.topic, replay.partition, replay.offset) != (
            first.topic,
            first.partition,
            first.offset,
        ):
            raise RuntimeError("Kafka replay returned a different broker position")

        result = pipeline.process(replay)
        if result.appended:
            raise RuntimeError("recovery replay appended a duplicate Iceberg row")
        if iceberg_store.row_count(args.table) != 1:
            raise RuntimeError("recovery smoke expected exactly one Iceberg row")

        metadata = run_store.get("broker_to_iceberg", result.ingestion_id)
        if metadata is None or metadata.status != "success":
            raise RuntimeError("recovery smoke did not finish with successful run metadata")
        if not blob_store.exists(result.raw_object_key):
            raise RuntimeError("recovery smoke raw object is missing from object storage")

        print("=== End-to-End Recovery Smoke ===")
        print(f"Broker position : {result.ingestion_id}")
        print(f"Raw object      : recovery/{result.raw_object_key}")
        print(f"Iceberg table   : {args.table}")
        print("Iceberg rows    : 1")
        print("Replay duplicate: no")
        print("Final status    : success")
        print("Kafka ack       : committed after durable recovery")
    finally:
        broker.close()
        run_store.close()


if __name__ == "__main__":
    main()
