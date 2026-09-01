"""Wire-level smoke command for Iceberg analytical tables."""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime
from importlib import import_module

from data_platform_lab.iceberg import IcebergCatalogConfig, IcebergTableStore, build_catalog

_DEFAULT_DSN = "postgresql://data_platform_lab:data_platform_lab@localhost:5432/data_platform_lab"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify Iceberg catalog and S3 table storage.")
    parser.add_argument("--catalog", default="platform")
    parser.add_argument("--namespace", default="analytics")
    parser.add_argument("--table", default="events_smoke")
    parser.add_argument("--postgres-dsn", default=os.getenv("DPL_POSTGRES_DSN", _DEFAULT_DSN))
    parser.add_argument(
        "--warehouse",
        default=os.getenv("DPL_ICEBERG_WAREHOUSE", "s3://data-platform-lab/iceberg"),
    )
    parser.add_argument(
        "--s3-endpoint",
        default=os.getenv("DPL_S3_ENDPOINT_URL", "http://localhost:3900"),
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Create, append, and scan one Iceberg table through the local platform services."""
    args = _build_parser().parse_args(argv)
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")

    try:
        pa = import_module("pyarrow")
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyArrow is required for the Iceberg smoke check") from exc

    catalog = build_catalog(
        IcebergCatalogConfig(
            catalog_name=args.catalog,
            postgres_dsn=args.postgres_dsn,
            warehouse=args.warehouse,
            s3_endpoint=args.s3_endpoint,
            s3_access_key_id=access_key,
            s3_secret_access_key=secret_key,
            s3_region=os.getenv("AWS_DEFAULT_REGION", "garage"),
        )
    )
    store = IcebergTableStore(catalog)
    identifier = f"{args.namespace}.{args.table}"
    schema = pa.schema(
        [
            ("event_id", pa.string()),
            ("event_time", pa.timestamp("us", tz="UTC")),
            ("value", pa.float64()),
        ]
    )
    store.ensure_table(identifier, schema)
    batch = pa.Table.from_pylist(
        [
            {
                "event_id": "smoke-1",
                "event_time": datetime(2026, 9, 1, tzinfo=UTC),
                "value": 1.0,
            }
        ],
        schema=schema,
    )
    store.append(identifier, batch)
    count = store.row_count(identifier)
    if count < 1:
        raise RuntimeError("Iceberg smoke check failed: appended row was not visible")

    print("=== Iceberg Table Smoke Check ===")
    print(f"Table     : {identifier}")
    print(f"Warehouse : {args.warehouse}")
    print(f"Rows      : {count}")
    print("Round trip: ok")


if __name__ == "__main__":
    main()
