"""PostgreSQL metadata-store smoke command."""

from __future__ import annotations

import argparse
import os

from data_platform_lab.metadata import PostgresRunStore
from data_platform_lab.observability import RunMetadata


def main(argv: list[str] | None = None) -> None:
    """Verify PostgreSQL run-metadata persistence through the platform adapter."""
    parser = argparse.ArgumentParser(
        description="Verify PostgreSQL run metadata persistence.",
    )
    parser.add_argument(
        "--dsn",
        default=os.getenv(
            "DPL_POSTGRES_DSN",
            "postgresql://data_platform_lab:data_platform_lab@localhost:5432/data_platform_lab",
        ),
    )
    args = parser.parse_args(argv)

    store = PostgresRunStore.connect(args.dsn)
    try:
        run = RunMetadata(
            pipeline_name="platform_metadata_smoke",
            run_id="smoke",
            status="success",
            started_at="2026-09-01T00:00:00+00:00",
            ended_at="2026-09-01T00:00:01+00:00",
            duration_seconds=1.0,
            rows_read=1,
            rows_written=1,
        )
        store.save(run)
        loaded = store.get(run.pipeline_name, run.run_id)
        if loaded is None or loaded.status != "success" or loaded.rows_written != 1:
            raise RuntimeError("metadata smoke check failed")

        print("=== Metadata Store Smoke Check ===")
        print(f"Pipeline : {loaded.pipeline_name}")
        print(f"Run ID   : {loaded.run_id}")
        print("Round trip: ok")
    finally:
        store.close()


if __name__ == "__main__":
    main()
