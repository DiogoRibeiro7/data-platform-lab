"""Local, machine-invokable ONS pay refresh/backfill and SLA preflight CLI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform_lab.orchestration.ons_pay_refresh import (
    RefreshError,
    RefreshSource,
    plan_ons_pay_refresh,
    refresh_ons_pay,
)
from data_platform_lab.storage import LocalBlobStore


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-refresh",
        description="Verify, ingest and curate one or more explicitly pinned ONS pay vintages.",
    )
    parser.add_argument(
        "--source",
        nargs=2,
        metavar=("HANDOFF_JSON", "PINNED_CHECKOUT"),
        type=Path,
        action="append",
        required=True,
        help="Repeat for historical snapshots, in intended backfill order.",
    )
    parser.add_argument("--object-root", type=Path, default=Path("../data/object-store"))
    parser.add_argument("--output-root", type=Path, default=Path("../data"))
    parser.add_argument(
        "--max-source-age-days",
        type=int,
        default=None,
        help="Optional retrieval-age SLA; omit for intentionally historical backfills.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read-only manifest/freshness preflight; does not verify Git blobs.",
    )
    args = parser.parse_args(argv)
    sources = [RefreshSource(manifest, checkout) for manifest, checkout in args.source]
    for destination in (args.object_root, args.output_root):
        if any(
            destination.resolve().is_relative_to(source.registry_root.resolve())
            for source in sources
        ):
            parser.error("object and output roots must remain outside canonical Git checkouts")

    if args.dry_run:
        try:
            plan = plan_ons_pay_refresh(sources, max_source_age_days=args.max_source_age_days)
        except RefreshError as exc:
            parser.exit(2, f"refresh preflight failed: {exc}\n")
        print(
            json.dumps(
                {
                    "status": "planned",
                    "writes": False,
                    "max_source_age_days": args.max_source_age_days,
                    "vintages": [
                        {
                            "registry_commit": item.registry_commit,
                            "source_sha256": item.source_sha256,
                            "retrieved_at": item.retrieved_at,
                            "source_age_days": item.age_days,
                            "manifest_path": str(item.source.manifest_path),
                        }
                        for item in plan
                    ],
                },
                indent=2,
            )
        )
        return

    try:
        result = refresh_ons_pay(
            sources=sources,
            store=LocalBlobStore(args.object_root),
            output_root=args.output_root,
            max_source_age_days=args.max_source_age_days,
        )
    except RefreshError as exc:
        parser.exit(1, f"{exc}\n")
    print(
        json.dumps(
            {
                "status": "success",
                "run_id": result.run_id,
                "vintages_completed": len(result.sources),
                "objects_published": sum(item.objects_published for item in result.sources),
                "objects_reused": sum(item.objects_reused for item in result.sources),
                "parquet_published": sum(item.parquet_published for item in result.sources),
                "parquet_reused": sum(item.parquet_reused for item in result.sources),
                "receipt_path": str(result.receipt_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
