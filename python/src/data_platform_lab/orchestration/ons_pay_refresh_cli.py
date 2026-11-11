"""Pinned checkout or HTTPS ONS pay refresh/backfill and SLA preflight CLI."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.curation.ons_pay import DATASET_ID
from data_platform_lab.ingestion.registry_fetch import RegistryFetchOptions
from data_platform_lab.orchestration.ons_pay_refresh import (
    RefreshError,
    RefreshInput,
    RefreshSource,
    RemoteRefreshSource,
    plan_ons_pay_refresh,
    refresh_ons_pay,
)
from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser
from data_platform_lab.storage import LocalBlobStore


def main(argv: list[str] | None = None) -> None:
    parser = _Parser(
        prog="data-platform-lab ons-pay-refresh",
        description="Verify, ingest and curate one or more explicitly pinned ONS pay vintages.",
        allow_abbrev=False,
    )
    source_mode = parser.add_mutually_exclusive_group(required=True)
    source_mode.add_argument(
        "--source",
        nargs=2,
        metavar=("HANDOFF_JSON", "PINNED_CHECKOUT"),
        type=Path,
        action="append",
        help="Repeat for historical snapshots, in intended backfill order.",
    )
    source_mode.add_argument(
        "--fetch-source",
        nargs=2,
        metavar=("HANDOFF_JSON", "REGISTRY_COMMIT"),
        action="append",
        help="Explicitly fetch a pinned ONS registry snapshot; repeat in backfill order.",
    )
    parser.add_argument(
        "--timeout-seconds", type=float, help="Fetch socket timeout, at most 300 (default: 30)"
    )
    parser.add_argument(
        "--max-attempts", type=int, help="Fetch attempts per object, 1-5 (default: 1; no retries)"
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        help="Initial fetch retry delay, at most 30 (default: 1)",
    )
    parser.add_argument(
        "--max-object-bytes", type=int, help="Fetch budget per object (default: 16777216)"
    )
    parser.add_argument(
        "--max-source-bytes", type=int, help="Fetch budget per handoff (default: 67108864)"
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
        help="Read-only manifest/freshness preflight; no source reads, network access or writes.",
    )
    try:
        args = parser.parse_args(argv)
        sources: list[RefreshInput]
        if args.fetch_source:
            sources = [
                RemoteRefreshSource(
                    Path(manifest),
                    RegistryFetchOptions(
                        repository="DiogoRibeiro7/data",
                        commit=commit,
                        dataset_id=DATASET_ID,
                        timeout_seconds=args.timeout_seconds
                        if args.timeout_seconds is not None
                        else 30,
                        max_attempts=args.max_attempts if args.max_attempts is not None else 1,
                        retry_delay_seconds=args.retry_delay_seconds
                        if args.retry_delay_seconds is not None
                        else 1,
                        max_object_bytes=args.max_object_bytes
                        if args.max_object_bytes is not None
                        else 16 * 1024 * 1024,
                        max_total_bytes=args.max_source_bytes
                        if args.max_source_bytes is not None
                        else 64 * 1024 * 1024,
                    ),
                )
                for manifest, commit in args.fetch_source
            ]
        else:
            if any(
                value is not None
                for value in (
                    args.timeout_seconds,
                    args.max_object_bytes,
                    args.max_source_bytes,
                    args.max_attempts,
                    args.retry_delay_seconds,
                )
            ):
                raise _ArgumentError("Fetch limits require --fetch-source.")
            sources = [RefreshSource(manifest, checkout) for manifest, checkout in args.source]
        for destination in (args.object_root, args.output_root):
            if any(
                destination.resolve().is_relative_to(source.registry_root.resolve())
                for source in sources
                if isinstance(source, RefreshSource)
            ):
                parser.error("object and output roots must remain outside canonical Git checkouts")
    except _ArgumentError as exc:
        parser.exit(2, f"{exc}\n")

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
                            "manifest_sha256": item.manifest_sha256,
                            "source_transport": "https"
                            if isinstance(item.source, RemoteRefreshSource)
                            else "checkout",
                            **(
                                {
                                    "max_attempts": item.source.fetch_options.max_attempts,
                                    "retry_delay_seconds": (
                                        item.source.fetch_options.retry_delay_seconds
                                    ),
                                }
                                if isinstance(item.source, RemoteRefreshSource)
                                else {}
                            ),
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
