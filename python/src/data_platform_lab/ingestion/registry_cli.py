"""Canonical registry loading, with explicitly requested pinned HTTPS extraction."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from data_platform_lab.ingestion.registry_fetch import (
    RegistryFetchOptions,
    ingest_registry_fetch,
    plan_registry_fetch,
)
from data_platform_lab.ingestion.registry_handoff import HandoffError, ingest_handoff
from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser
from data_platform_lab.storage import BlobStore, LocalBlobStore, S3BlobStore


def _build_parser() -> argparse.ArgumentParser:
    parser = _Parser(
        description="Ingest a SHA-pinned canonical registry handoff into a local or S3 store.",
        allow_abbrev=False,
    )
    parser.add_argument("--manifest", type=Path, required=True, help="Handoff v1 JSON file")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--registry-root", type=Path, help="Load from an exact pinned Git checkout")
    source.add_argument(
        "--fetch", action="store_true", help="Explicitly fetch pinned public HTTPS objects"
    )
    parser.add_argument(
        "--registry-repository", help="Independently trusted owner/repo; required with --fetch"
    )
    parser.add_argument(
        "--registry-commit", help="Independently trusted full Git SHA; required with --fetch"
    )
    parser.add_argument("--dataset-id", help="Expected canonical dataset; required with --fetch")
    parser.add_argument(
        "--timeout-seconds", type=float, help="Fetch socket timeout, at most 300 (default: 30)"
    )
    parser.add_argument(
        "--max-object-bytes", type=int, help="Fetch limit per object (default: 16777216)"
    )
    parser.add_argument(
        "--max-total-bytes", type=int, help="Fetch limit per run (default: 67108864)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate fetch selection without network access or writes",
    )
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--root", type=Path, default=Path("../data/object-store"))
    parser.add_argument("--receipt-dir", type=Path, default=Path("../data/manifests"))
    parser.add_argument("--bucket", default=os.getenv("DPL_S3_BUCKET", "data-platform-lab"))
    parser.add_argument("--endpoint-url", default=os.getenv("DPL_S3_ENDPOINT_URL"))
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION"))
    parser.add_argument("--key-prefix", default=os.getenv("DPL_S3_KEY_PREFIX", ""))
    return parser


def _store(args: argparse.Namespace) -> BlobStore:
    if args.backend == "local":
        return LocalBlobStore(args.root)
    return S3BlobStore.from_boto3(
        bucket=args.bucket,
        endpoint_url=args.endpoint_url,
        region_name=args.region,
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        key_prefix=args.key_prefix,
    )


def main(argv: list[str] | None = None) -> None:
    """Execute a manifest with SHA verification and print the receipt location."""
    try:
        args = _build_parser().parse_args(argv)
        options = None
        if args.fetch:
            options = RegistryFetchOptions(
                repository=args.registry_repository,
                commit=args.registry_commit,
                dataset_id=args.dataset_id,
                timeout_seconds=args.timeout_seconds if args.timeout_seconds is not None else 30,
                max_object_bytes=args.max_object_bytes
                if args.max_object_bytes is not None
                else 16 * 1024 * 1024,
                max_total_bytes=args.max_total_bytes
                if args.max_total_bytes is not None
                else 64 * 1024 * 1024,
            )
            plan = plan_registry_fetch(manifest_path=args.manifest, options=options)
            if args.dry_run:
                print(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "operation": "registry_fetch_plan",
                            "dataset_id": plan.dataset_id,
                            "registry": {"repository": plan.repository, "commit": plan.commit},
                            "objects": len(plan.objects),
                            "known_source_bytes": sum(
                                item.size_bytes or 0 for item in plan.objects
                            ),
                            "unknown_sizes": sum(item.size_bytes is None for item in plan.objects),
                            "timeout_seconds": options.timeout_seconds,
                            "max_object_bytes": options.max_object_bytes,
                            "max_total_bytes": options.max_total_bytes,
                        },
                        indent=2,
                    )
                )
                return
        elif args.dry_run or any(
            value is not None
            for value in (
                args.registry_repository,
                args.registry_commit,
                args.dataset_id,
                args.timeout_seconds,
                args.max_object_bytes,
                args.max_total_bytes,
            )
        ):
            raise _ArgumentError("Fetch selection, limits and --dry-run require --fetch.")
    except (_ArgumentError, HandoffError) as exc:
        print(json.dumps({"error": {"code": "invalid_arguments", "message": str(exc)}}))
        raise SystemExit(2) from None
    try:
        if options is not None:
            result = ingest_registry_fetch(
                manifest_path=args.manifest,
                options=options,
                store=_store(args),
                receipt_dir=args.receipt_dir,
            )
        else:
            result = ingest_handoff(
                manifest_path=args.manifest,
                registry_root=args.registry_root,
                store=_store(args),
                receipt_dir=args.receipt_dir,
            )
    except Exception:
        # Transport/storage exceptions may include credentials or response bodies.
        print(
            json.dumps(
                {
                    "error": {
                        "code": "ingestion_error",
                        "message": "Registry ingestion did not complete.",
                    }
                }
            )
        )
        raise SystemExit(1) from None
    print(
        json.dumps(
            {
                "dataset_id": result.dataset_id,
                "run_id": result.run_id,
                "files_published": result.files_published,
                "files_reused": result.files_reused,
                "bytes_verified": result.bytes_verified,
                "receipt_path": str(result.receipt_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
