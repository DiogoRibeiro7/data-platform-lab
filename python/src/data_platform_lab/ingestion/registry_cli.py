"""Offline-first canonical registry ingestion command."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from data_platform_lab.ingestion.registry_handoff import ingest_handoff
from data_platform_lab.storage import BlobStore, LocalBlobStore, S3BlobStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest a SHA-pinned canonical registry handoff into a local or S3 store."
    )
    parser.add_argument("--manifest", type=Path, required=True, help="Handoff v1 JSON file")
    parser.add_argument(
        "--registry-root", type=Path, required=True, help="Exact pinned Git checkout"
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
    args = _build_parser().parse_args(argv)
    result = ingest_handoff(
        manifest_path=args.manifest,
        registry_root=args.registry_root,
        store=_store(args),
        receipt_dir=args.receipt_dir,
    )
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
