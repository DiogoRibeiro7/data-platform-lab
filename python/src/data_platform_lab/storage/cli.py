"""Command-line smoke check for local and S3-compatible storage adapters."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from data_platform_lab.storage.local import BlobStore, LocalBlobStore
from data_platform_lab.storage.s3 import S3BlobStore

_SMOKE_PAYLOAD = b"data-platform-lab-storage-smoke\n"


def run_storage_smoke(store: BlobStore, key: str = "_platform/smoke.txt") -> dict[str, Any]:
    """Exercise the minimal blob-store contract and return a compact report."""
    stored = store.put_bytes(key, _SMOKE_PAYLOAD)
    round_trip = store.get_bytes(key)
    listed = [item.key for item in store.list_objects("_platform")]

    if round_trip != _SMOKE_PAYLOAD:
        raise RuntimeError("storage smoke check failed: round-trip payload changed")
    if stored.key not in listed:
        raise RuntimeError("storage smoke check failed: stored key was not listed")
    if not store.exists(stored.key):
        raise RuntimeError("storage smoke check failed: stored key does not exist")

    return {
        "key": stored.key,
        "size_bytes": stored.size_bytes,
        "round_trip": True,
        "listed": True,
    }


def _build_parser() -> argparse.ArgumentParser:
    """Build the storage smoke-check parser."""
    parser = argparse.ArgumentParser(description="Verify a local or S3-compatible blob store.")
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--root", type=Path, default=Path("../data/object-store"))
    parser.add_argument("--bucket", default=os.getenv("DPL_S3_BUCKET", "data-platform-lab"))
    parser.add_argument("--endpoint-url", default=os.getenv("DPL_S3_ENDPOINT_URL"))
    parser.add_argument("--region", default=os.getenv("AWS_DEFAULT_REGION", "garage"))
    parser.add_argument("--key-prefix", default=os.getenv("DPL_S3_KEY_PREFIX", ""))
    parser.add_argument("--smoke-key", default="_platform/smoke.txt")
    return parser


def _build_store(args: argparse.Namespace) -> BlobStore:
    """Construct the selected storage adapter from CLI options and environment."""
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
    """Run the storage smoke check and print its result."""
    args = _build_parser().parse_args(argv)
    report = run_storage_smoke(_build_store(args), args.smoke_key)

    print("=== Storage Smoke Check ===")
    print(f"Backend    : {args.backend}")
    print(f"Key        : {report['key']}")
    print(f"Bytes      : {report['size_bytes']}")
    print("Round trip : ok")
    print("Listing    : ok")


if __name__ == "__main__":
    main()
