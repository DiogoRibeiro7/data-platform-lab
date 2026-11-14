"""Explicit, credential-free S3 selection for saved ONS jobs and command lines."""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from data_platform_lab.storage.local import normalize_key
from data_platform_lab.storage.s3 import S3BlobStore


class StorageConfigurationError(ValueError):
    """Storage selection is invalid; diagnostics never echo configuration values."""


@dataclass(frozen=True, slots=True)
class S3Storage:
    """Serializable storage location; authentication stays in the SDK credential chain."""

    bucket: str
    endpoint_url: str | None = None
    region: str | None = None
    key_prefix: str = ""

    def __post_init__(self) -> None:
        try:
            if not isinstance(self.bucket, str) or not re.fullmatch(
                r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", self.bucket
            ):
                raise ValueError
            if self.region is not None and (
                not isinstance(self.region, str)
                or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", self.region)
            ):
                raise ValueError
            if not isinstance(self.key_prefix, str) or len(self.key_prefix) > 1024:
                raise ValueError
            if self.key_prefix and (
                normalize_key(self.key_prefix) != self.key_prefix
                or any(ord(char) < 32 or ord(char) == 127 for char in self.key_prefix)
            ):
                raise ValueError
            if self.endpoint_url is not None:
                if (
                    not isinstance(self.endpoint_url, str)
                    or len(self.endpoint_url) > 2048
                    or any(char.isspace() or ord(char) < 32 for char in self.endpoint_url)
                ):
                    raise ValueError
                endpoint = urlsplit(self.endpoint_url)
                if (
                    endpoint.scheme not in {"http", "https"}
                    or not endpoint.hostname
                    or endpoint.username is not None
                    or endpoint.password is not None
                    or endpoint.path not in {"", "/"}
                    or endpoint.query
                    or endpoint.fragment
                ):
                    raise ValueError
                # Accessing port also rejects malformed or out-of-range port numbers.
                _ = endpoint.port
        except (ValueError, TypeError):
            raise StorageConfigurationError(
                "Invalid S3 bucket, endpoint, region or key prefix."
            ) from None

    def to_dict(self) -> dict[str, Any]:
        """Describe the selected location without reading credentials or constructing a client."""
        return {
            "backend": "s3",
            "bucket": self.bucket,
            "key_prefix": self.key_prefix,
            **({"endpoint_url": self.endpoint_url} if self.endpoint_url is not None else {}),
            **({"region": self.region} if self.region is not None else {}),
        }

    def open(self) -> S3BlobStore:
        """Construct the existing adapter only when execution needs raw storage."""
        return S3BlobStore.from_boto3(
            bucket=self.bucket,
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            key_prefix=self.key_prefix,
        )


def add_s3_arguments(parser: argparse.ArgumentParser) -> None:
    """Add explicit S3 selection without ambient location defaults or credential flags."""
    parser.add_argument(
        "--backend", choices=("local", "s3"), help="Raw storage backend (default: local)."
    )
    parser.add_argument("--bucket", help="Explicit bucket required with --backend s3.")
    parser.add_argument("--endpoint-url", help="Optional S3-compatible HTTP(S) endpoint.")
    parser.add_argument("--region", help="Optional S3 signing region.")
    parser.add_argument("--key-prefix", help="Optional literal S3 key prefix.")


def s3_from_arguments(args: argparse.Namespace) -> S3Storage | None:
    """Validate mutually exclusive local/S3 locations without opening either store."""
    if args.backend != "s3":
        if any(
            value is not None
            for value in (args.bucket, args.endpoint_url, args.region, args.key_prefix)
        ):
            raise StorageConfigurationError("S3 settings require --backend s3.")
        return None
    if args.object_root is not None:
        raise StorageConfigurationError("--object-root cannot accompany --backend s3.")
    return S3Storage(
        bucket=args.bucket,
        endpoint_url=args.endpoint_url,
        region=args.region,
        key_prefix=args.key_prefix if args.key_prefix is not None else "",
    )
