"""Tests for the S3-compatible object-store adapter."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from dataexcept import StorageError

from data_platform_lab.storage import BlobStore, LocalBlobStore, S3BlobStore
from data_platform_lab.storage.cli import _build_parser, run_storage_smoke


class FakeS3Error(Exception):
    """Minimal botocore-style exception for adapter tests."""

    def __init__(self, code: str, status: int) -> None:
        super().__init__(code)
        self.response = {
            "Error": {"Code": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


class FakeS3Client:
    """In-memory S3 client implementing the calls used by S3BlobStore."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.list_calls: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> dict[str, object]:
        bucket = str(kwargs["Bucket"])
        key = str(kwargs["Key"])
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        self.objects[(bucket, key)] = body
        return {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        object_key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if object_key not in self.objects:
            raise FakeS3Error("NoSuchKey", 404)
        return {"Body": BytesIO(self.objects[object_key])}

    def head_object(self, **kwargs: object) -> dict[str, object]:
        object_key = (str(kwargs["Bucket"]), str(kwargs["Key"]))
        if object_key not in self.objects:
            raise FakeS3Error("NotFound", 404)
        return {"ContentLength": len(self.objects[object_key])}

    def list_objects_v2(self, **kwargs: object) -> dict[str, Any]:
        self.list_calls.append(dict(kwargs))
        bucket = str(kwargs["Bucket"])
        prefix = str(kwargs["Prefix"])
        keys = sorted(
            key
            for (stored_bucket, key) in self.objects
            if stored_bucket == bucket and key.startswith(prefix)
        )
        start = 1 if kwargs.get("ContinuationToken") == "page-2" else 0
        page = keys[start : start + 1]
        truncated = start + 1 < len(keys)
        return {
            "Contents": [{"Key": key, "Size": len(self.objects[(bucket, key)])} for key in page],
            "IsTruncated": truncated,
            "NextContinuationToken": "page-2" if truncated else None,
        }


def test_s3_store_implements_blob_contract_and_namespace() -> None:
    client = FakeS3Client()
    store = S3BlobStore(client, bucket="platform", key_prefix="lab")
    first = store.put_bytes("bronze/events/b.jsonl", b"bb")
    store.put_bytes("bronze/events/a.jsonl", b"a")
    assert isinstance(store, BlobStore)
    assert client.objects[("platform", "lab/bronze/events/b.jsonl")] == b"bb"
    assert store.get_bytes("bronze/events/a.jsonl") == b"a"
    assert store.exists(first.key)
    assert not store.exists("bronze/events/missing.jsonl")
    assert [item.key for item in store.list_objects("bronze/events")] == [
        "bronze/events/a.jsonl",
        "bronze/events/b.jsonl",
    ]
    assert len(client.list_calls) == 2


def test_s3_exists_classifies_non_missing_backend_errors() -> None:
    class DeniedClient(FakeS3Client):
        def head_object(self, **kwargs: object) -> dict[str, object]:
            raise FakeS3Error("AccessDenied", 403)

    with pytest.raises(StorageError, match="AccessDenied") as error:
        S3BlobStore(DeniedClient(), bucket="platform").exists("private/object")

    assert isinstance(error.value.__cause__, FakeS3Error)
    assert error.value.operation == "stat"
    assert error.value.location == "s3://platform/private/object"


def test_s3_get_classifies_backend_read_errors() -> None:
    with pytest.raises(StorageError, match="NoSuchKey") as error:
        S3BlobStore(FakeS3Client(), bucket="platform").get_bytes("missing")

    assert isinstance(error.value.__cause__, FakeS3Error)
    assert error.value.operation == "read"


def test_storage_smoke_uses_the_common_contract(tmp_path: Path) -> None:
    report = run_storage_smoke(LocalBlobStore(tmp_path / "objects"))
    assert report["round_trip"] is True
    assert report["listed"] is True
    assert report["key"] == "_platform/smoke.txt"


def test_storage_smoke_derives_prefix_from_custom_key(tmp_path: Path) -> None:
    report = run_storage_smoke(LocalBlobStore(tmp_path / "objects"), "gold/check.txt")
    assert report["round_trip"] is True
    assert report["listed"] is True
    assert report["key"] == "gold/check.txt"


def test_storage_cli_does_not_force_local_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    args = _build_parser().parse_args(["--backend", "s3"])
    assert args.region is None
