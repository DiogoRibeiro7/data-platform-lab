"""Bounded, read-only S3 verification and whole-batch evidence preflight."""

from __future__ import annotations

import hashlib
import json
from io import BytesIO
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_ons_pay_load_verification import NOW, _snapshot
from test_ons_pay_transform import _loaded
from test_registry_fetch import PAYLOAD, SECRET
from test_storage_s3 import FakeS3Client, FakeS3Error

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability import ons_pay_load_verification as verification
from data_platform_lab.orchestration import ons_pay_transform as transform
from data_platform_lab.storage import LocalBlobStore, S3BlobStore
from data_platform_lab.storage.config import S3Storage


class Body(BytesIO):
    """Record every bounded read and simulate broken streaming responses."""

    def __init__(self, payload: bytes, fault: str = "") -> None:
        super().__init__(payload)
        self.fault = fault
        self.sizes: list[int] = []

    def read(self, size: int | None = -1) -> bytes:
        assert size is not None and 0 < size <= 64 * 1024
        self.sizes.append(size)
        if self.fault == "read_error" or (self.fault == "partial_error" and self.tell()):
            raise OSError(SECRET)
        if self.fault == "invalid_body":
            return SECRET  # type: ignore[return-value]
        if self.fault == "oversized_chunk":
            return b"x" * (size + 1)
        if self.fault in {"short_chunks", "partial_error"}:
            size = min(size, 13)
        return super().read(size)

    def close(self) -> None:
        super().close()
        if self.fault == "close_error":
            raise OSError(SECRET)


class InspectClient(FakeS3Client):
    """Only GET is allowed during inspection; faults affect the first source only."""

    def __init__(self) -> None:
        super().__init__()
        self.calls: list[dict[str, object]] = []
        self.bodies: list[Body] = []
        self.fault = ""
        self.put_object = Mock(side_effect=AssertionError("write"))  # type: ignore[method-assign]
        self.head_object = Mock(side_effect=AssertionError("HEAD"))  # type: ignore[method-assign]
        self.list_objects_v2 = Mock(side_effect=AssertionError("list"))  # type: ignore[method-assign]

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        fault = self.fault if len(self.calls) == 1 else ""
        if fault in {"missing", "denied", "bucket_missing", "unknown_404"}:
            code, status = {
                "missing": ("NoSuchKey", 404),
                "denied": ("AccessDenied", 403),
                "bucket_missing": ("NoSuchBucket", 404),
                "unknown_404": ("ServiceFailure", 404),
            }[fault]
            raise FakeS3Error(code, status)
        if fault == "transport":
            raise RuntimeError(SECRET)
        payload = self.objects[(str(kwargs["Bucket"]), str(kwargs["Key"]))]
        size = len(payload)
        if fault == "checksum":
            payload = b"x" * size
        elif fault == "short_body":
            payload = payload[:-1]
        elif fault == "long_body":
            payload += b"x"
        body = Body(payload, fault)
        self.bodies.append(body)
        lengths: dict[str, object] = {
            "short_size": size - 1,
            "large_size": 10**12,
            "negative_size": -1,
            "bool_size": True,
            "string_size": str(size),
            "missing_size": None,
        }
        return {"Body": body, "ContentLength": lengths.get(fault, size)}


def _prepared(tmp_path: Path, payload: bytes | None = None) -> Any:
    load = _loaded(
        tmp_path,
        payload=payload if payload is not None else PAYLOAD.replace(b"200,Pounds", b"205,Pounds"),
    )
    config = S3Storage(
        bucket="platform", endpoint_url="http://localhost:3900", region="garage", key_prefix="ons"
    )
    client = InspectClient()
    plan = transform.plan_ons_pay_transform(load_audit=load.receipt_path)
    for item in plan.sources:
        key = item.source.storage_key
        client.objects[(config.bucket, config.key_prefix + "/" + key)] = (
            tmp_path / "objects" / key
        ).read_bytes()
    return load, config, client


def _verify(load: Any, config: S3Storage, client: InspectClient, **options: Any) -> Any:
    with patch.object(S3Storage, "open", return_value=S3BlobStore(client, config.bucket, "ons")):
        return verification.verify_ons_pay_load(
            load_audit=load.receipt_path, object_store=config, now=NOW, **options
        )


def _args(load: Any, *extra: str) -> list[str]:
    return [
        "ons-pay-load-verify",
        "--load-audit",
        str(load.receipt_path),
        "--backend",
        "s3",
        "--bucket",
        "platform",
        "--key-prefix",
        "ons",
        *extra,
    ]


@pytest.mark.parametrize("payload", [b"", PAYLOAD, PAYLOAD * 200])
def test_s3_verification_is_read_only_streamed_and_preserves_evidence(
    tmp_path: Path, payload: bytes
) -> None:
    load, config, client = _prepared(tmp_path, payload)
    before = _snapshot(tmp_path)
    objects = client.objects.copy()
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("source HTTP")),
        patch.object(LocalBlobStore, "__init__", side_effect=AssertionError("local storage")),
        patch.object(transform, "curate_ons_pay", side_effect=AssertionError("analytics")),
        patch.object(transform, "guard_local_roots", side_effect=AssertionError("guard")),
        patch.object(Path, "mkdir", side_effect=AssertionError("directory")),
    ):
        report = _verify(load, config, client)
    assert report.status == "healthy" and report.writes is False
    assert report.objects_expected == report.objects_verified == 2
    assert report.bytes_verified == len(PAYLOAD) + len(payload)
    assert report.load_audit_sha256 == hashlib.sha256(load.receipt_path.read_bytes()).hexdigest()
    assert report.load_job_sha256 == "c" * 64
    assert report.to_dict()["object_store"] == config.to_dict()
    assert "object_root" not in report.to_dict()
    assert all(item.observed_sha256 == item.source_sha256 for item in report.sources)
    assert len(client.calls) == 2 and all(body.closed for body in client.bodies)
    assert all(set(call) == {"Bucket", "Key"} for call in client.calls)
    assert all(str(call["Key"]).startswith("ons/canonical/") for call in client.calls)
    assert _snapshot(tmp_path) == before and client.objects == objects
    client.put_object.assert_not_called()
    client.head_object.assert_not_called()
    client.list_objects_v2.assert_not_called()


@pytest.mark.parametrize(
    "fault,expected",
    [
        ("missing", "missing"),
        ("denied", "unreadable"),
        ("bucket_missing", "unreadable"),
        ("unknown_404", "unreadable"),
        ("transport", "unreadable"),
        ("checksum", "checksum_mismatch"),
        ("short_size", "size_mismatch"),
        ("large_size", "size_mismatch"),
        ("short_body", "size_mismatch"),
        ("long_body", "size_mismatch"),
        ("negative_size", "unreadable"),
        ("bool_size", "unreadable"),
        ("string_size", "unreadable"),
        ("missing_size", "unreadable"),
        ("invalid_body", "unreadable"),
        ("oversized_chunk", "unreadable"),
        ("read_error", "unreadable"),
        ("partial_error", "unreadable"),
        ("close_error", "unreadable"),
    ],
)
def test_s3_failures_close_bodies_continue_batch_and_never_expose_sdk_errors(
    tmp_path: Path, fault: str, expected: str
) -> None:
    load, config, client = _prepared(tmp_path)
    client.fault = fault
    report = _verify(load, config, client)
    check = report.sources[0]
    assert report.status == "unhealthy" and check.integrity == expected
    assert report.sources[1].integrity == "verified" and report.objects_verified == 1
    assert check.bytes_read <= len(PAYLOAD) + 1
    assert SECRET not in json.dumps(report.to_dict())
    assert all(body.closed for body in client.bodies) and len(client.calls) == 2
    if fault in {"short_size", "large_size", "negative_size", "bool_size", "missing_size"}:
        assert not client.bodies[0].sizes and check.bytes_read == 0
    if fault == "partial_error":
        assert check.bytes_read == 13
    if fault != "checksum":
        assert check.observed_sha256 is None


def test_s3_short_reads_are_hashed_until_eof_and_freshness_is_independent(tmp_path: Path) -> None:
    load, config, client = _prepared(tmp_path)
    client.fault = "short_chunks"
    report = _verify(load, config, client, max_source_age_days=10)
    assert report.status == "unhealthy" and report.objects_verified == 2
    assert all(
        item.integrity == "verified" and item.freshness_passed is False for item in report.sources
    )
    assert len(client.bodies[0].sizes) > 2


@pytest.mark.parametrize(
    "options",
    [
        {"max_object_bytes": 0},
        {"max_total_bytes": True},
        {"max_object_bytes": len(PAYLOAD) - 1},
        {"max_total_bytes": 2 * len(PAYLOAD) - 1},
        {"expected_load_sha256": "0" * 64},
        {"max_source_age_days": -1},
        {"object_root": Path("objects")},
    ],
)
def test_s3_invalid_policy_and_budget_fail_before_client_creation(
    tmp_path: Path, options: dict[str, Any]
) -> None:
    load, config, _ = _prepared(tmp_path)
    with (
        patch.object(S3Storage, "open") as factory,
        pytest.raises(verification.LoadVerificationError),
    ):
        verification.verify_ons_pay_load(
            load_audit=load.receipt_path, object_store=config, **options
        )
    factory.assert_not_called()


@pytest.mark.parametrize("fault", ["missing_receipt", "failed", "counts", "future"])
def test_s3_entire_batch_evidence_is_checked_before_client_creation(
    tmp_path: Path, fault: str
) -> None:
    load, config, _ = _prepared(tmp_path)
    audit = json.loads(load.receipt_path.read_bytes())
    if fault == "missing_receipt":
        load.sources[1].ingestion_receipt_path.unlink()
    elif fault == "failed":
        audit["status"] = "failed"
    elif fault == "counts":
        audit["metrics"]["bytes_verified"] += 1
    load.receipt_path.write_text(json.dumps(audit), encoding="utf-8")
    now = NOW.replace(day=1) if fault == "future" else NOW
    with (
        patch.object(S3Storage, "open") as factory,
        pytest.raises(verification.LoadVerificationError),
    ):
        verification.verify_ons_pay_load(load_audit=load.receipt_path, object_store=config, now=now)
    factory.assert_not_called()


@pytest.mark.parametrize(
    "settings",
    [
        [],
        ["--backend", "s3"],
        ["--bucket", "platform"],
        ["--backend", "local", "--bucket", "platform"],
        ["--backend", "s3", "--bucket", "platform", "--object-root", "objects"],
        [
            "--backend",
            "s3",
            "--bucket",
            "platform",
            "--endpoint-url",
            f"https://u:{SECRET}@example.com",
        ],
        ["--backend", "s3", "--bucket", "platform", "--key-prefix", "../escape"],
        ["--secret-access-key", SECRET],
        ["--backend", SECRET],
    ],
)
def test_cli_rejects_ambiguous_locations_and_credential_fields_without_connecting(
    tmp_path: Path, capsys: Any, settings: list[str]
) -> None:
    load, _, _ = _prepared(tmp_path)
    with patch.object(S3Storage, "open") as factory, pytest.raises(SystemExit) as error:
        platform_main(["ons-pay-load-verify", "--load-audit", str(load.receipt_path), *settings])
    output = capsys.readouterr()
    assert error.value.code == 2 and not output.out and SECRET not in output.err
    factory.assert_not_called()


def test_cli_s3_client_initialization_failure_is_sanitized_and_creates_no_report(
    tmp_path: Path, capsys: Any
) -> None:
    load, _, _ = _prepared(tmp_path)
    before = _snapshot(tmp_path)
    with (
        patch.object(S3Storage, "open", side_effect=RuntimeError(SECRET)),
        pytest.raises(SystemExit) as error,
    ):
        platform_main(_args(load))
    output = capsys.readouterr()
    assert error.value.code == 1 and not output.out
    assert output.err == "Unable to access verification storage.\n"
    assert _snapshot(tmp_path) == before


def test_cli_s3_reports_success_and_failure_without_changing_files(
    tmp_path: Path, capsys: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    load, config, client = _prepared(tmp_path)
    monkeypatch.chdir(tmp_path)
    before = _snapshot(tmp_path)
    with patch.object(S3Storage, "open", return_value=S3BlobStore(client, config.bucket, "ons")):
        platform_main(
            _args(load, "--load-sha256", hashlib.sha256(load.receipt_path.read_bytes()).hexdigest())
        )
        report = json.loads(capsys.readouterr().out)
        assert report["status"] == "healthy" and report["writes"] is False
        assert report["object_store"] == {
            "backend": "s3",
            "bucket": "platform",
            "key_prefix": "ons",
        }
        assert "object_root" not in report
        client.calls.clear()
        client.fault = "denied"
        with pytest.raises(SystemExit) as error:
            platform_main(_args(load))
        assert error.value.code == 1
        report = json.loads(capsys.readouterr().out)
        assert report["status"] == "unhealthy" and report["objects_verified"] == 1
    assert _snapshot(tmp_path) == before
