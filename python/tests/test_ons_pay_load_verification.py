"""Read-only raw integrity checks, source-age gates and bounded local inspection."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_local_run_guard import _child_options, _holding_roots
from test_ons_pay_transform import _loaded
from test_registry_fetch import PAYLOAD, SECRET

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.curation import ons_pay
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability import ons_pay_load_verification as verification
from data_platform_lab.orchestration import ons_pay_transform as transform
from data_platform_lab.storage import LocalBlobStore

NOW = datetime(2026, 11, 20, tzinfo=UTC)


def _load(tmp_path: Path) -> Any:
    return _loaded(tmp_path, payload=PAYLOAD.replace(b"200,Pounds", b"205,Pounds"))


def _verify(tmp_path: Path, load: Any, **kwargs: Any) -> verification.LoadVerificationReport:
    return verification.verify_ons_pay_load(
        load_audit=load.receipt_path, object_root=tmp_path / "objects", now=NOW, **kwargs
    )


def _raw(tmp_path: Path, load: Any, index: int = 0) -> Path:
    receipt = json.loads(load.sources[index].ingestion_receipt_path.read_bytes())
    return tmp_path / "objects" / receipt["objects"][0]["storage_key"]


def _args(tmp_path: Path, load: Any, *extra: str) -> list[str]:
    return [
        "ons-pay-load-verify",
        "--load-audit",
        str(load.receipt_path),
        "--object-root",
        str(tmp_path / "objects"),
        *extra,
    ]


def _snapshot(root: Path) -> dict[Path, tuple[bytes, int]]:
    return {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    }


@pytest.mark.parametrize("mixed", [False, True])
def test_verification_preserves_files_and_works_without_sources_or_transformation(
    tmp_path: Path,
    mixed: bool,
) -> None:
    load = _loaded(tmp_path, mixed=mixed)
    for item in load.sources:
        item.manifest_path.unlink()
    before = _snapshot(tmp_path)
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("HTTP")),
        patch.object(LocalBlobStore, "__init__", side_effect=AssertionError("store constructed")),
        patch.object(transform, "curate_ons_pay", side_effect=AssertionError("curation")),
        patch.object(transform, "guard_local_roots", side_effect=AssertionError("guard")),
        patch.object(ons_pay, "_connect", side_effect=AssertionError("analytics")),
        patch.object(Path, "mkdir", side_effect=AssertionError("directory created")),
    ):
        report = _verify(tmp_path, load)
    assert report.status == "healthy" and report.writes is False
    assert report.objects_expected == report.objects_verified == 2
    assert report.bytes_verified == 2 * len(PAYLOAD)
    assert report.load_audit_sha256 == hashlib.sha256(load.receipt_path.read_bytes()).hexdigest()
    assert report.load_run_id == load.run_id and report.load_job_sha256 == "c" * 64
    assert all(
        item.integrity == "verified" and item.freshness_passed is None for item in report.sources
    )
    assert [item.registry_commit for item in report.sources] == [
        item.registry_commit for item in load.sources
    ]
    assert all(item.observed_sha256 == item.source_sha256 for item in report.sources)
    assert all(item.bytes_read == len(PAYLOAD) for item in report.sources)
    assert json.loads(json.dumps(report.to_dict()))["writes"] is False
    assert report.to_dict()["object_root"] == str(tmp_path / "objects")
    assert "object_store" not in report.to_dict()
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "fault", ["missing", "short", "large", "checksum", "directory", "unreadable"]
)
def test_bad_object_is_reported_and_later_sources_are_still_checked(
    tmp_path: Path,
    fault: str,
) -> None:
    load = _load(tmp_path)
    raw = _raw(tmp_path, load)
    if fault == "missing":
        raw.unlink()
    elif fault == "short":
        raw.write_bytes(PAYLOAD[:-1])
    elif fault == "large":
        raw.write_bytes(PAYLOAD + b"x")
    elif fault == "checksum":
        raw.write_bytes(b"x" * len(PAYLOAD))
    elif fault == "directory":
        raw.unlink()
        raw.mkdir()
    before = _snapshot(tmp_path)
    original_open = verification.os.open

    def open_object(path: Any, *args: Any, **kwargs: Any) -> Any:
        if Path(path) == raw and fault == "unreadable":
            raise PermissionError(SECRET)
        return original_open(path, *args, **kwargs)

    with patch.object(verification.os, "open", side_effect=open_object):
        report = _verify(tmp_path, load)
    expected = {
        "short": "size_mismatch",
        "large": "size_mismatch",
        "checksum": "checksum_mismatch",
        "directory": "unsafe",
    }.get(fault, fault)
    assert report.status == "unhealthy" and report.sources[0].integrity == expected
    assert report.objects_verified == 1 and report.sources[1].integrity == "verified"
    assert report.bytes_verified == len(PAYLOAD)
    assert SECRET not in json.dumps(report.to_dict())
    assert _snapshot(tmp_path) == before
    if fault in {"short", "large"}:
        assert report.sources[0].bytes_read == 0 and report.sources[0].observed_sha256 is None


def test_missing_root_is_reported_without_creating_it(tmp_path: Path, capsys: Any) -> None:
    load = _load(tmp_path)
    missing = tmp_path / "missing" / "objects"
    with (
        patch.object(Path, "mkdir", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*_args(tmp_path, load)[:-1], str(missing)])
    report = json.loads(capsys.readouterr().out)
    assert error.value.code == 1 and report["status"] == "unhealthy"
    assert all(item["integrity"] == "missing" for item in report["sources"])
    assert not missing.parent.exists()


@pytest.mark.parametrize("payload", [b"", PAYLOAD * 200])
def test_empty_and_multichunk_raw_bytes_use_exact_limits_without_csv_quality_claims(
    tmp_path: Path, payload: bytes
) -> None:
    load = _loaded(tmp_path, payload=payload)
    report = _verify(
        tmp_path,
        load,
        max_object_bytes=max(len(PAYLOAD), len(payload)),
        max_total_bytes=len(PAYLOAD) + len(payload),
    )
    assert report.status == "healthy" and report.objects_verified == 2
    assert report.bytes_verified == len(PAYLOAD) + len(payload)
    assert report.sources[1].bytes_read == len(payload)
    assert report.sources[1].observed_sha256 == hashlib.sha256(payload).hexdigest()
    assert not list(tmp_path.rglob("*.parquet"))


def test_file_as_object_root_is_invalid_before_object_reads(tmp_path: Path) -> None:
    load = _load(tmp_path)
    with (
        patch.object(verification, "_check_object", side_effect=AssertionError),
        pytest.raises(verification.LoadVerificationError),
    ):
        verification.verify_ons_pay_load(
            load_audit=load.receipt_path, object_root=load.receipt_path
        )


@pytest.mark.parametrize("limit,healthy", [(None, True), (11, True), (10, False), (0, False)])
def test_source_age_is_independent_of_raw_integrity_and_inclusive(
    tmp_path: Path,
    limit: int | None,
    healthy: bool,
) -> None:
    load = _load(tmp_path)
    report = _verify(tmp_path, load, max_source_age_days=limit)
    assert (report.status == "healthy") is healthy
    assert report.objects_verified == 2
    assert all(item.source_age_days == 11 for item in report.sources)
    assert all(
        item.freshness_passed == (healthy if limit is not None else None) for item in report.sources
    )
    same_instant = NOW.astimezone(timezone(timedelta(hours=-5)))
    offset = verification.verify_ons_pay_load(
        load_audit=load.receipt_path,
        object_root=tmp_path / "objects",
        now=same_instant,
        max_source_age_days=limit,
    )
    assert offset == report


@pytest.mark.parametrize(
    "options",
    [
        {"max_object_bytes": 0},
        {"max_total_bytes": -1},
        {"max_object_bytes": True},
        {"max_total_bytes": float("inf")},
        {"max_source_age_days": -1},
        {"max_source_age_days": True},
        {"max_source_age_days": 1.5},
        {"max_object_bytes": len(PAYLOAD) - 1},
        {"max_total_bytes": 2 * len(PAYLOAD) - 1},
        {"expected_load_sha256": "0" * 64},
    ],
)
def test_invalid_policy_pins_and_budgets_fail_before_opening_any_object(
    tmp_path: Path,
    options: dict[str, Any],
) -> None:
    load = _load(tmp_path)
    with (
        patch.object(verification, "_check_object", side_effect=AssertionError("object read")),
        pytest.raises(verification.LoadVerificationError),
    ):
        _verify(tmp_path, load, **options)


@pytest.mark.parametrize("now", [datetime(2026, 11, 20), datetime(2026, 11, 8, tzinfo=UTC)])
def test_invalid_clock_or_future_source_fails_preflight(tmp_path: Path, now: datetime) -> None:
    load = _load(tmp_path)
    with (
        patch.object(verification, "_check_object", side_effect=AssertionError),
        pytest.raises(verification.LoadVerificationError),
    ):
        verification.verify_ons_pay_load(
            load_audit=load.receipt_path, object_root=tmp_path / "objects", now=now
        )


@pytest.mark.parametrize(
    "fault", ["missing_receipt", "identity", "counts", "failed", "duplicate_json"]
)
def test_invalid_whole_batch_evidence_blocks_all_object_reads(tmp_path: Path, fault: str) -> None:
    load = _load(tmp_path)
    audit = json.loads(load.receipt_path.read_bytes())
    if fault == "missing_receipt":
        load.sources[1].ingestion_receipt_path.unlink()
    elif fault == "identity":
        audit["sources"][1]["source_sha256"] = "0" * 64
    elif fault == "counts":
        audit["metrics"]["bytes_verified"] += 1
    elif fault == "failed":
        audit["status"] = "failed"
    if fault == "duplicate_json":
        load.receipt_path.write_text('{"status":"success","status":"failed"}', encoding="utf-8")
    else:
        load.receipt_path.write_text(json.dumps(audit), encoding="utf-8")
    with (
        patch.object(verification, "_check_object", side_effect=AssertionError),
        pytest.raises(verification.LoadVerificationError),
    ):
        _verify(tmp_path, load)


@pytest.mark.parametrize("target", ["file", "parent", "root"])
def test_symlinked_storage_is_rejected_without_following_bytes(tmp_path: Path, target: str) -> None:
    load = _load(tmp_path)
    raw = _raw(tmp_path, load)
    original = (
        raw if target == "file" else raw.parent if target == "parent" else tmp_path / "objects"
    )
    moved = original.with_name(original.name + "-moved")
    original.rename(moved)
    try:
        original.symlink_to(moved, target_is_directory=target != "file")
    except OSError:
        pytest.skip("Creating symlinks requires local privileges")
    with patch.object(verification.os, "open", side_effect=AssertionError("object followed")):
        if target == "root":
            with pytest.raises(verification.LoadVerificationError):
                _verify(tmp_path, load)
        else:
            # Both synthetic vintages use different keys; only inspect the unsafe source here.
            item = transform.plan_ons_pay_transform(load_audit=load.receipt_path).sources[0]
            check = verification._check_object(tmp_path / "objects", item, 11, None)
            assert check.integrity == "unsafe"


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="POSIX special-file check")
def test_fifo_is_rejected_without_opening_it(tmp_path: Path) -> None:
    load = _load(tmp_path)
    raw = _raw(tmp_path, load)
    raw.unlink()
    os.mkfifo(raw)
    report = _verify(tmp_path, load)
    assert report.sources[0].integrity == "unsafe"
    assert report.sources[1].integrity == "verified"


@pytest.mark.parametrize("change", ["grow", "shrink", "read_error"])
def test_bounded_reads_detect_changes_after_stat_and_close_descriptors(
    tmp_path: Path, change: str
) -> None:
    load = _load(tmp_path)
    item = transform.plan_ons_pay_transform(load_audit=load.receipt_path).sources[0]
    real_fdopen = verification.os.fdopen
    streams = []
    read_sizes = []

    def fdopen(fd: int, *args: Any, **kwargs: Any) -> Any:
        stream = real_fdopen(fd, *args, **kwargs)
        called = False

        def read(size: int) -> bytes:
            nonlocal called
            assert 0 < size <= 64 * 1024
            read_sizes.append(size)
            if change == "read_error":
                raise OSError(SECRET)
            if not called:
                called = True
                return (PAYLOAD + b"x")[:size] if change == "grow" else PAYLOAD[:-1]
            return b""

        proxy = Mock(wraps=stream)
        proxy.read.side_effect = read
        proxy.__enter__ = Mock(return_value=proxy)
        proxy.__exit__ = Mock(side_effect=lambda *args: stream.close())
        streams.append(stream)
        return proxy

    with patch.object(verification.os, "fdopen", side_effect=fdopen):
        check = verification._check_object(tmp_path / "objects", item, 11, None)
    assert check.integrity == ("unreadable" if change == "read_error" else "size_mismatch")
    assert check.bytes_read <= len(PAYLOAD) + 1
    assert check.observed_sha256 is None and read_sizes and all(stream.closed for stream in streams)


def test_cli_reports_exact_pins_and_preserves_data(
    tmp_path: Path, capsys: Any, monkeypatch: Any
) -> None:
    load = _load(tmp_path)
    before = _snapshot(tmp_path)
    digest = hashlib.sha256(load.receipt_path.read_bytes()).hexdigest()
    monkeypatch.chdir(tmp_path)
    platform_main(_args(tmp_path, load, "--load-sha256", digest))
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "healthy" and report["objects_verified"] == 2
    assert report["load_audit_sha256"] == digest and _snapshot(tmp_path) == before
    with pytest.raises(SystemExit) as stale:
        platform_main(_args(tmp_path, load, "--max-source-age-days", "0"))
    assert stale.value.code == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "unhealthy" and report["objects_verified"] == 2


@pytest.mark.parametrize(
    "extra", [["--load-sha256", SECRET], ["--max-object-bytes", "0"], ["--bad", SECRET]]
)
def test_cli_input_failures_do_not_print_secrets_or_partial_reports(
    tmp_path: Path,
    capsys: Any,
    extra: list[str],
) -> None:
    load = _load(tmp_path)
    with pytest.raises(SystemExit) as error:
        platform_main(_args(tmp_path, load, *extra))
    output = capsys.readouterr()
    assert error.value.code == 2 and not output.out and SECRET not in output.err


def test_verification_needs_no_analytics_and_works_while_writers_hold_guards(
    tmp_path: Path,
) -> None:
    load = _load(tmp_path)
    before = _snapshot(tmp_path)
    code = """
import importlib.abc, sys
class BlockAnalytics(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', 'pyarrow', 'boto3', 'pymongo', 'psycopg'}:
            raise AssertionError('optional client imported')
sys.meta_path.insert(0, BlockAnalytics())
from data_platform_lab.cli.main import main
main(sys.argv[1:])
"""
    with _holding_roots(tmp_path / "objects", tmp_path / "curated"):
        result = subprocess.run(
            [sys.executable, "-c", code, *_args(tmp_path, load)],
            capture_output=True,
            **_child_options(),
        )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "healthy"
    assert _snapshot(tmp_path) == before
