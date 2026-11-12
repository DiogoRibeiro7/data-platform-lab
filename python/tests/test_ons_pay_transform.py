"""Deferred whole-batch transformation, receipt binding and offline recovery."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_local_run_guard import _child_options, _holding_roots, _probe
from test_ons_pay_refresh import _source as local_source
from test_ons_pay_remote_refresh import _run as load_batch
from test_ons_pay_remote_refresh import _source as remote_source
from test_registry_fetch import PAYLOAD, SECRET, Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.curation.ons_pay import CurationError, curate_ons_pay
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability.ons_pay_monitor import monitor_ons_pay
from data_platform_lab.orchestration import ons_pay_transform as transform
from data_platform_lab.orchestration.local_run_guard import LocalRunGuardError
from data_platform_lab.serving.catalog import CuratedCatalog
from data_platform_lab.storage import BlobStore, LocalBlobStore


def _json(path: Path) -> Any:
    return json.loads(path.read_bytes())


def _write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def _loaded(tmp_path: Path, *, mixed: bool = False, payload: bytes = PAYLOAD) -> Any:
    first, one = remote_source(tmp_path)
    if mixed:
        second = local_source(tmp_path)
        responses = [Response(one["objects"][0])]
    else:
        second, two = remote_source(tmp_path, "b", payload)
        responses = [Response(one["objects"][0]), Response(two["objects"][0], payload)]
    return load_batch(tmp_path, [first, second], responses, load_only=True, job_sha256="c" * 64)


def _transform(tmp_path: Path, load: Any, **kwargs: Any) -> Any:
    return transform.transform_ons_pay(
        load_audit=load.receipt_path,
        store=LocalBlobStore(tmp_path / "objects"),
        output_root=tmp_path / "transformed",
        **kwargs,
    )


def _args(tmp_path: Path, load: Any, *extra: str) -> list[str]:
    return [
        "ons-pay-transform",
        "--load-audit",
        str(load.receipt_path),
        "--object-root",
        str(tmp_path / "objects"),
        "--output-root",
        str(tmp_path / "transformed"),
        *extra,
    ]


@pytest.mark.parametrize("mixed", [False, True])
def test_offline_transform_preserves_lineage_and_reuses_outputs(
    tmp_path: Path, mixed: bool
) -> None:
    load = _loaded(tmp_path, mixed=mixed)
    load_bytes = load.receipt_path.read_bytes()
    for source in load.sources:
        source.manifest_path.unlink()
    raw_before = {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in (tmp_path / "objects").rglob("*.csv")
    }
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("network")),
        patch.object(LocalBlobStore, "put_bytes", side_effect=AssertionError("raw write")),
    ):
        first = _transform(tmp_path, load)
        mtimes = {p: p.stat().st_mtime_ns for p in (tmp_path / "transformed").rglob("*.parquet")}
        replay = _transform(tmp_path, load)
    assert len(first.sources) == len(replay.sources) == 2
    audit = _json(replay.receipt_path)
    assert audit["pipeline_name"] == "ons_pay_transform" and audit["status"] == "success"
    assert audit["input"]["load_audit_sha256"] == hashlib.sha256(load_bytes).hexdigest()
    assert audit["input"]["load_run_id"] == load.run_id
    assert audit["input"]["load_job_sha256"] == "c" * 64 and "job_sha256" not in audit
    assert audit["metrics"]["source_rows"] == 14 and audit["metrics"]["parquet_reused"] == 6
    assert audit["metrics"]["parquet_published"] == 0
    assert {p: p.stat().st_mtime_ns for p in mtimes} == mtimes
    assert {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in raw_before} == raw_before
    assert load.receipt_path.read_bytes() == load_bytes
    for actual, source in zip(audit["sources"], load.sources, strict=True):
        assert actual["registry_commit"] == source.registry_commit
        assert actual["source_sha256"] == source.source_sha256
        curated = _json(Path(actual["curation_receipt_path"]))
        assert curated["input"]["ingestion_receipt_sha256"] == actual["ingestion_receipt_sha256"]
    catalog = CuratedCatalog(tmp_path / "transformed")
    assert len(catalog.list_vintages()) == 2
    for vintage in catalog.list_vintages():
        health = monitor_ons_pay(data_root=tmp_path / "transformed", key=vintage.key)
        assert health.latest_batch_run_id is None
        assert health.metrics["curation_integrity"] == health.metrics["table_integrity"] == 1
        assert health.status == "unhealthy"
    assert not list((tmp_path / "transformed").rglob("ons_pay_refresh_*.json"))
    assert not (tmp_path / "transformed" / "releases").exists()


def test_preview_checks_receipts_without_store_analytics_or_destination_writes(
    tmp_path: Path, capsys: Any, monkeypatch: Any
) -> None:
    load = _loaded(tmp_path)
    before = {p: p.stat().st_mtime_ns for p in tmp_path.rglob("*")}
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    # A parent-relative audit path is accepted, while links within it remain absolute.
    args = _args(tmp_path, load, "--dry-run")
    args[2] = str(Path("..") / "curated" / "manifests" / load.receipt_path.name)
    with (
        patch.object(transform, "curate_ons_pay", side_effect=AssertionError),
        patch.object(LocalBlobStore, "__init__", side_effect=AssertionError),
        patch.object(transform, "guard_local_roots", side_effect=AssertionError),
    ):
        platform_main(args)
    plan = json.loads(capsys.readouterr().out)
    assert plan["status"] == "planned" and plan["writes"] is False
    assert len(plan["sources"]) == 2 and plan["load_run_id"] == load.run_id
    assert not (tmp_path / "transformed").exists()
    assert set(tmp_path.rglob("*")) == set(before) | {elsewhere}
    assert {p: p.stat().st_mtime_ns for p in before} == before


def test_cli_executes_preview_digest_and_reports_replay(tmp_path: Path, capsys: Any) -> None:
    load = _loaded(tmp_path)
    platform_main(_args(tmp_path, load, "--dry-run"))
    digest = json.loads(capsys.readouterr().out)["load_audit_sha256"]
    platform_main(_args(tmp_path, load, "--load-sha256", digest))
    first = json.loads(capsys.readouterr().out)
    assert first["parquet_published"] == 6 and first["vintages_completed"] == 2
    platform_main(_args(tmp_path, load))
    assert json.loads(capsys.readouterr().out)["parquet_reused"] == 6


def test_late_transformation_failure_retains_progress_and_replays_safely(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    original = transform.curate_ons_pay
    calls = 0

    def fail_second(**kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise CurationError(SECRET)
        return original(**kwargs)

    with (
        patch.object(transform, "curate_ons_pay", side_effect=fail_second),
        pytest.raises(transform.TransformError) as caught,
    ):
        _transform(tmp_path, load)
    audit = _json(caught.value.receipt_path)
    assert audit["status"] == "failed" and audit["metrics"]["vintages_completed"] == 1
    assert audit["failure"]["phase"] == "curation" and audit["failure"]["source_index"] == 1
    assert SECRET not in caught.value.receipt_path.read_text()
    preserved = caught.value.receipt_path.read_bytes()
    times = {p: p.stat().st_mtime_ns for p in (tmp_path / "transformed").rglob("*.parquet")}
    replay = _transform(tmp_path, load)
    assert _json(replay.receipt_path)["metrics"]["parquet_reused"] == 3
    assert _json(replay.receipt_path)["metrics"]["parquet_published"] == 3
    assert {p: p.stat().st_mtime_ns for p in times} == times
    assert caught.value.receipt_path.read_bytes() == preserved


def test_whole_batch_receipt_preflight_precedes_first_curation(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    load.sources[1].ingestion_receipt_path.unlink()
    with (
        patch.object(transform, "curate_ons_pay", side_effect=AssertionError),
        pytest.raises(transform.TransformError) as caught,
    ):
        _transform(tmp_path, load)
    audit = _json(caught.value.receipt_path)
    assert audit["failure"]["phase"] == "preflight" and audit["sources"] == []
    assert not list((tmp_path / "transformed").rglob("*.parquet"))


def test_receipt_replacement_between_preflight_and_curation_is_rejected(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    original = transform.curate_ons_pay

    def replace_then_curate(**kwargs: Any) -> Any:
        if kwargs["input_receipt"] == load.sources[1].ingestion_receipt_path:
            receipt = _json(kwargs["input_receipt"])
            receipt["citation"] = SECRET
            _write(kwargs["input_receipt"], receipt)
        return original(**kwargs)

    with (
        patch.object(transform, "curate_ons_pay", side_effect=replace_then_curate),
        pytest.raises(transform.TransformError) as caught,
    ):
        _transform(tmp_path, load)
    audit = _json(caught.value.receipt_path)
    assert audit["failure"]["source_index"] == 1 and len(audit["sources"]) == 1
    assert SECRET not in json.dumps(audit)


@pytest.mark.parametrize("fault", ["missing", "corrupt", "quality"])
def test_stored_data_is_verified_only_on_execution(tmp_path: Path, fault: str) -> None:
    load = _loaded(
        tmp_path, payload=b"unsupported,columns\nraw,data\n" if fault == "quality" else PAYLOAD
    )
    plan = transform.plan_ons_pay_transform(load_audit=load.receipt_path)
    if fault != "quality":
        raw = tmp_path / "objects" / plan.sources[1].source.storage_key
        if fault == "missing":
            raw.unlink()
        else:
            raw.write_bytes(b"corrupt")
    assert len(transform.plan_ons_pay_transform(load_audit=load.receipt_path).sources) == 2
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(transform.TransformError) as caught,
    ):
        _transform(tmp_path, load)
    assert _json(caught.value.receipt_path)["failure"]["phase"] == "curation"


def test_changed_load_audit_does_not_match_preview_digest(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    digest = transform.plan_ons_pay_transform(load_audit=load.receipt_path).load_audit_sha256
    load.receipt_path.write_bytes(load.receipt_path.read_bytes() + b"\n")
    with pytest.raises(transform.TransformError) as caught:
        _transform(tmp_path, load, expected_load_sha256=digest)
    assert _json(caught.value.receipt_path)["failure"]["phase"] == "preflight"
    assert not list((tmp_path / "transformed").rglob("*.parquet"))


@pytest.mark.parametrize(
    "fault",
    [
        "pipeline",
        "schema",
        "failed",
        "model",
        "empty",
        "too_many",
        "duplicate",
        "identity",
        "relative_path",
        "source_count",
        "aggregate",
        "job_digest",
        "timestamp",
        "duration",
        "receipt_count",
        "receipt_status",
        "receipt_pin",
    ],
)
def test_inconsistent_load_evidence_is_rejected(tmp_path: Path, capsys: Any, fault: str) -> None:
    load = _loaded(tmp_path)
    audit = _json(load.receipt_path)
    if fault == "pipeline":
        audit["pipeline_name"] = "ons_pay_refresh"
    elif fault == "schema":
        audit["schema_version"] = True
    elif fault == "failed":
        audit["status"] = "failed"
    elif fault == "model":
        audit["model_version"] = 1
    elif fault == "empty":
        audit["sources"] = []
    elif fault == "too_many":
        audit["sources"] *= 129
    elif fault == "duplicate":
        audit["sources"][1] = audit["sources"][0]
    elif fault == "identity":
        audit["sources"][1]["registry_commit"] = "e" * 40
    elif fault == "relative_path":
        audit["sources"][0]["ingestion_receipt_path"] = "relative.json"
    elif fault == "source_count":
        audit["sources"][0]["bytes_verified"] = True
    elif fault == "aggregate":
        audit["metrics"]["bytes_verified"] += 1
    elif fault == "job_digest":
        audit["job_sha256"] = SECRET
    elif fault == "timestamp":
        audit["created_at"] = "9998-01-01T00:00:00+00:00"
    elif fault == "duration":
        audit["duration_seconds"] = -1
    else:
        path = load.sources[1].ingestion_receipt_path
        receipt = _json(path)
        if fault == "receipt_count":
            receipt["files_reused"] += 1
        elif fault == "receipt_status":
            receipt["status"] = "failed"
        else:
            receipt["objects"][0]["source_uri"] = SECRET
        _write(path, receipt)
    _write(load.receipt_path, audit)
    with pytest.raises(SystemExit) as caught:
        platform_main(_args(tmp_path, load, "--dry-run"))
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert not captured.out and SECRET not in captured.err
    assert not (tmp_path / "transformed").exists()


@pytest.mark.parametrize("raw", [b"{", b"[]", b'{"x":1,"x":2}', b'{"x":NaN}'])
def test_strict_json_rejects_ambiguous_evidence(tmp_path: Path, raw: bytes) -> None:
    load = _loaded(tmp_path)
    load.receipt_path.write_bytes(raw)
    with pytest.raises(transform.TransformError):
        transform.plan_ons_pay_transform(load_audit=load.receipt_path)


@pytest.mark.parametrize("limit", ["_MAX_AUDIT_BYTES", "_MAX_RECEIPT_BYTES"])
def test_evidence_byte_limits(tmp_path: Path, monkeypatch: Any, limit: str) -> None:
    load = _loaded(tmp_path)
    monkeypatch.setattr(transform, limit, 1)
    with pytest.raises(transform.TransformError, match="byte limit"):
        transform.plan_ons_pay_transform(load_audit=load.receipt_path)


@pytest.mark.parametrize("which", ["load", "ingestion"])
def test_symlinked_evidence_is_rejected(tmp_path: Path, which: str) -> None:
    load = _loaded(tmp_path)
    path = load.receipt_path if which == "load" else load.sources[0].ingestion_receipt_path
    moved = path.with_name("moved.json")
    path.rename(moved)
    try:
        path.symlink_to(moved)
    except OSError:
        pytest.skip("Creating symlinks requires local privileges")
    with pytest.raises(transform.TransformError):
        transform.plan_ons_pay_transform(load_audit=load.receipt_path)


@pytest.mark.parametrize("root", ["objects", "transformed"])
def test_busy_rejection_precedes_preflight_and_preview_does_not_lock(
    tmp_path: Path, capsys: Any, root: str
) -> None:
    load = _loaded(tmp_path)
    with _holding_roots(tmp_path / root):
        platform_main(_args(tmp_path, load, "--dry-run"))
        assert json.loads(capsys.readouterr().out)["writes"] is False
        with (
            patch.object(transform, "plan_ons_pay_transform", side_effect=AssertionError),
            pytest.raises(SystemExit) as caught,
        ):
            platform_main(_args(tmp_path, load))
        assert caught.value.code == 3
    assert not (tmp_path / "transformed").exists()
    assert _probe(tmp_path / "objects", tmp_path / "transformed").returncode == 0


def test_audit_is_written_while_guards_are_held(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    original = transform._write_audit

    def inspect(path: Path, document: Any) -> None:
        assert _probe(tmp_path / "objects").returncode == 3
        assert _probe(tmp_path / "transformed").returncode == 3
        original(path, document)

    with patch.object(transform, "_write_audit", side_effect=inspect):
        _transform(tmp_path, load)


def test_audit_write_failure_does_not_report_success(tmp_path: Path, capsys: Any) -> None:
    load = _loaded(tmp_path)
    with (
        patch.object(transform, "_write_audit", side_effect=OSError(SECRET)),
        pytest.raises(SystemExit) as caught,
    ):
        platform_main(_args(tmp_path, load))
    assert caught.value.code == 1
    captured = capsys.readouterr()
    assert not captured.out and SECRET not in captured.err
    assert len(list((tmp_path / "transformed").rglob("*.parquet"))) == 6
    assert _probe(tmp_path / "objects", tmp_path / "transformed").returncode == 0


def test_preview_does_not_import_optional_analytics(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    code = """
import importlib.abc, sys
class BlockAnalytics(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', 'pyarrow'}:
            raise AssertionError('analytics imported')
sys.meta_path.insert(0, BlockAnalytics())
from data_platform_lab.cli.main import main
main(sys.argv[1:])
"""
    result = subprocess.run(
        [sys.executable, "-c", code, *_args(tmp_path, load, "--dry-run")],
        capture_output=True,
        **_child_options(),
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "planned"


def test_direct_curation_receipt_pin_is_checked_before_store_io(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    store = LocalBlobStore(tmp_path / "objects")
    with (
        patch.object(store, "get_bytes", side_effect=AssertionError),
        pytest.raises(CurationError, match="planned SHA-256"),
    ):
        curate_ons_pay(
            input_receipt=load.sources[0].ingestion_receipt_path,
            store=store,
            output_root=tmp_path / "transformed",
            expected_receipt_sha256="0" * 64,
        )
    assert not (tmp_path / "transformed").exists()


def test_invalid_cli_digest_is_rejected_before_execution(tmp_path: Path, capsys: Any) -> None:
    load = _loaded(tmp_path)
    with pytest.raises(SystemExit) as caught:
        platform_main(_args(tmp_path, load, "--load-sha256", SECRET))
    assert caught.value.code == 2 and SECRET not in capsys.readouterr().err
    assert not (tmp_path / "transformed").exists()


def test_same_output_root_preserves_load_evidence_and_detects_parquet_drift(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    before = load.receipt_path.read_bytes()
    store = LocalBlobStore(tmp_path / "objects")
    result = transform.transform_ons_pay(
        load_audit=load.receipt_path, store=store, output_root=tmp_path / "curated"
    )
    result.sources[0].result.outputs[0].path.write_bytes(b"corrupt")
    with pytest.raises(transform.TransformError) as caught:
        transform.transform_ons_pay(
            load_audit=load.receipt_path, store=store, output_root=tmp_path / "curated"
        )
    assert _json(caught.value.receipt_path)["failure"]["phase"] == "curation"
    assert load.receipt_path.read_bytes() == before
    assert result.sources[0].result.outputs[0].path.read_bytes() == b"corrupt"


def test_transform_uses_blobstore_reads_without_object_mutation(tmp_path: Path) -> None:
    load = _loaded(tmp_path)
    store = Mock(spec=BlobStore)
    store.get_bytes.side_effect = LocalBlobStore(tmp_path / "objects").get_bytes
    result = transform.transform_ons_pay(
        load_audit=load.receipt_path, store=store, output_root=tmp_path / "transformed"
    )
    assert len(result.sources) == store.get_bytes.call_count == 2
    store.put_bytes.assert_not_called()


def test_guard_setup_failure_leaves_no_batch_audit(tmp_path: Path, capsys: Any) -> None:
    load = _loaded(tmp_path)
    with (
        patch.object(transform, "guard_local_roots", side_effect=LocalRunGuardError(SECRET)),
        pytest.raises(SystemExit) as caught,
    ):
        platform_main(_args(tmp_path, load))
    assert caught.value.code == 1 and SECRET not in capsys.readouterr().err
    assert not (tmp_path / "transformed").exists()


def test_missing_analytics_is_audited_without_leaking_driver_errors(
    tmp_path: Path, capsys: Any
) -> None:
    load = _loaded(tmp_path)
    with (
        patch(
            "data_platform_lab.curation.ons_pay.import_module",
            side_effect=ModuleNotFoundError(SECRET),
        ),
        pytest.raises(SystemExit) as caught,
    ):
        platform_main(_args(tmp_path, load))
    assert caught.value.code == 1 and SECRET not in capsys.readouterr().err
    path = next((tmp_path / "transformed" / "manifests").glob("ons_pay_transform_*.json"))
    assert _json(path)["failure"]["phase"] == "curation"
    assert _json(path)["failure"]["error_type"] == "DependencyError"
