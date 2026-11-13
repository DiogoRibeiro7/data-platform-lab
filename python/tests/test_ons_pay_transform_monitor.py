"""Health and lineage for offline ONS transformations, separate from refresh age."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from test_ons_pay_monitor import _check, _now
from test_ons_pay_transform import _json, _loaded, _transform, _write
from test_registry_fetch import SECRET

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability import ons_pay_transform_audits as audits
from data_platform_lab.observability.ons_pay_monitor import (
    MonitorError,
    format_prometheus,
    monitor_ons_pay,
)
from data_platform_lab.orchestration import ons_pay_transform as transform
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.storage import LocalBlobStore


@pytest.fixture
def ready(tmp_path: Path) -> tuple[Any, Any, Path, VintageKey]:
    loaded = _loaded(tmp_path)
    result = _transform(tmp_path, loaded)
    source = result.sources[0].planned.source
    return (
        loaded,
        result,
        tmp_path / "transformed",
        VintageKey(1, source.registry_commit, source.source_sha256),
    )


def _monitor(ready: Any, **kwargs: Any) -> Any:
    _, _, root, key = ready
    return monitor_ons_pay(data_root=root, key=key, workflow="transform", **kwargs)


def _args(ready: Any, *extra: str) -> list[str]:
    _, _, root, key = ready
    return [
        "ons-pay-monitor",
        "--data-root",
        str(root),
        "--registry-commit",
        key.registry_commit,
        "--source-sha256",
        key.source_sha256,
        "--workflow",
        "transform",
        *extra,
    ]


def test_transformed_vintage_can_be_healthy_without_refresh_evidence(ready: Any) -> None:
    _, result, root, key = ready
    report = _monitor(
        ready,
        max_transform_age_hours=2,
        max_source_age_days=365,
        max_missing_pay_percent=20,
        now=_now(),
    )
    assert report.status == "healthy" and all(check.state == "pass" for check in report.checks)
    assert report.latest_batch_run_id == report.latest_vintage_success_run_id == result.run_id
    assert report.to_dict()["workflow"] == "transform"
    assert report.metrics["transform_audit_integrity"] == report.metrics["table_integrity"] == 1
    assert "refresh_audit_integrity" not in report.metrics
    assert "last_vintage_refresh_age_hours" not in report.metrics
    original = monitor_ons_pay(data_root=root, key=key, now=_now())
    assert original.status == "unhealthy" and original.latest_batch_run_id is None
    assert "workflow" not in original.to_dict() and "workflow" not in asdict(original)
    assert not list(root.glob("manifests/ons_pay_refresh_*.json"))


def test_source_age_and_transform_age_are_independent(ready: Any) -> None:
    report = _monitor(ready, max_source_age_days=0, max_transform_age_hours=24, now=_now())
    assert report.status == "unhealthy" and _check(report, "source_freshness").state == "fail"
    assert _check(report, "transform_recency").state == "pass"
    report = _monitor(
        ready, max_source_age_days=365, max_transform_age_hours=1, now=_now() + timedelta(hours=2)
    )
    assert _check(report, "source_freshness").state == "pass"
    assert _check(report, "transform_recency").state == "fail"


def test_age_equality_timezone_and_missing_observations(ready: Any) -> None:
    _, result, _, _ = ready
    completed = datetime.fromisoformat(_json(result.receipt_path)["created_at"])
    report = _monitor(ready, now=completed + timedelta(hours=1), max_transform_age_hours=1)
    assert report.metrics["last_vintage_transform_age_hours"] == 1 and report.status == "healthy"
    result.receipt_path.unlink()
    report = _monitor(ready, max_transform_age_hours=1, now=_now())
    assert report.status == "unhealthy" and report.latest_batch_run_id is None
    assert report.metrics["last_vintage_transform_age_hours"] is None
    assert "dpl_ons_pay_last_vintage_transform_age_hours" not in format_prometheus(report)
    assert _check(report, "vintage_transform_success").state == "fail"


@pytest.mark.parametrize("phase", ["preflight", "curation"])
def test_later_failure_preserves_last_success_and_does_not_reset_age(
    ready: Any, tmp_path: Path, phase: str
) -> None:
    loaded, result, _, _ = ready
    if phase == "preflight":
        # No load metadata was available during this attempt, but its failure audit is valid.
        missing = tmp_path / "missing.json"
        with pytest.raises(transform.TransformError) as caught:
            transform.transform_ons_pay(
                load_audit=missing,
                store=LocalBlobStore(tmp_path / "objects"),
                output_root=tmp_path / "transformed",
            )
    else:
        original = transform.curate_ons_pay

        def fail_second(**kwargs: Any) -> Any:
            if kwargs["input_receipt"] == loaded.sources[1].ingestion_receipt_path:
                raise ValueError(SECRET)
            return original(**kwargs)

        with (
            patch.object(transform, "curate_ons_pay", side_effect=fail_second),
            pytest.raises(transform.TransformError) as caught,
        ):
            _transform(tmp_path, loaded)
    report = _monitor(ready, now=_now())
    assert (
        report.status == "unhealthy"
        and report.latest_batch_run_id == _json(caught.value.receipt_path)["run_id"]
    )
    assert report.latest_vintage_success_run_id == result.run_id
    assert report.metrics["transform_audit_integrity"] == 1
    assert _check(report, "last_batch").state == "fail" and SECRET not in json.dumps(
        report.to_dict()
    )
    recovery = _transform(tmp_path, loaded)
    assert _monitor(ready, now=_now()).latest_vintage_success_run_id == recovery.run_id


def test_partial_batch_never_establishes_whole_batch_success(tmp_path: Path) -> None:
    loaded = _loaded(tmp_path)
    original = transform.curate_ons_pay

    def fail_second(**kwargs: Any) -> Any:
        if kwargs["input_receipt"] == loaded.sources[1].ingestion_receipt_path:
            raise ValueError(SECRET)
        return original(**kwargs)

    with (
        patch.object(transform, "curate_ons_pay", side_effect=fail_second),
        pytest.raises(transform.TransformError),
    ):
        _transform(tmp_path, loaded)
    loaded.sources[1].ingestion_receipt_path.unlink()
    key = VintageKey(1, loaded.sources[0].registry_commit, loaded.sources[0].source_sha256)
    report = monitor_ons_pay(
        data_root=tmp_path / "transformed", key=key, workflow="transform", now=_now()
    )
    assert report.metrics["table_integrity"] == report.metrics["transform_audit_integrity"] == 1
    assert report.latest_batch_run_id is not None and report.latest_vintage_success_run_id is None
    assert _check(report, "vintage_transform_success").state == "fail"


def test_raw_load_and_refresh_audits_cannot_change_transform_health(ready: Any) -> None:
    _, result, root, _ = ready
    for name in (
        "ons_pay_load_broken.json",
        "ons_pay_refresh_broken.json",
        ".ons_pay_refresh_unfinished",
    ):
        (root / "manifests" / name).write_text("broken", encoding="utf-8")
    report = _monitor(ready, now=_now())
    assert report.status == "healthy" and report.latest_batch_run_id == result.run_id


@pytest.mark.parametrize("part", ["load", "ingestion", "curation", "parquet"])
def test_linked_evidence_and_selected_tables_are_reverified(ready: Any, part: str) -> None:
    loaded, result, _, _ = ready
    paths = {
        "load": loaded.receipt_path,
        "ingestion": loaded.sources[0].ingestion_receipt_path,
        "curation": result.sources[0].result.receipt_path,
        "parquet": result.sources[0].result.outputs[0].path,
    }
    path = paths[part]
    path.write_bytes(path.read_bytes() + b"\n" if part != "curation" else b"{}")
    report = _monitor(ready, now=_now())
    assert report.status == "unhealthy"
    if part == "parquet":
        assert report.metrics["curation_integrity"] == 0
    else:
        assert report.metrics["transform_audit_integrity"] == 0


@pytest.mark.parametrize(
    "fault",
    [
        "schema",
        "model",
        "filename",
        "future",
        "duration",
        "status",
        "planned",
        "partial_success",
        "counts",
        "row_counts",
        "load_hash",
        "load_run",
        "ingestion_hash",
        "curation_path",
        "source_pin",
        "duplicate",
        "failure",
    ],
)
def test_inconsistent_audits_fail_closed(ready: Any, fault: str) -> None:
    _, result, _, _ = ready
    path = result.receipt_path
    document = _json(path)
    if fault == "schema":
        document["schema_version"] = True
    elif fault == "model":
        document["model_version"] = 99
    elif fault == "filename":
        document["run_id"] = "0" * 32
    elif fault == "future":
        document["created_at"] = "9998-01-01T00:00:00+00:00"
    elif fault == "duration":
        document["duration_seconds"] += 2
    elif fault == "status":
        document["status"] = "running"
    elif fault == "planned":
        document["sources_planned"] = True
    elif fault == "partial_success":
        document["sources"].pop()
    elif fault == "counts":
        document["metrics"]["vintages_completed"] += 1
    elif fault == "row_counts":
        document["sources"][0]["source_rows"] += 1
    elif fault == "load_hash":
        document["input"]["load_audit_sha256"] = "0" * 64
    elif fault == "load_run":
        document["input"]["load_run_id"] = "0" * 32
    elif fault == "ingestion_hash":
        document["sources"][0]["ingestion_receipt_sha256"] = "0" * 64
    elif fault == "curation_path":
        document["sources"][0]["curation_receipt_path"] = "relative.json"
    elif fault == "source_pin":
        document["sources"][0]["registry_commit"] = "e" * 40
    elif fault == "duplicate":
        document["sources"][1] = document["sources"][0]
    else:
        document["failure"] = {"message": SECRET}
    _write(path, document)
    report = _monitor(ready, now=_now())
    assert report.status == "unhealthy" and report.metrics["transform_audit_integrity"] == 0
    assert report.latest_batch_run_id is None and SECRET not in json.dumps(report.to_dict())


@pytest.mark.parametrize("raw", [b"{", b"[]", b'{"x":1,"x":2}', b'{"x":NaN}'])
def test_invalid_json_is_not_silently_ignored(ready: Any, raw: bytes) -> None:
    _, _, root, _ = ready
    (root / "manifests" / "ons_pay_transform_broken.json").write_bytes(raw)
    assert _monitor(ready, now=_now()).metrics["transform_audit_integrity"] == 0


@pytest.mark.parametrize("limit", ["_MAX_FILES", "_MAX_FILE_BYTES", "_MAX_TOTAL_BYTES"])
def test_history_and_linked_evidence_are_bounded(ready: Any, monkeypatch: Any, limit: str) -> None:
    monkeypatch.setattr(audits, limit, 1)
    report = _monitor(ready, now=_now())
    assert report.status == "unhealthy" and report.latest_batch_run_id is None


@pytest.mark.parametrize("part", ["directory", "audit", "ingestion"])
def test_symlinked_history_or_links_are_rejected(ready: Any, part: str) -> None:
    loaded, result, root, _ = ready
    path = {
        "directory": root / "manifests",
        "audit": result.receipt_path,
        "ingestion": loaded.sources[0].ingestion_receipt_path,
    }[part]
    moved = path.with_name("moved")
    path.rename(moved)
    try:
        path.symlink_to(moved, target_is_directory=part == "directory")
    except OSError:
        pytest.skip("Creating symlinks requires local privileges")
    assert _monitor(ready, now=_now()).metrics["transform_audit_integrity"] == 0


def test_monitor_is_read_only_and_does_not_reexecute_sources(ready: Any, tmp_path: Path) -> None:
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob("*") if p.is_file()}
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(LocalBlobStore, "__init__", side_effect=AssertionError),
        patch.object(transform, "curate_ons_pay", side_effect=AssertionError),
    ):
        assert _monitor(ready, now=_now()).status == "healthy"
    assert {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob("*") if p.is_file()
    } == before


def test_cli_json_prometheus_and_textfile_use_distinct_workflow(
    ready: Any, tmp_path: Path, capsys: Any
) -> None:
    platform_main(_args(ready, "--max-transform-age-hours", "24"))
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "healthy" and report["workflow"] == "transform"
    textfile = tmp_path / "metrics" / "transform.prom"
    textfile.parent.mkdir()
    platform_main(_args(ready, "--format", "prometheus", "--textfile", str(textfile)))
    metrics = capsys.readouterr().out
    assert textfile.read_text() == metrics and 'workflow="transform"' in metrics
    assert "dpl_ons_pay_last_vintage_transform_age_hours" in metrics
    assert "dpl_ons_pay_transform_audit_integrity" in metrics
    assert "dpl_ons_pay_last_transform_batch_success" in metrics
    assert "dpl_ons_pay_last_batch_success" not in metrics
    assert (
        "dpl_ons_pay_refresh_audit_integrity" not in metrics
        and "last_vintage_refresh" not in metrics
    )
    with pytest.raises(SystemExit) as caught:
        platform_main(_args(ready, "--max-transform-age-hours", "0"))
    assert caught.value.code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "unhealthy"


@pytest.mark.parametrize(
    "options",
    [
        {"workflow": "bad"},
        {"workflow": "refresh", "max_transform_age_hours": 1},
        {"workflow": "transform", "max_refresh_age_hours": 1},
        {"workflow": "transform", "max_transform_age_hours": float("nan")},
        {"workflow": "transform", "max_transform_age_hours": -1},
        {"workflow": "transform", "max_transform_age_hours": True},
    ],
)
def test_invalid_workflow_thresholds_are_rejected(ready: Any, options: Any) -> None:
    _, _, root, key = ready
    with pytest.raises(MonitorError):
        monitor_ons_pay(data_root=root, key=key, **options)


def test_cli_rejects_refresh_threshold_in_transform_mode(ready: Any, capsys: Any) -> None:
    with pytest.raises(SystemExit) as caught:
        platform_main(_args(ready, "--max-refresh-age-hours", "1"))
    assert caught.value.code == 2 and not capsys.readouterr().out


def test_unselected_vintage_is_never_inferred_from_latest_batch(ready: Any) -> None:
    _, _, root, key = ready
    report = monitor_ons_pay(
        data_root=root,
        key=VintageKey(1, "f" * 40, key.source_sha256),
        workflow="transform",
        now=_now(),
    )
    assert report.status == "unhealthy" and report.latest_batch_run_id is not None
    assert report.latest_vintage_success_run_id is None
    assert _check(report, "vintage_transform_success").state == "fail"


def test_retained_corruption_cannot_be_hidden_by_a_later_success(
    ready: Any, tmp_path: Path
) -> None:
    loaded, result, _, _ = ready
    _transform(tmp_path, loaded)
    document = _json(result.receipt_path)
    document["metrics"]["vintages_completed"] = -1
    _write(result.receipt_path, document)
    report = _monitor(ready, now=_now())
    assert report.status == "unhealthy" and report.latest_batch_run_id is None


def test_directory_access_errors_fail_the_integrity_check(ready: Any) -> None:
    with patch.object(audits.os, "scandir", side_effect=PermissionError(SECRET)):
        report = _monitor(ready, now=_now())
    assert report.status == "unhealthy" and report.metrics["transform_audit_integrity"] == 0
    assert SECRET not in json.dumps(report.to_dict())


@pytest.mark.parametrize("fault", ["phase", "source_index", "planned"])
def test_failed_audit_progress_must_be_consistent(ready: Any, tmp_path: Path, fault: str) -> None:
    loaded, _, _, _ = ready
    with (
        patch.object(transform, "curate_ons_pay", side_effect=ValueError(SECRET)),
        pytest.raises(transform.TransformError) as caught,
    ):
        _transform(tmp_path, loaded)
    document = _json(caught.value.receipt_path)
    if fault == "phase":
        document["failure"]["phase"] = "ingestion"
    elif fault == "source_index":
        document["failure"]["source_index"] = 1
    else:
        document["sources_planned"] = None
    _write(caught.value.receipt_path, document)
    assert _monitor(ready, now=_now()).metrics["transform_audit_integrity"] == 0
