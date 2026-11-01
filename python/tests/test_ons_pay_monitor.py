"""Offline operational monitoring of pinned ONS pay refresh/curation receipts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from test_ons_pay_refresh import AS_OF, _source

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_monitor import (
    MonitorCheck,
    MonitorError,
    MonitorReport,
    format_prometheus,
    monitor_ons_pay,
)
from data_platform_lab.orchestration.ons_pay_refresh import RefreshResult, refresh_ons_pay
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.storage import LocalBlobStore


@pytest.fixture
def ready(tmp_path: Path) -> tuple[Path, VintageKey, RefreshResult]:
    source = _source(tmp_path)
    root = tmp_path / "curated"
    result = refresh_ons_pay(
        sources=[source],
        store=LocalBlobStore(tmp_path / "objects"),
        output_root=root,
        as_of=AS_OF,
    )
    item = result.sources[0]
    return root, VintageKey(1, item.registry_commit, item.source_sha256), result


def _check(report: MonitorReport, name: str) -> MonitorCheck:
    return next(check for check in report.checks if check.name == name)


def _now() -> datetime:
    """Reference time after a just-created audit, including filesystem jitter."""
    return datetime.now(UTC) + timedelta(minutes=1)


def test_healthy_pinned_vintage_and_all_measurable_metrics(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, result = ready
    report = monitor_ons_pay(
        data_root=root,
        key=key,
        max_source_age_days=30,
        max_refresh_age_hours=2,
        max_missing_pay_percent=20,
        now=_now(),
    )
    assert report.status == "healthy"
    assert all(check.state == "pass" for check in report.checks)
    assert report.latest_batch_run_id == result.run_id
    assert report.latest_vintage_success_run_id == result.run_id
    assert report.metrics["curation_integrity"] == 1
    assert report.metrics["table_integrity"] == 1
    assert report.metrics["refresh_audit_integrity"] == 1
    assert report.metrics["last_batch_success"] == 1
    assert report.metrics["source_rows"] == 7
    assert report.metrics["rows_without_pay"] == 1
    assert report.metrics["missing_pay_percent"] == pytest.approx(100 / 7)
    assert report.metrics["geography_year_rows"] == 5
    assert report.metrics["local_authority_yoy_rows"] == 6
    assert report.metrics["last_vintage_refresh_age_hours"] >= 0
    assert report.to_dict()["vintage"]["registry_commit"] == key.registry_commit


def test_source_age_sla_breach_independent_of_recent_refresh(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, _ = ready
    report = monitor_ons_pay(
        data_root=root,
        key=key,
        max_source_age_days=1,
        max_refresh_age_hours=3,
        now=_now(),
    )
    assert report.status == "unhealthy"
    assert _check(report, "source_freshness").state == "fail"
    assert _check(report, "refresh_recency").state == "pass"
    assert _check(report, "table_contract").state == "pass"


def test_missing_pay_rate_policy_can_alert_without_discarding_suppression(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, _ = ready
    report = monitor_ons_pay(data_root=root, key=key, max_missing_pay_percent=10, now=_now())
    assert report.status == "unhealthy"
    assert _check(report, "missing_pay_rate").state == "fail"
    assert report.metrics["rows_without_pay"] == 1
    assert _check(report, "source_freshness").state == "skipped"


def test_last_successful_refresh_age_is_a_distinct_sla(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, _ = ready
    report = monitor_ons_pay(
        data_root=root,
        key=key,
        max_source_age_days=30,
        max_refresh_age_hours=3,
        now=_now() + timedelta(hours=6),
    )
    assert _check(report, "source_freshness").state == "pass"
    assert _check(report, "refresh_recency").state == "fail"
    assert report.status == "unhealthy"


def test_integrity_detects_tampered_parquet_and_keeps_audit_signal(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, result = ready
    curation = json.loads(result.sources[0].curation_receipt_path.read_text())
    Path(curation["outputs"][0]["path"]).write_bytes(b"altered")
    report = monitor_ons_pay(data_root=root, key=key, now=_now())
    assert report.status == "unhealthy"
    assert _check(report, "curation_integrity").state == "fail"
    assert _check(report, "table_contract").state == "skipped"
    assert _check(report, "refresh_audit_integrity").state == "pass"


@pytest.mark.parametrize("fault", ["schema", "row_count", "lineage"])
def test_table_contract_detects_drift_even_when_receipt_hash_is_updated(
    ready: tuple[Path, VintageKey, RefreshResult], fault: str
) -> None:
    import duckdb

    root, key, result = ready
    manifest = result.sources[0].curation_receipt_path
    receipt = json.loads(manifest.read_text())
    output = receipt["outputs"][1]
    path = Path(output["path"])
    if fault in {"schema", "lineage"}:
        rewritten = path.with_name("rewritten.parquet")
        conn = duckdb.connect(":memory:")
        try:
            if fault == "schema":
                conn.execute(
                    "COPY (SELECT * EXCLUDE(median_area_weekly_pay_gbp) "
                    "FROM read_parquet(?)) TO ? (FORMAT PARQUET)",
                    [str(path), str(rewritten)],
                )
            else:
                conn.execute(
                    "COPY (SELECT * REPLACE (? AS registry_commit) "
                    "FROM read_parquet(?)) TO ? (FORMAT PARQUET)",
                    ["f" * 40, str(path), str(rewritten)],
                )
        finally:
            conn.close()
        rewritten.replace(path)
        output["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    else:
        output["rows"] += 1
    manifest.write_text(json.dumps(receipt), encoding="utf-8")
    report = monitor_ons_pay(data_root=root, key=key, now=_now())
    assert report.status == "unhealthy"
    assert _check(report, "curation_integrity").state == "pass"
    assert _check(report, "table_contract").state == "fail"


def test_missing_audit_never_silently_counts_as_healthy(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, result = ready
    result.receipt_path.unlink()
    report = monitor_ons_pay(data_root=root, key=key, now=_now())
    assert report.status == "unhealthy"
    assert _check(report, "curation_integrity").state == "pass"
    assert _check(report, "refresh_audit_integrity").state == "fail"
    assert _check(report, "vintage_refresh_success").state == "fail"
    assert report.metrics["last_vintage_refresh_age_hours"] is None
    assert "dpl_ons_pay_last_vintage_refresh_age_hours" not in format_prometheus(report)


def test_latest_failed_batch_alerts_even_with_previous_pinned_success(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, result = ready
    later = _now() + timedelta(minutes=5)
    original = json.loads(result.receipt_path.read_text())
    failed = {
        **original,
        "run_id": uuid4().hex,
        "created_at": later.isoformat(),
        "status": "failed",
        "sources": [],
        "metrics": {name: 0 for name in original["metrics"]},
        "failure": {
            "phase": "ingestion",
            "source_index": 0,
            "error_type": "HandoffError",
            "message": "simulated source failure",
        },
    }
    (root / "manifests" / f"ons_pay_refresh_{failed['run_id']}.json").write_text(
        json.dumps(failed), encoding="utf-8"
    )
    report = monitor_ons_pay(data_root=root, key=key, now=later + timedelta(minutes=1))
    assert report.status == "unhealthy"
    assert _check(report, "last_batch").state == "fail"
    assert _check(report, "vintage_refresh_success").state == "pass"
    assert report.metrics["last_batch_success"] == 0
    assert report.latest_vintage_success_run_id == result.run_id


@pytest.mark.parametrize(
    "fault", ["invalid_json", "symlink", "future", "duplicate_id", "invalid_metric"]
)
def test_unsafe_or_corrupt_audit_fails_closed(
    ready: tuple[Path, VintageKey, RefreshResult], tmp_path: Path, fault: str
) -> None:
    root, key, result = ready
    target = root / "manifests" / "ons_pay_refresh_extra.json"
    if fault == "invalid_json":
        target.write_text("{invalid", encoding="utf-8")
    elif fault == "symlink":
        external = tmp_path / "external.json"
        external.write_text("{}", encoding="utf-8")
        target.symlink_to(external)
    else:
        data = json.loads(result.receipt_path.read_text())
        if fault == "future":
            data["run_id"] = uuid4().hex
            data["created_at"] = (_now() + timedelta(days=1)).isoformat()
        elif fault == "invalid_metric":
            data["run_id"] = uuid4().hex
            data["metrics"]["source_rows"] = "not-a-count"
        target.write_text(json.dumps(data), encoding="utf-8")
    report = monitor_ons_pay(data_root=root, key=key, now=_now())
    assert report.status == "unhealthy"
    assert _check(report, "refresh_audit_integrity").state == "fail"
    assert _check(report, "curation_integrity").state == "pass"


def test_invalid_config_is_rejected_before_any_data_read(tmp_path: Path) -> None:
    key = VintageKey(1, "a" * 40, "b" * 64)
    for kwargs in (
        {"max_source_age_days": -1},
        {"max_source_age_days": True},
        {"max_refresh_age_hours": float("nan")},
        {"max_refresh_age_hours": -2},
        {"max_missing_pay_percent": 101},
        {"max_missing_pay_percent": float("inf")},
    ):
        with pytest.raises(MonitorError):
            monitor_ons_pay(data_root=tmp_path, key=key, **kwargs)
    with pytest.raises(MonitorError, match="exact v1"):
        monitor_ons_pay(
            data_root=tmp_path, key=VintageKey(2, key.registry_commit, key.source_sha256)
        )
    with pytest.raises(MonitorError, match="timezone"):
        monitor_ons_pay(data_root=tmp_path, key=key, now=datetime(2026, 10, 31))


def test_prometheus_gauges_have_pinned_labels_and_no_missing_zeros(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, _ = ready
    report = monitor_ons_pay(data_root=root, key=key, now=_now())
    metrics = format_prometheus(report)
    assert "dpl_ons_pay_monitor_healthy{" in metrics
    assert f'registry_commit="{key.registry_commit}"' in metrics
    assert f'source_sha256="{key.source_sha256}"' in metrics
    assert "dpl_ons_pay_source_rows{" in metrics
    assert "dpl_ons_pay_missing_pay_percent{" in metrics
    assert "# TYPE dpl_ons_pay_monitor_healthy gauge" in metrics
    assert not metrics.endswith("\n\n")


def test_monitoring_has_no_output_mutations(ready: tuple[Path, VintageKey, RefreshResult]) -> None:
    root, key, _ = ready
    before = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}
    monitor_ons_pay(data_root=root, key=key, now=_now())
    after = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}
    assert before == after


def test_cli_json_and_prometheus_and_error_exit_codes(
    ready: tuple[Path, VintageKey, RefreshResult], capsys: pytest.CaptureFixture[str]
) -> None:
    root, key, _ = ready
    args = [
        "ons-pay-monitor",
        "--data-root",
        str(root),
        "--registry-commit",
        key.registry_commit,
        "--source-sha256",
        key.source_sha256,
    ]
    platform_main([*args, "--max-source-age-days", "30"])
    assert json.loads(capsys.readouterr().out)["status"] == "healthy"
    platform_main([*args, "--format", "prometheus"])
    assert "dpl_ons_pay_monitor_healthy{" in capsys.readouterr().out

    with pytest.raises(SystemExit) as unhealthy:
        platform_main([*args, "--max-missing-pay-percent", "1"])
    assert unhealthy.value.code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "unhealthy"

    with pytest.raises(SystemExit) as invalid:
        platform_main([*args, "--max-source-age-days", "-1"])
    assert invalid.value.code == 2


def test_nonexistent_pinned_vintage_is_unhealthy_not_implicitly_promoted(
    ready: tuple[Path, VintageKey, RefreshResult],
) -> None:
    root, key, _ = ready
    report = monitor_ons_pay(
        data_root=root, key=VintageKey(1, "b" * 40, key.source_sha256), now=_now()
    )
    assert report.status == "unhealthy"
    assert _check(report, "curation_integrity").state == "fail"
    assert _check(report, "vintage_refresh_success").state == "fail"
