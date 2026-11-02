"""Offline, opt-in quality drift policies over SHA-linked release health history."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from test_ons_pay_release_health import _policy, _prepare

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_history_alerts import (
    HistoryAlertError,
    HistoryAlertReport,
    evaluate_ons_pay_history_alerts,
    format_history_alerts_prometheus,
)
from data_platform_lab.observability.ons_pay_release_health import assess_ons_pay_release
from data_platform_lab.observability.ons_pay_release_history import (
    ReleaseHealthArchive,
    ReleaseHistoryError,
)
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.serving.release_store import ReleaseStore


def _record(root: Path, *, minutes: int = 0, maximum: float = 20.0) -> None:
    report = assess_ons_pay_release(
        data_root=root,
        max_missing_pay_percent=maximum,
        now=datetime.now(UTC) + timedelta(minutes=minutes),
    )
    ReleaseHealthArchive(root).record(report)


def _checks(report: HistoryAlertReport) -> dict[str, tuple[str, int | float | None]]:
    return {check.name: (check.state, check.observed) for check in report.checks}


@pytest.fixture
def history_with_drift(tmp_path: Path) -> tuple[Path, tuple[VintageKey, ...]]:
    """Observe A -> B -> C on three adjacent new activation generations."""
    root, keys, _ = _prepare(tmp_path, three=True)
    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=3, reason="Restart A").applied
    _record(root, minutes=1)  # generation 4, A, healthy
    assert store.activate(
        candidate=keys[1], expected_generation=4, reason="Promote B", **_policy()
    ).applied
    _record(root, minutes=2)  # generation 5, B, unhealthy
    assert store.activate(
        candidate=keys[2], expected_generation=5, reason="Promote C", **_policy()
    ).applied
    _record(root, minutes=3)  # generation 6, C, unhealthy
    return root, keys


def test_no_active_release_or_records_is_explicit_alert_and_never_creates_data(
    tmp_path: Path,
) -> None:
    root = tmp_path / "absent-curation"
    report = evaluate_ons_pay_history_alerts(
        data_root=root,
        max_assessment_age_hours=24,
        alert_after_unhealthy=2,
        max_missing_pay_increase_pp=3,
    )
    assert report.status == "alert"
    assert report.active_generation is None
    assert report.window_assessments == 0
    assert _checks(report)["active_assessment"] == ("alert", 0)
    assert _checks(report)["assessment_age_hours"] == ("unavailable", None)
    assert _checks(report)["consecutive_unhealthy"] == ("unavailable", None)
    assert _checks(report)["missing_pay_increase_pp"] == ("unavailable", None)
    assert _checks(report)["unhealthy_share_percent"][0] == "skipped"
    assert not root.exists()


def test_prior_generation_health_cannot_satisfy_current_active_check(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root)
    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=2, reason="Operator").applied
    report = evaluate_ons_pay_history_alerts(data_root=root, alert_after_unhealthy=1)
    assert report.active_generation == 3
    assert _checks(report)["active_assessment"] == ("alert", 0)
    assert _checks(report)["consecutive_unhealthy"] == ("unavailable", None)
    assert report.status == "alert"


def test_current_healthy_snapshot_passes_configured_non_drift_policies(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root, maximum=40)
    report = evaluate_ons_pay_history_alerts(
        data_root=root,
        max_assessment_age_hours=2,
        alert_after_unhealthy=2,
        max_unhealthy_share_percent=10,
    )
    assert report.status == "ok"
    checks = _checks(report)
    assert checks["active_assessment"] == ("pass", 1)
    assert checks["consecutive_unhealthy"] == ("pass", 0)
    assert checks["unhealthy_share_percent"] == ("pass", 0.0)
    assert checks["assessment_age_hours"][0] == "pass"
    assert checks["missing_pay_increase_pp"][0] == "skipped"
    assert format_history_alerts_prometheus(report).endswith(
        'dpl_ons_pay_history_policy_state{policy="cumulative_missing_pay_drift_pp"} 3\n'
    )


def test_repeat_unhealthy_triggers_streak_and_observation_weighted_share(
    tmp_path: Path,
) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root, minutes=1)
    _record(root, minutes=2)
    report = evaluate_ons_pay_history_alerts(
        data_root=root,
        now=datetime.now(UTC) + timedelta(minutes=3),
        alert_after_unhealthy=2,
        max_unhealthy_share_percent=50,
    )
    assert report.status == "alert"
    assert _checks(report)["consecutive_unhealthy"] == ("alert", 2)
    assert _checks(report)["unhealthy_share_percent"] == ("alert", 100.0)
    metrics = format_history_alerts_prometheus(report)
    assert 'dpl_ons_pay_history_policy_state{policy="consecutive_unhealthy"} 1' in metrics
    assert "dpl_ons_pay_history_checks_alerting" in metrics
    assert report.active_vintage is not None
    assert f'source_sha256="{report.active_vintage.source_sha256}"' in metrics


def test_adjacent_release_drift_and_cumulative_positive_drift(
    history_with_drift: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = history_with_drift
    report = evaluate_ons_pay_history_alerts(
        data_root=root,
        now=datetime.now(UTC) + timedelta(minutes=4),
        last=50,
        max_assessment_age_hours=1,
        alert_after_unhealthy=1,
        max_unhealthy_share_percent=50,
        max_missing_pay_increase_pp=10,
        max_cumulative_missing_pay_drift_pp=25,
    )
    assert report.active_generation == 6
    assert report.window_assessments == 3 and report.window_generations == 3
    checks = _checks(report)
    assert checks["active_assessment"] == ("pass", 1)
    assert checks["assessment_age_hours"][0] == "pass"
    assert checks["consecutive_unhealthy"] == ("alert", 1)
    assert checks["unhealthy_share_percent"] == ("alert", 100.0)
    assert checks["missing_pay_increase_pp"][0] == "alert"
    assert checks["cumulative_missing_pay_drift_pp"][0] == "alert"
    assert checks["missing_pay_increase_pp"][1] == pytest.approx(14.285714, abs=0.00001)
    assert checks["cumulative_missing_pay_drift_pp"][1] == pytest.approx(28.571429, abs=0.00001)
    assert report.status == "alert"


def test_missing_adjacent_generation_or_earlier_window_is_unavailable(
    history_with_drift: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = history_with_drift
    last_two = evaluate_ons_pay_history_alerts(
        data_root=root,
        now=datetime.now(UTC) + timedelta(minutes=4),
        last=2,
        max_missing_pay_increase_pp=20,
        max_cumulative_missing_pay_drift_pp=25,
    )
    assert last_two.status == "ok"
    assert _checks(last_two)["missing_pay_increase_pp"][0] == "pass"
    assert _checks(last_two)["cumulative_missing_pay_drift_pp"][0] == "pass"

    only_latest = evaluate_ons_pay_history_alerts(
        data_root=root,
        now=datetime.now(UTC) + timedelta(minutes=4),
        last=1,
        max_missing_pay_increase_pp=20,
        max_cumulative_missing_pay_drift_pp=25,
    )
    assert only_latest.status == "unavailable"
    assert _checks(only_latest)["active_assessment"][0] == "pass"
    assert _checks(only_latest)["missing_pay_increase_pp"][0] == "unavailable"
    assert _checks(only_latest)["cumulative_missing_pay_drift_pp"][0] == "unavailable"


def test_recorded_generation_gap_is_not_interpolated(tmp_path: Path) -> None:
    root, keys, _ = _prepare(tmp_path, three=True)
    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=3, reason="A again").applied
    _record(root, minutes=1)
    assert store.activate(candidate=keys[1], expected_generation=4, reason="B", **_policy()).applied
    assert store.activate(candidate=keys[2], expected_generation=5, reason="C", **_policy()).applied
    _record(root, minutes=2)  # generation 5 intentionally has no observation
    report = evaluate_ons_pay_history_alerts(
        data_root=root,
        now=datetime.now(UTC) + timedelta(minutes=3),
        max_missing_pay_increase_pp=20,
        max_cumulative_missing_pay_drift_pp=25,
    )
    assert _checks(report)["active_assessment"][0] == "pass"
    assert _checks(report)["missing_pay_increase_pp"][0] == "unavailable"
    assert _checks(report)["cumulative_missing_pay_drift_pp"][0] == "unavailable"
    assert report.status == "unavailable"


def test_stale_or_future_dated_current_assessment_is_not_fresh(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root)
    stale = evaluate_ons_pay_history_alerts(
        data_root=root,
        max_assessment_age_hours=1,
        now=datetime.now(UTC) + timedelta(hours=3),
    )
    assert _checks(stale)["assessment_age_hours"][0] == "alert"
    assert stale.status == "alert"

    future = evaluate_ons_pay_history_alerts(
        data_root=root,
        max_assessment_age_hours=1,
        now=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert _checks(future)["active_assessment"][0] == "alert"
    assert _checks(future)["assessment_age_hours"][0] == "unavailable"


def test_corrupted_active_parquet_has_no_comparable_missing_rate(tmp_path: Path) -> None:
    root, keys, _ = _prepare(tmp_path)
    curated_path = CuratedCatalog(root).get_vintage(keys[1]).artifacts["observations"].path
    curated_path.write_bytes(b"damaged Parquet")
    _record(root)
    report = evaluate_ons_pay_history_alerts(
        data_root=root, max_missing_pay_increase_pp=3, max_cumulative_missing_pay_drift_pp=8
    )
    assert _checks(report)["active_assessment"][0] == "pass"
    assert _checks(report)["missing_pay_increase_pp"][0] == "unavailable"
    assert _checks(report)["cumulative_missing_pay_drift_pp"][0] == "unavailable"
    assert report.status == "unavailable"


def test_history_alerts_fail_closed_on_old_tampered_record(
    history_with_drift: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = history_with_drift
    archive = ReleaseHealthArchive(root)
    old = archive.trends().samples[0]
    path = archive.records / f"{old.record_sha256}.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ReleaseHistoryError, match="health assessment"):
        evaluate_ons_pay_history_alerts(data_root=root, last=1)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"last": 0}, "last"),
        ({"last": True}, "last"),
        ({"alert_after_unhealthy": 0}, "alert_after_unhealthy"),
        ({"alert_after_unhealthy": True}, "alert_after_unhealthy"),
        ({"max_assessment_age_hours": -1.0}, "max_assessment_age_hours"),
        ({"max_missing_pay_increase_pp": math.nan}, "max_missing_pay_increase_pp"),
        ({"max_unhealthy_share_percent": 101}, "max_unhealthy_share_percent"),
        ({"max_cumulative_missing_pay_drift_pp": math.inf}, "max_cumulative"),
        ({"max_assessment_age_hours": True}, "max_assessment_age_hours"),
        ({"max_assessment_age_hours": 10**400}, "max_assessment_age_hours"),
    ],
)
def test_invalid_policies_fail_before_archive_access(
    tmp_path: Path, options: dict[str, object], message: str
) -> None:
    root = tmp_path / "absent-curation"
    with pytest.raises(HistoryAlertError, match=message):
        evaluate_ons_pay_history_alerts(data_root=root, **options)  # type: ignore[arg-type]
    assert not root.exists()


def test_naive_reference_time_rejected(tmp_path: Path) -> None:
    with pytest.raises(HistoryAlertError, match="timezone-aware"):
        evaluate_ons_pay_history_alerts(data_root=tmp_path, now=datetime(2026, 1, 1))


def test_alert_cli_json_and_explicit_prometheus_export_are_read_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root, maximum=40)
    store = ReleaseStore(root)
    head_before = store.head.read_bytes()
    events_before = {p.name: p.read_bytes() for p in store.events.glob("*.json")}
    records_before = {p.name: p.read_bytes() for p in ReleaseHealthArchive(root).records.glob("*")}
    prefix = ["ons-pay-release", "--data-root", str(root), "alerts"]

    platform_main([*prefix, "--max-assessment-age-hours", "1", "--alert-after-unhealthy", "2"])
    body = json.loads(capsys.readouterr().out)
    assert body["status"] == "ok" and body["active_generation"] == 2
    assert body["checks"][0]["name"] == "active_assessment"

    textfile = tmp_path / "release-alerts.prom"
    platform_main([*prefix, "--format", "prometheus", "--textfile", str(textfile)])
    metrics = capsys.readouterr().out
    assert textfile.read_text(encoding="utf-8") == metrics
    assert "dpl_ons_pay_history_alert" in metrics
    assert 'dpl_ons_pay_history_policy_state{policy="active_assessment"} 0' in metrics
    assert store.head.read_bytes() == head_before
    assert {p.name: p.read_bytes() for p in store.events.glob("*.json")} == events_before
    assert {
        p.name: p.read_bytes() for p in ReleaseHealthArchive(root).records.glob("*")
    } == records_before


def test_alert_cli_exit_codes_and_invalid_textfile_usage(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, _, _ = _prepare(tmp_path)
    prefix = ["ons-pay-release", "--data-root", str(root), "alerts"]
    with pytest.raises(SystemExit) as no_assessment:
        platform_main(prefix)
    assert no_assessment.value.code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "alert"

    for extra in (
        ["--last", "0"],
        ["--max-assessment-age-hours", "nan"],
        ["--alert-after-unhealthy", "0"],
        ["--textfile", str(tmp_path / "invalid.prom")],
        ["--format", "prometheus", "--textfile", str(tmp_path / "invalid.txt")],
    ):
        with pytest.raises(SystemExit) as bad_request:
            platform_main([*prefix, *extra])
        assert bad_request.value.code == 2
        assert capsys.readouterr().err
    assert not ReleaseHealthArchive(root).root.exists()


def test_concurrent_active_pointer_switch_invalidates_alert_report(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root)
    read = ReleaseStore.history
    calls = 0

    def changed(store: ReleaseStore):
        nonlocal calls
        calls += 1
        state = read(store)
        if calls >= 4 and state is not None:
            return replace(state, generation=state.generation + 1)
        return state

    with (
        patch.object(ReleaseStore, "history", changed),
        pytest.raises(HistoryAlertError, match="active release changed"),
    ):
        evaluate_ons_pay_history_alerts(data_root=root)
