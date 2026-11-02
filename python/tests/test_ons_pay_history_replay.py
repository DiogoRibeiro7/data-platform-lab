"""Offline, point-in-time replay of verified quality-alert history."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from test_ons_pay_release_health import _policy, _prepare

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_alert_policy import AlertPolicy
from data_platform_lab.observability.ons_pay_history_alerts import HistoryAlertError
from data_platform_lab.observability.ons_pay_history_replay import (
    ReplayPoint,
    format_history_replay_csv,
    replay_ons_pay_history_alerts,
)
from data_platform_lab.observability.ons_pay_release_health import assess_ons_pay_release
from data_platform_lab.observability.ons_pay_release_history import (
    ReleaseHealthArchive,
    ReleaseHistoryError,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseStore


def _record(root: Path, minutes: int, *, maximum: float = 20) -> None:
    report = assess_ons_pay_release(
        data_root=root,
        max_missing_pay_percent=maximum,
        now=datetime.now(UTC) + timedelta(minutes=minutes),
    )
    ReleaseHealthArchive(root).record(report)


@pytest.fixture
def observations(tmp_path: Path) -> tuple[Path, tuple[VintageKey, ...]]:
    """Three adjacent observed generations, then repeated unhealthy C."""
    root, keys, _ = _prepare(tmp_path, three=True)
    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=3, reason="Restart A").applied
    _record(root, 1)  # generation 4, A healthy
    assert store.activate(
        candidate=keys[1], expected_generation=4, reason="Promote B", **_policy()
    ).applied
    _record(root, 2)  # generation 5, B unhealthy
    assert store.activate(
        candidate=keys[2], expected_generation=5, reason="Promote C", **_policy()
    ).applied
    _record(root, 3)  # generation 6, C unhealthy
    _record(root, 4)  # generation 6, C repeated unhealthy
    return root, keys


def _policy_example(*, last: int = 50) -> AlertPolicy:
    return AlertPolicy(
        name="ons-pay-replay",
        last=last,
        alert_after_unhealthy=2,
        max_unhealthy_share_percent=80,
        max_missing_pay_increase_pp=10,
        max_cumulative_missing_pay_drift_pp=25,
    )


def _check_states(point: ReplayPoint) -> dict[str, str]:
    """Map a checkpoint's fixed checks to their replayed alert states."""
    return {check.name: check.state for check in point.checks}


def test_replay_uses_only_prior_observations_and_detects_policy_drift(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, keys = observations
    report = replay_ons_pay_history_alerts(data_root=root, policy=_policy_example())
    assert report.total_records == 4
    assert report.summary.evaluated == 4
    assert (report.summary.ok, report.summary.alert, report.summary.unavailable) == (0, 3, 1)
    assert [point.generation for point in report.points] == [4, 5, 6, 6]
    assert [point.vintage for point in report.points] == [keys[0], keys[1], keys[2], keys[2]]
    assert [point.recorded_health_status for point in report.points] == [
        "healthy",
        "unhealthy",
        "unhealthy",
        "unhealthy",
    ]
    assert [point.window_assessments for point in report.points] == [1, 2, 3, 4]
    assert _check_states(report.points[0])["active_assessment"] == "pass"
    assert _check_states(report.points[0])["missing_pay_increase_pp"] == "unavailable"
    assert _check_states(report.points[0])["cumulative_missing_pay_drift_pp"] == "unavailable"
    assert _check_states(report.points[1])["missing_pay_increase_pp"] == "alert"
    assert _check_states(report.points[1])["cumulative_missing_pay_drift_pp"] == "pass"
    assert _check_states(report.points[2])["cumulative_missing_pay_drift_pp"] == "alert"
    assert _check_states(report.points[2])["consecutive_unhealthy"] == "pass"
    assert _check_states(report.points[3])["consecutive_unhealthy"] == "alert"
    assert report.policy_sha256 == _policy_example().sha256


def test_display_limit_does_not_truncate_prior_policy_evidence(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    only_last = replay_ons_pay_history_alerts(
        data_root=root, policy=_policy_example(), replay_last=1
    )
    assert only_last.total_records == 4
    assert only_last.summary.evaluated == 1
    assert only_last.points[0].window_assessments == 4
    assert _check_states(only_last.points[0])["consecutive_unhealthy"] == "alert"
    assert _check_states(only_last.points[0])["cumulative_missing_pay_drift_pp"] == "alert"

    short_policy = replay_ons_pay_history_alerts(
        data_root=root, policy=_policy_example(last=2), replay_last=1
    )
    assert short_policy.points[0].window_assessments == 2
    assert _check_states(short_policy.points[0])["consecutive_unhealthy"] == "alert"
    assert _check_states(short_policy.points[0])["cumulative_missing_pay_drift_pp"] == "unavailable"


def test_csv_is_flat_fixed_schema_and_deterministic(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    a = replay_ons_pay_history_alerts(data_root=root, policy=_policy_example(), replay_last=2)
    b = replay_ons_pay_history_alerts(data_root=root, policy=_policy_example(), replay_last=2)
    assert a.to_dict() == b.to_dict()
    csv_text = format_history_replay_csv(a)
    assert csv_text == format_history_replay_csv(b)
    rows = list(csv.DictReader(io.StringIO(csv_text)))
    assert len(rows) == 2
    assert rows[0]["generation"] == "6"
    assert rows[0]["policy_sha256"] == a.policy_sha256
    assert rows[0]["cumulative_missing_pay_drift_pp_state"] == "alert"
    assert rows[1]["consecutive_unhealthy_state"] == "alert"
    assert "consecutive_unhealthy" in rows[1]["alerting_policies"]
    assert rows[0]["source_sha256"] == a.points[0].vintage.source_sha256


def test_replay_is_read_only_and_cli_can_export_json_and_csv(
    observations: tuple[Path, tuple[VintageKey, ...]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, _ = observations
    store = ReleaseStore(root)
    archive = ReleaseHealthArchive(root)
    head = store.head.read_bytes()
    events = {p.name: p.read_bytes() for p in store.events.glob("*.json")}
    records = {p.name: p.read_bytes() for p in archive.records.glob("*.json")}
    policy_path = tmp_path / "example.json"
    policy_path.write_text(json.dumps(_policy_example().to_dict()), encoding="utf-8")
    prefix = [
        "ons-pay-release",
        "--data-root",
        str(root),
        "replay-alerts",
        "--policy-file",
        str(policy_path),
        "--replay-last",
        "2",
    ]
    platform_main(prefix)
    body = json.loads(capsys.readouterr().out)
    assert body["summary"]["evaluated"] == 2 and body["summary"]["alert"] == 2
    assert body["policy_sha256"] == _policy_example().sha256
    platform_main([*prefix, "--format", "csv"])
    rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert len(rows) == 2
    assert head == store.head.read_bytes()
    assert events == {p.name: p.read_bytes() for p in store.events.glob("*.json")}
    assert records == {p.name: p.read_bytes() for p in archive.records.glob("*.json")}


def test_cli_overrides_change_replayed_window(
    observations: tuple[Path, tuple[VintageKey, ...]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, _ = observations
    config = tmp_path / "policy.json"
    config.write_text(json.dumps(_policy_example(last=2).to_dict()), encoding="utf-8")
    platform_main(
        [
            "ons-pay-release",
            "--data-root",
            str(root),
            "replay-alerts",
            "--policy-file",
            str(config),
            "--last",
            "50",
            "--replay-last",
            "1",
        ]
    )
    report = json.loads(capsys.readouterr().out)
    assert report["policy"]["last"] == 50
    assert report["points"][0]["window_assessments"] == 4
    assert report["points"][0]["checks"][5]["state"] == "alert"
    assert report["policy_sha256"] != _policy_example(last=2).sha256


def test_new_observations_do_not_change_past_replay_decisions(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root, 1)
    policy = AlertPolicy(name="no-future", alert_after_unhealthy=1)
    old = replay_ons_pay_history_alerts(data_root=root, policy=policy)
    assert len(old.points) == 1

    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=2, reason="Back to A").applied
    _record(root, 2)
    later = replay_ons_pay_history_alerts(data_root=root, policy=policy)
    assert later.total_records == 2
    assert later.points[0] == old.points[0]


def test_empty_archive_is_unavailable_without_creating_directories(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "absent"
    report = replay_ons_pay_history_alerts(data_root=root)
    assert report.total_records == 0 and report.summary.evaluated == 0
    assert report.points == ()
    assert format_history_replay_csv(report).startswith("policy_sha256,record_sha256,")
    with pytest.raises(SystemExit) as empty:
        platform_main(["ons-pay-release", "--data-root", str(root), "replay-alerts"])
    assert empty.value.code == 1
    assert json.loads(capsys.readouterr().out)["summary"]["evaluated"] == 0
    assert not root.exists()


@pytest.mark.parametrize("last", [0, 1001, True, 1.5, "3"])
def test_invalid_replay_limits_fail_before_archive_reads(tmp_path: Path, last: object) -> None:
    root = tmp_path / "absent"
    with pytest.raises(HistoryAlertError, match="replay_last"):
        replay_ons_pay_history_alerts(data_root=root, replay_last=last)  # type: ignore[arg-type]
    assert not root.exists()


def test_cli_invalid_bounds_and_policy_are_errors(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "absent"
    base = ["ons-pay-release", "--data-root", str(root), "replay-alerts"]
    for extra in (
        ["--replay-last", "0"],
        ["--replay-last", "1001"],
        ["--last", "0"],
        ["--max-assessment-age-hours", "nan"],
        ["--policy-file", str(tmp_path / "missing.json")],
    ):
        with pytest.raises(SystemExit) as failed:
            platform_main([*base, *extra])
        assert failed.value.code == 2
        assert capsys.readouterr().err
    assert not root.exists()


def test_old_tampered_file_invalidates_even_single_checkpoint(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    archive = ReleaseHealthArchive(root)
    oldest = archive.trends(last=None).samples[0]
    path = archive.records / f"{oldest.record_sha256}.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ReleaseHistoryError, match="assessment"):
        replay_ons_pay_history_alerts(data_root=root, replay_last=1)


def test_concurrent_pointer_change_invalidates_entire_replay(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root, 1)
    original = ReleaseStore.history
    calls = 0

    def changed(store: ReleaseStore):
        nonlocal calls
        calls += 1
        state = original(store)
        if calls >= 4 and state is not None:
            return replace(state, generation=state.generation + 1)
        return state

    with (
        patch.object(ReleaseStore, "history", changed),
        pytest.raises(HistoryAlertError, match="active release changed"),
    ):
        replay_ons_pay_history_alerts(data_root=root)
