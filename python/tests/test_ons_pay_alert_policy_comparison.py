"""Offline paired-policy backtests on one verified release-health archive."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
from test_ons_pay_history_replay import _record
from test_ons_pay_release_health import _policy, _prepare

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_alert_comparison import (
    PolicyComparison,
    _pair_points,
    compare_ons_pay_alert_policies,
    format_alert_policy_comparison_csv,
)
from data_platform_lab.observability.ons_pay_alert_policy import AlertPolicy
from data_platform_lab.observability.ons_pay_history_alerts import HistoryAlertError
from data_platform_lab.observability.ons_pay_history_replay import replay_ons_pay_history_alerts
from data_platform_lab.observability.ons_pay_release_history import (
    HealthTrends,
    ReleaseHealthArchive,
    ReleaseHistoryError,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseState, ReleaseStore


@pytest.fixture
def observations(tmp_path: Path) -> tuple[Path, tuple[VintageKey, ...]]:
    """Prepare three adjacent generations and a repeated unhealthy C check."""
    root, keys, _ = _prepare(tmp_path, three=True)
    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=3, reason="Restart A").applied
    _record(root, 1)
    assert store.activate(
        candidate=keys[1], expected_generation=4, reason="Promote B", **_policy()
    ).applied
    _record(root, 2)
    assert store.activate(
        candidate=keys[2], expected_generation=5, reason="Promote C", **_policy()
    ).applied
    _record(root, 3)
    _record(root, 4)
    return root, keys


def _baseline(*, last: int = 50) -> AlertPolicy:
    return AlertPolicy(
        name="comparison-baseline",
        last=last,
        alert_after_unhealthy=3,
        max_missing_pay_increase_pp=20,
        max_cumulative_missing_pay_drift_pp=50,
    )


def _candidate(*, last: int = 50) -> AlertPolicy:
    return AlertPolicy(
        name="comparison-candidate",
        last=last,
        alert_after_unhealthy=2,
        max_unhealthy_share_percent=80,
        max_missing_pay_increase_pp=10,
        max_cumulative_missing_pay_drift_pp=25,
    )


def _matrix(report: PolicyComparison) -> dict[tuple[str, str], int]:
    """Index the fixed transition matrix without relying on display order."""
    return {(cell.baseline, cell.candidate): cell.checkpoints for cell in report.transitions}


def test_comparison_pairs_one_archive_and_counts_transitions(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, keys = observations
    original = ReleaseHealthArchive.trends
    archive_reads = 0

    def counted_trends(
        archive: ReleaseHealthArchive, *, last: int | None = 50, generation: int | None = None
    ) -> HealthTrends:
        nonlocal archive_reads
        archive_reads += 1
        return original(archive, last=last, generation=generation)

    with patch.object(ReleaseHealthArchive, "trends", counted_trends):
        report = compare_ons_pay_alert_policies(
            data_root=root, baseline=_baseline(), candidate=_candidate()
        )

    assert archive_reads == 1
    assert report.total_records == 4
    assert report.summary.evaluated == 4
    assert [point.generation for point in report.points] == [4, 5, 6, 6]
    assert [point.vintage for point in report.points] == [keys[0], keys[1], keys[2], keys[2]]
    assert [point.baseline_status for point in report.points] == [
        "unavailable",
        "ok",
        "ok",
        "ok",
    ]
    assert [point.candidate_status for point in report.points] == [
        "unavailable",
        "alert",
        "alert",
        "alert",
    ]
    assert report.summary.status_unchanged == 1
    assert report.summary.status_changed == 3
    assert report.summary.check_evidence_changed == 4
    assert report.summary.alert_added == 3
    assert report.summary.alert_cleared == 0
    assert report.summary.alert_to_unavailable == 0
    matrix = _matrix(report)
    assert len(matrix) == 9
    assert sum(matrix.values()) == 4
    assert matrix["ok", "alert"] == 3
    assert matrix["unavailable", "unavailable"] == 1
    assert all(point.baseline_checks and point.candidate_checks for point in report.points)
    step_difference = next(
        change
        for change in report.points[1].check_differences
        if change.name == "missing_pay_increase_pp"
    )
    assert step_difference.baseline.state == "pass"
    assert step_difference.candidate.state == "alert"
    assert step_difference.baseline.threshold == 20
    assert step_difference.candidate.threshold == 10


def test_identical_rules_have_only_diagonal_transitions(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    same_rules_different_name = replace(_candidate(), name="candidate-renamed")
    report = compare_ons_pay_alert_policies(
        data_root=root, baseline=_candidate(), candidate=same_rules_different_name
    )
    assert report.baseline_policy_sha256 != report.candidate_policy_sha256
    assert report.summary.status_unchanged == 4
    assert report.summary.status_changed == 0
    assert report.summary.check_evidence_changed == 0
    assert all(not point.check_differences for point in report.points)
    assert all(
        cell.checkpoints == 0 for cell in report.transitions if cell.baseline != cell.candidate
    )
    assert sum(cell.checkpoints for cell in report.transitions) == 4


def test_alert_to_unavailable_is_not_reported_as_cleared(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    baseline = AlertPolicy(name="long-drift", last=50, max_missing_pay_increase_pp=10)
    candidate = AlertPolicy(name="short-drift", last=1, max_missing_pay_increase_pp=10)
    report = compare_ons_pay_alert_policies(
        data_root=root, baseline=baseline, candidate=candidate, replay_last=1
    )
    assert report.total_records == 4
    assert report.summary.evaluated == 1
    assert report.points[0].baseline_window_assessments == 4
    assert report.points[0].candidate_window_assessments == 1
    assert report.points[0].baseline_status == "alert"
    assert report.points[0].candidate_status == "unavailable"
    assert report.summary.alert_cleared == 0
    assert report.summary.alert_to_unavailable == 1
    assert report.summary.became_unavailable == 1
    assert _matrix(report)["alert", "unavailable"] == 1
    assert any(
        change.name == "missing_pay_increase_pp"
        and change.baseline.state == "alert"
        and change.candidate.state == "unavailable"
        for change in report.points[0].check_differences
    )


def test_alert_to_ok_is_the_only_cleared_transition(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    report = compare_ons_pay_alert_policies(
        data_root=root, baseline=_candidate(), candidate=_baseline()
    )
    assert report.summary.alert_cleared == 3
    assert report.summary.alert_to_unavailable == 0
    assert report.summary.alert_added == 0
    assert _matrix(report)["alert", "ok"] == 3


def test_unavailable_to_alert_is_counted_without_implying_better_accuracy(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    short = AlertPolicy(name="short-drift", last=1, max_missing_pay_increase_pp=10)
    long = AlertPolicy(name="long-drift", last=50, max_missing_pay_increase_pp=10)
    report = compare_ons_pay_alert_policies(
        data_root=root, baseline=short, candidate=long, replay_last=1
    )
    assert report.points[0].baseline_status == "unavailable"
    assert report.points[0].candidate_status == "alert"
    assert report.summary.alert_added == 1
    assert report.summary.no_longer_unavailable == 1
    assert _matrix(report)["unavailable", "alert"] == 1


def test_comparison_is_deterministic_json_and_flat_csv(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    args = {"data_root": root, "baseline": _baseline(), "candidate": _candidate(), "replay_last": 2}
    first = compare_ons_pay_alert_policies(**args)
    second = compare_ons_pay_alert_policies(**args)
    assert first.to_dict() == second.to_dict()
    assert json.dumps(first.to_dict(), sort_keys=True) == json.dumps(
        second.to_dict(), sort_keys=True
    )
    exported = format_alert_policy_comparison_csv(first)
    assert exported == format_alert_policy_comparison_csv(second)
    rows = list(csv.DictReader(io.StringIO(exported)))
    assert len(rows) == 2
    assert rows[0]["record_sha256"] == first.points[0].record_sha256
    assert rows[0]["baseline_policy_sha256"] == _baseline().sha256
    assert rows[0]["candidate_policy_sha256"] == _candidate().sha256
    assert rows[0]["baseline_status"] == "ok"
    assert rows[0]["candidate_status"] == "alert"
    assert rows[0]["baseline_missing_pay_increase_pp_state"] == "pass"
    assert rows[0]["candidate_cumulative_missing_pay_drift_pp_state"] == "alert"
    assert "missing_pay_increase_pp" in rows[0]["changed_check_names"]
    assert rows[0]["source_sha256"] == first.points[0].vintage.source_sha256


def test_cli_json_and_csv_are_read_only(
    observations: tuple[Path, tuple[VintageKey, ...]],
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, _ = observations
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(_baseline().to_dict()), encoding="utf-8")
    candidate_path.write_text(json.dumps(_candidate().to_dict()), encoding="utf-8")
    store = ReleaseStore(root)
    archive = ReleaseHealthArchive(root)
    head = store.head.read_bytes()
    events = {path.name: path.read_bytes() for path in store.events.glob("*.json")}
    records = {path.name: path.read_bytes() for path in archive.records.glob("*.json")}
    command = [
        "ons-pay-release",
        "--data-root",
        str(root),
        "compare-alert-policies",
        "--baseline-policy-file",
        str(baseline_path),
        "--candidate-policy-file",
        str(candidate_path),
        "--replay-last",
        "2",
    ]
    platform_main(command)
    report = json.loads(capsys.readouterr().out)
    assert report["summary"]["evaluated"] == 2
    assert report["summary"]["alert_added"] == 2
    assert report["candidate_policy_sha256"] == _candidate().sha256
    platform_main([*command, "--format", "csv"])
    rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert len(rows) == 2
    assert head == store.head.read_bytes()
    assert events == {path.name: path.read_bytes() for path in store.events.glob("*.json")}
    assert records == {path.name: path.read_bytes() for path in archive.records.glob("*.json")}


def test_empty_archive_and_invalid_limits_fail_without_writing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "absent"
    report = compare_ons_pay_alert_policies(
        data_root=root, baseline=_baseline(), candidate=_candidate()
    )
    assert report.total_records == 0
    assert report.summary.evaluated == 0
    assert report.points == ()
    assert len(report.transitions) == 9
    assert "baseline_policy_sha256,candidate_policy_sha256" in format_alert_policy_comparison_csv(
        report
    )
    for limit in (0, 1001, True, 1.5, "1"):
        with pytest.raises(HistoryAlertError, match="replay_last"):
            compare_ons_pay_alert_policies(
                data_root=root,
                baseline=_baseline(),
                candidate=_candidate(),
                replay_last=limit,  # type: ignore[arg-type]
            )
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(_baseline().to_dict()), encoding="utf-8")
    candidate_path.write_text(json.dumps(_candidate().to_dict()), encoding="utf-8")
    command = [
        "ons-pay-release",
        "--data-root",
        str(root),
        "compare-alert-policies",
        "--baseline-policy-file",
        str(baseline_path),
        "--candidate-policy-file",
        str(candidate_path),
    ]
    with pytest.raises(SystemExit) as empty:
        platform_main(command)
    assert empty.value.code == 1
    assert json.loads(capsys.readouterr().out)["summary"]["evaluated"] == 0
    with pytest.raises(SystemExit) as empty_csv:
        platform_main([*command, "--format", "csv"])
    assert empty_csv.value.code == 1
    assert capsys.readouterr().out.startswith("baseline_policy_sha256,")
    for invalid in (
        ["--replay-last", "0"],
        ["--replay-last", "1001"],
        ["--candidate-policy-file", str(tmp_path / "not-found.json")],
    ):
        with pytest.raises(SystemExit) as error:
            platform_main([*command, *invalid])
        assert error.value.code == 2
        assert capsys.readouterr().err
    candidate_path.write_text(
        '{"schema_version": 1, "name": "duplicate", "name": "ambiguous"}',
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as ambiguous:
        platform_main(command)
    assert ambiguous.value.code == 2
    assert "duplicate" in capsys.readouterr().err
    assert not root.exists()


def test_tampered_old_record_invalidates_even_single_checkpoint(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    archive = ReleaseHealthArchive(root)
    oldest = archive.trends(last=None).samples[0]
    path = archive.records / f"{oldest.record_sha256}.json"
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ReleaseHistoryError, match="assessment"):
        compare_ons_pay_alert_policies(
            data_root=root, baseline=_baseline(), candidate=_candidate(), replay_last=1
        )


def test_concurrent_pointer_change_invalidates_both_policies(tmp_path: Path) -> None:
    root, _, _ = _prepare(tmp_path)
    _record(root, 1)
    original = ReleaseStore.history
    calls = 0

    def changed(store: ReleaseStore) -> ReleaseState | None:
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
        compare_ons_pay_alert_policies(data_root=root, baseline=_baseline(), candidate=_candidate())


def test_misaligned_checkpoint_fails_closed(
    observations: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = observations
    baseline_point = replay_ons_pay_history_alerts(data_root=root, policy=_baseline()).points[0]
    with pytest.raises(HistoryAlertError, match="not aligned"):
        _pair_points(baseline_point, replace(baseline_point, event_sha256="0" * 64))
    with pytest.raises(HistoryAlertError, match="check schemas"):
        _pair_points(baseline_point, replace(baseline_point, checks=baseline_point.checks[:-1]))
