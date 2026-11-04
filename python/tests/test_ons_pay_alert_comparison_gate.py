"""Offline, fail-closed acceptance budgets for paired alert policy decisions."""

from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest
from test_ons_pay_alert_policy_comparison import _baseline, _candidate
from test_ons_pay_history_replay import _record
from test_ons_pay_release_health import _policy, _prepare

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_alert_comparison import (
    compare_ons_pay_alert_policies,
)
from data_platform_lab.observability.ons_pay_alert_comparison_gate import (
    AlertComparisonGate,
    AlertComparisonGateError,
    evaluate_alert_comparison_gate,
    load_alert_comparison_gate,
)
from data_platform_lab.observability.ons_pay_alert_policy import AlertPolicy
from data_platform_lab.observability.ons_pay_release_history import ReleaseHealthArchive
from data_platform_lab.serving.release_store import ReleaseStore


@pytest.fixture
def history_root(tmp_path: Path) -> Path:
    """Create adjacent A/B/C historical releases with four assessments."""
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
    return root


def _gate(*, max_alert_added: int = 3) -> AlertComparisonGate:
    """Build one explicit, immutable operator review configuration."""
    return AlertComparisonGate(
        name="review-rules",
        min_evaluated=4,
        max_alert_added=max_alert_added,
        max_became_unavailable=0,
        max_alert_to_unavailable=0,
        max_status_changed=3,
    )


def _comparison_command(
    root: Path, baseline_file: Path, candidate_file: Path, gate_file: Path
) -> list[str]:
    """Return a completely explicit, source-pinned comparison invocation."""
    return [
        "ons-pay-release",
        "--data-root",
        str(root),
        "compare-alert-policies",
        "--baseline-policy-file",
        str(baseline_file),
        "--candidate-policy-file",
        str(candidate_file),
        "--gate-file",
        str(gate_file),
        "--replay-last",
        "4",
    ]


def _write_configs(tmp_path: Path, gate: AlertComparisonGate) -> tuple[Path, Path, Path]:
    """Write three test-local explicit versioned JSON configurations."""
    baseline = tmp_path / "baseline.json"
    candidate = tmp_path / "candidate.json"
    review = tmp_path / "review.json"
    baseline.write_text(json.dumps(_baseline().to_dict()), encoding="utf-8")
    candidate.write_text(json.dumps(_candidate().to_dict()), encoding="utf-8")
    review.write_text(json.dumps(gate.to_dict()), encoding="utf-8")
    return baseline, candidate, review


def test_passing_gate_preserves_fingerprints_and_budget_evidence(history_root: Path) -> None:
    report = compare_ons_pay_alert_policies(
        data_root=history_root, baseline=_baseline(), candidate=_candidate()
    )
    gate = _gate()
    evaluation = evaluate_alert_comparison_gate(report=report, gate=gate)
    assert evaluation.status == "pass"
    assert evaluation.gate_sha256 == gate.sha256
    assert evaluation.to_dict()["gate"] == gate.to_dict()
    assert [
        (item.metric, item.observed, item.limit, item.status) for item in evaluation.checks
    ] == [
        ("evaluated", 4, 4, "pass"),
        ("alert_added", 3, 3, "pass"),
        ("became_unavailable", 0, 0, "pass"),
        ("alert_to_unavailable", 0, 0, "pass"),
        ("status_changed", 3, 3, "pass"),
    ]
    assert evaluation.checks[0].operator == "at_least"
    assert all(check.operator == "at_most" for check in evaluation.checks[1:])


def test_added_alert_budget_fails_without_mutating_comparison(history_root: Path) -> None:
    report = compare_ons_pay_alert_policies(
        data_root=history_root, baseline=_baseline(), candidate=_candidate()
    )
    before = report.to_dict()
    evaluation = evaluate_alert_comparison_gate(report=report, gate=_gate(max_alert_added=2))
    assert evaluation.status == "fail"
    assert len([check for check in evaluation.checks if check.status == "fail"]) == 1
    assert evaluation.checks[1].metric == "alert_added"
    assert evaluation.checks[1].observed == 3
    assert evaluation.checks[1].limit == 2
    assert report.to_dict() == before


def test_alert_to_unavailable_is_a_separate_gate_failure(history_root: Path) -> None:
    baseline = AlertPolicy(name="long-drift", last=50, max_missing_pay_increase_pp=10)
    candidate = AlertPolicy(name="short-drift", last=1, max_missing_pay_increase_pp=10)
    report = compare_ons_pay_alert_policies(
        data_root=history_root, baseline=baseline, candidate=candidate, replay_last=1
    )
    gate = AlertComparisonGate(name="block-lost-alerts", max_alert_to_unavailable=0)
    evaluation = evaluate_alert_comparison_gate(report=report, gate=gate)
    assert report.summary.alert_cleared == 0
    assert report.summary.alert_to_unavailable == 1
    assert evaluation.status == "fail"
    assert [(check.metric, check.status) for check in evaluation.checks] == [
        ("evaluated", "pass"),
        ("alert_to_unavailable", "fail"),
    ]


def test_missing_observations_never_satisfy_a_gate(tmp_path: Path, history_root: Path) -> None:
    empty = compare_ons_pay_alert_policies(
        data_root=tmp_path / "absent", baseline=_baseline(), candidate=_candidate()
    )
    zero_evidence = evaluate_alert_comparison_gate(
        report=empty, gate=AlertComparisonGate(name="zero-alert-budget", max_alert_added=0)
    )
    assert zero_evidence.status == "fail"
    assert zero_evidence.checks[0].metric == "evaluated"
    assert zero_evidence.checks[0].status == "fail"
    assert zero_evidence.checks[1].status == "pass"

    limited = compare_ons_pay_alert_policies(
        data_root=history_root,
        baseline=_baseline(),
        candidate=_candidate(),
        replay_last=1,
    )
    too_short = evaluate_alert_comparison_gate(report=limited, gate=_gate())
    assert limited.total_records == 4
    assert limited.summary.evaluated == 1
    assert too_short.checks[0].observed == 1
    assert too_short.checks[0].limit == 4
    assert too_short.status == "fail"
    assert not (tmp_path / "absent").exists()


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("schema_version", True, "schema_version"),
        ("schema_version", 2, "schema_version"),
        ("name", "UpperCase", "name"),
        ("name", "", "name"),
        ("min_evaluated", True, "min_evaluated"),
        ("min_evaluated", 0, "min_evaluated"),
        ("min_evaluated", 1001, "min_evaluated"),
        ("max_alert_added", True, "max_alert_added"),
        ("max_alert_added", -1, "max_alert_added"),
        ("max_became_unavailable", 1001, "max_became_unavailable"),
        ("max_alert_to_unavailable", 1.5, "max_alert_to_unavailable"),
        ("max_status_changed", "3", "max_status_changed"),
    ],
)
def test_gate_rejects_invalid_types_and_bounds(field: str, value: object, error: str) -> None:
    configuration: dict[str, object] = _gate().to_dict()
    configuration[field] = value
    with pytest.raises(AlertComparisonGateError, match=error):
        AlertComparisonGate(**configuration)  # type: ignore[arg-type]


def test_gate_requires_a_meaningful_constraint() -> None:
    with pytest.raises(AlertComparisonGateError, match="configure a maximum"):
        AlertComparisonGate(name="empty")
    with pytest.raises(AlertComparisonGateError, match="configure a maximum"):
        AlertComparisonGate(name="all-disabled", max_alert_added=None)
    assert AlertComparisonGate(name="coverage-only", min_evaluated=2).min_evaluated == 2


def test_gate_fingerprint_canonical_and_loader_strict(tmp_path: Path) -> None:
    gate = _gate()
    first = tmp_path / "gate-one.json"
    second = tmp_path / "gate-two.json"
    first.write_text(json.dumps(gate.to_dict(), indent=4), encoding="utf-8")
    second.write_text(json.dumps(gate.to_dict(), sort_keys=True), encoding="utf-8")
    loaded = load_alert_comparison_gate(first)
    assert loaded == load_alert_comparison_gate(second) == gate
    assert loaded.sha256 == gate.sha256
    assert len(loaded.sha256) == 64


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ('{"schema_version":1,"name":"valid","name":"duplicate"}', "duplicate"),
        ('{"schema_version":1,"name":"valid","max_alert_added":NaN}', "non-finite"),
        ('{"schema_version":1,"name":"valid","max_alert_added":Infinity}', "non-finite"),
        ('{"schema_version":1,"name":"valid","max_alert_added":false}', "max_alert_added"),
        ('{"schema_version":1,"name":"valid","unexpected":5}', "unknown"),
        ('{"schema_version":1,"max_alert_added":0}', "missing"),
        ("[1,2,3]", "JSON object"),
        ("{invalid}", "valid UTF-8 JSON"),
    ],
)
def test_invalid_gate_files_are_rejected(tmp_path: Path, payload: str, error: str) -> None:
    path = tmp_path / "bad-gate.json"
    path.write_text(payload, encoding="utf-8")
    with pytest.raises(AlertComparisonGateError, match=error):
        load_alert_comparison_gate(path)


def test_gate_loader_rejects_symlinks_large_files_and_missing_paths(tmp_path: Path) -> None:
    real = tmp_path / "review.json"
    real.write_text(json.dumps(_gate().to_dict()), encoding="utf-8")
    link = tmp_path / "symlink.json"
    link.symlink_to(real)
    with pytest.raises(AlertComparisonGateError, match="symlink"):
        load_alert_comparison_gate(link)
    large = tmp_path / "too-large.json"
    large.write_text(" " * 65_537, encoding="utf-8")
    with pytest.raises(AlertComparisonGateError, match="64 KiB"):
        load_alert_comparison_gate(large)
    with pytest.raises(AlertComparisonGateError, match="cannot read"):
        load_alert_comparison_gate(tmp_path / "missing.json")


def test_cli_gate_pass_fail_and_csv_keep_archive_read_only(
    history_root: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    passing = _gate()
    baseline, candidate, gate_path = _write_configs(tmp_path, passing)
    command = _comparison_command(history_root, baseline, candidate, gate_path)
    store = ReleaseStore(history_root)
    archive = ReleaseHealthArchive(history_root)
    head = store.head.read_bytes()
    events = {path.name: path.read_bytes() for path in store.events.glob("*.json")}
    assessments = {path.name: path.read_bytes() for path in archive.records.glob("*.json")}

    # Omitting the opt-in flag must preserve the pre-existing JSON contract.
    gate_offset = command.index("--gate-file")
    no_gate = [*command[:gate_offset], *command[gate_offset + 2 :]]
    platform_main(no_gate)
    assert "acceptance_gate" not in json.loads(capsys.readouterr().out)

    platform_main(command)
    response = json.loads(capsys.readouterr().out)
    assert response["summary"]["alert_added"] == 3
    assert response["acceptance_gate"]["status"] == "pass"
    assert response["acceptance_gate"]["gate_sha256"] == passing.sha256
    platform_main([*command, "--format", "csv"])
    rows = list(csv.DictReader(io.StringIO(capsys.readouterr().out)))
    assert len(rows) == 4
    assert "acceptance_gate" not in rows[0]

    failing = _gate(max_alert_added=2)
    gate_path.write_text(json.dumps(failing.to_dict()), encoding="utf-8")
    with pytest.raises(SystemExit) as rejected:
        platform_main(command)
    assert rejected.value.code == 1
    report = json.loads(capsys.readouterr().out)
    assert report["acceptance_gate"]["status"] == "fail"
    assert report["acceptance_gate"]["checks"][1]["observed"] == 3
    with pytest.raises(SystemExit) as rejected_csv:
        platform_main([*command, "--format", "csv"])
    assert rejected_csv.value.code == 1
    assert len(list(csv.DictReader(io.StringIO(capsys.readouterr().out)))) == 4

    assert head == store.head.read_bytes()
    assert events == {path.name: path.read_bytes() for path in store.events.glob("*.json")}
    assert assessments == {path.name: path.read_bytes() for path in archive.records.glob("*.json")}


def test_validate_gate_cli_requires_no_release(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "uncreated-release-root"
    path = tmp_path / "gate.json"
    path.write_text(json.dumps(_gate().to_dict()), encoding="utf-8")
    platform_main(
        [
            "ons-pay-release",
            "--data-root",
            str(root),
            "validate-alert-comparison-gate",
            "--gate-file",
            str(path),
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["gate_sha256"] == _gate().sha256
    assert result["gate"]["min_evaluated"] == 4
    assert not root.exists()


def test_cli_empty_history_cannot_pass_and_invalid_gate_is_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline, candidate, gate_path = _write_configs(tmp_path, _gate())
    absent = tmp_path / "absent-release"
    command = _comparison_command(absent, baseline, candidate, gate_path)
    with pytest.raises(SystemExit) as empty:
        platform_main(command)
    assert empty.value.code == 1
    result = json.loads(capsys.readouterr().out)
    assert result["summary"]["evaluated"] == 0
    assert result["acceptance_gate"]["checks"][0]["status"] == "fail"

    gate_path.write_text('{"schema_version":1,"name":"duplicate","name":"oops"}')
    for invocation in (
        command,
        [
            "ons-pay-release",
            "validate-alert-comparison-gate",
            "--gate-file",
            str(gate_path),
        ],
    ):
        with pytest.raises(SystemExit) as error:
            platform_main(invocation)
        assert error.value.code == 2
        assert "duplicate" in capsys.readouterr().err
    assert not absent.exists()
