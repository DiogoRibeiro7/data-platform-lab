"""Offline, null-safe candidate release comparisons over actual curated Parquet."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import pytest
from test_ons_pay_serving import COMMIT_A, COMMIT_B, PAY_CSV, _curate, _pin

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.ons_pay_compare import (
    ComparisonError,
    ComparisonReport,
    _check_unique_grain,
    compare_ons_pay_vintages,
)


def _candidate_csv() -> str:
    """One addition, deletion, numeric revision, and both missingness transitions."""
    changed = PAY_CSV
    replacements = (
        ("2022-04-01,100,Pounds,£,90,110,,", "2022-04-01,105,Pounds,£,90,110,,"),
        ("2024-04-01,,Pounds,£,,,x,", "2024-04-01,140,Pounds,£,,,,"),
        (
            "A001,Area A,Local Authority District,Gross median weekly pay,2025",
            "A001,Area Alpha,Local Authority District,Gross median weekly pay,2025",
        ),
        ("2022-04-01,200,Pounds,£,,,,", "2022-04-01,,Pounds,£,,,x,"),
        (
            "B001,Area B,Local Authority District,Gross median weekly pay,"
            "2024-04-01,220,Pounds,£,,,,\n",
            "",
        ),
    )
    for before, after in replacements:
        assert changed.count(before) == 1
        changed = changed.replace(before, after)
    return changed + (
        "C001,Area C,Local Authority District,Gross median weekly pay,2025-04-01,330,Pounds,£,,,,\n"
    )


def _key(receipt: dict[str, object]) -> VintageKey:
    pin = _pin(receipt)
    return VintageKey(1, pin["registry_commit"], pin["source_sha256"])


@pytest.fixture
def pair(tmp_path: Path) -> tuple[Path, VintageKey, VintageKey]:
    root, baseline = _curate(tmp_path, commit=COMMIT_A)
    _, candidate = _curate(tmp_path, commit=COMMIT_B, payload=_candidate_csv().encode("utf-8-sig"))
    return root, _key(baseline), _key(candidate)


def _check(report: ComparisonReport, name: str) -> str:
    return next(check.state for check in report.policy_checks if check.name == name)


def test_null_safe_revisions_and_bidirectional_suppression(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, baseline, candidate = pair
    report = compare_ons_pay_vintages(data_root=root, baseline=baseline, candidate=candidate)

    assert report.baseline == baseline
    assert report.candidate == candidate
    assert report.status == "pass"
    assert all(check.state == "skipped" for check in report.policy_checks)
    counts = report.counts
    assert (counts.baseline_rows, counts.candidate_rows) == (7, 7)
    assert (counts.added, counts.removed, counts.revised, counts.unchanged) == (1, 1, 4, 2)
    assert (counts.newly_missing_pay, counts.newly_reported_pay) == (1, 1)
    assert counts.numeric_pay_revisions == 1
    assert counts.max_absolute_pay_revision_gbp == 5
    assert counts.baseline_missing_pay_percent == pytest.approx(100 / 7)
    assert counts.candidate_missing_pay_percent == pytest.approx(100 / 7)
    assert counts.missing_pay_increase_pp == 0
    assert report.changed_fields == {
        "area_name": 1,
        "geography": 0,
        "pay_year": 0,
        "weekly_pay_gbp": 3,
        "lci_95_gbp": 0,
        "uci_95_gbp": 0,
        "status_code": 2,
        "notes": 0,
    }
    assert report.examples["added"][0].area_code == "C001"
    assert report.examples["added"][0].baseline is None
    assert report.examples["removed"][0].area_code == "B001"
    assert report.examples["removed"][0].candidate is None
    revised = report.examples["revised"]
    assert len(revised) == 4
    assert revised[0].area_code == "A001"
    assert revised[0].reference_date == "2022-04-01"
    assert revised[0].changed_fields == ("weekly_pay_gbp",)
    assert revised[0].baseline is not None and revised[0].baseline["weekly_pay_gbp"] == 100
    assert revised[0].candidate is not None and revised[0].candidate["weekly_pay_gbp"] == 105
    assert revised[1].changed_fields == ("weekly_pay_gbp", "status_code")
    assert revised[1].baseline is not None and revised[1].baseline["weekly_pay_gbp"] is None
    assert revised[1].candidate is not None and revised[1].candidate["weekly_pay_gbp"] == 140
    assert revised[2].changed_fields == ("area_name",)
    assert revised[3].changed_fields == ("weekly_pay_gbp", "status_code")
    assert json.loads(json.dumps(report.to_dict()))["counts"]["added"] == 1


def test_reversing_pins_reverses_suppression_transitions(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, baseline, candidate = pair
    reversed_report = compare_ons_pay_vintages(
        data_root=root, baseline=candidate, candidate=baseline, examples_per_change=0
    )
    assert reversed_report.counts.newly_missing_pay == 1
    assert reversed_report.counts.newly_reported_pay == 1
    assert reversed_report.counts.added == 1
    assert reversed_report.counts.removed == 1
    assert all(not examples for examples in reversed_report.examples.values())


def test_same_vintage_and_same_bytes_different_commit_are_identical(tmp_path: Path) -> None:
    root, first = _curate(tmp_path, commit=COMMIT_A)
    _, second = _curate(tmp_path, commit=COMMIT_B)
    baseline = _key(first)
    candidate = _key(second)
    assert baseline.source_sha256 == candidate.source_sha256
    assert baseline.registry_commit != candidate.registry_commit
    for right in (baseline, candidate):
        report = compare_ons_pay_vintages(
            data_root=root,
            baseline=baseline,
            candidate=right,
            max_added_rows=0,
            max_removed_rows=0,
            max_revised_rows=0,
            max_newly_missing_pay_rows=0,
            max_missing_pay_increase_pp=0,
            max_absolute_pay_revision_gbp=0,
        )
        assert all(check.state == "pass" for check in report.policy_checks)
        assert report.counts.unchanged == 7
        assert report.counts.revised == 0
        assert report.counts.added == 0
        assert report.counts.removed == 0
        assert report.counts.numeric_pay_revisions == 0
        assert report.counts.max_absolute_pay_revision_gbp == 0
        assert all(count == 0 for count in report.changed_fields.values())


def test_explicit_release_policy_can_block_and_allow_candidate(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, baseline, candidate = pair
    strict = compare_ons_pay_vintages(
        data_root=root,
        baseline=baseline,
        candidate=candidate,
        max_added_rows=0,
        max_removed_rows=0,
        max_revised_rows=2,
        max_newly_missing_pay_rows=0,
        max_missing_pay_increase_pp=0,
        max_absolute_pay_revision_gbp=4,
    )
    assert strict.status == "breach"
    assert [
        _check(strict, name)
        for name in (
            "added_rows",
            "removed_rows",
            "revised_rows",
            "newly_missing_pay_rows",
            "missing_pay_increase_pp",
            "absolute_pay_revision_gbp",
        )
    ] == ["breach", "breach", "breach", "breach", "pass", "breach"]

    allowed = compare_ons_pay_vintages(
        data_root=root,
        baseline=baseline,
        candidate=candidate,
        max_added_rows=1,
        max_removed_rows=1,
        max_revised_rows=4,
        max_newly_missing_pay_rows=1,
        max_missing_pay_increase_pp=0,
        max_absolute_pay_revision_gbp=5,
    )
    assert allowed.status == "pass"
    assert all(check.state == "pass" for check in allowed.policy_checks)


def test_missing_pay_share_increase_is_a_distinct_gate(tmp_path: Path) -> None:
    root, first = _curate(tmp_path, commit=COMMIT_A)
    updated = PAY_CSV.replace("2022-04-01,200,Pounds,£,,,,", "2022-04-01,,Pounds,£,,,x,")
    _, second = _curate(tmp_path, commit=COMMIT_B, payload=updated.encode("utf-8-sig"))
    report = compare_ons_pay_vintages(
        data_root=root,
        baseline=_key(first),
        candidate=_key(second),
        max_missing_pay_increase_pp=10,
    )
    assert report.counts.newly_missing_pay == 1
    assert report.counts.newly_reported_pay == 0
    assert report.counts.missing_pay_increase_pp == pytest.approx(100 / 7)
    assert _check(report, "missing_pay_increase_pp") == "breach"


def test_configured_numeric_revision_gate_fails_closed_without_overlap(tmp_path: Path) -> None:
    root, first = _curate(tmp_path, commit=COMMIT_A)
    disjoint = PAY_CSV.replace("A001", "C001").replace("B001", "D001").replace("R001", "S001")
    _, second = _curate(tmp_path, commit=COMMIT_B, payload=disjoint.encode("utf-8-sig"))
    report = compare_ons_pay_vintages(
        data_root=root,
        baseline=_key(first),
        candidate=_key(second),
        max_absolute_pay_revision_gbp=100,
    )
    assert report.counts.added == 7
    assert report.counts.removed == 7
    assert report.counts.revised == 0
    assert report.counts.max_absolute_pay_revision_gbp is None
    assert _check(report, "absolute_pay_revision_gbp") == "breach"


def test_corrupt_parquet_is_rejected_before_comparison(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, baseline, candidate = pair
    path = (
        root
        / "silver"
        / "ons-gross-median-weekly-pay"
        / "v1"
        / candidate.registry_commit
        / candidate.source_sha256
        / "observations.parquet"
    )
    path.write_bytes(b"corrupted")
    with pytest.raises(ComparisonError, match="integrity"):
        compare_ons_pay_vintages(data_root=root, baseline=baseline, candidate=candidate)


def test_schema_drift_rejected_even_when_artifact_hash_receipt_is_updated(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    import duckdb

    root, baseline, candidate = pair
    receipt_path = next(
        path
        for path in (root / "manifests").glob("ons_pay_curation_*.json")
        if json.loads(path.read_text(encoding="utf-8"))["input"]["source_sha256"]
        == candidate.source_sha256
    )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    silver = next(item for item in receipt["outputs"] if item["layer"] == "silver")
    artifact = Path(silver["path"])
    rewritten = artifact.with_name("rewritten.parquet")
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            "COPY (SELECT * EXCLUDE(weekly_pay_gbp) FROM read_parquet(?)) TO ? (FORMAT PARQUET)",
            [str(artifact), str(rewritten)],
        )
    finally:
        conn.close()
    rewritten.replace(artifact)
    silver["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(ComparisonError, match="table contract"):
        compare_ons_pay_vintages(data_root=root, baseline=baseline, candidate=candidate)


def test_invalid_pins_limits_and_missing_vintage_are_config_errors(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, baseline, candidate = pair
    for key in (
        VintageKey(2, baseline.registry_commit, baseline.source_sha256),
        VintageKey(True, baseline.registry_commit, baseline.source_sha256),
        VintageKey(1, "not-git", baseline.source_sha256),
        VintageKey(1, baseline.registry_commit, "no-sha"),
    ):
        with pytest.raises(ComparisonError, match="exact v1"):
            compare_ons_pay_vintages(data_root=root, baseline=key, candidate=candidate)
    for kwargs in (
        {"max_added_rows": -1},
        {"max_removed_rows": -1},
        {"max_revised_rows": True},
        {"max_newly_missing_pay_rows": -2},
        {"max_missing_pay_increase_pp": math.nan},
        {"max_missing_pay_increase_pp": math.inf},
        {"max_missing_pay_increase_pp": 101},
        {"max_absolute_pay_revision_gbp": -1},
        {"max_absolute_pay_revision_gbp": math.inf},
        {"examples_per_change": -1},
        {"examples_per_change": True},
        {"examples_per_change": 101},
    ):
        with pytest.raises(ComparisonError):
            compare_ons_pay_vintages(
                data_root=root, baseline=baseline, candidate=candidate, **kwargs
            )
    with pytest.raises(ComparisonError, match="curated exact pins"):
        compare_ons_pay_vintages(
            data_root=root,
            baseline=baseline,
            candidate=VintageKey(1, "f" * 40, candidate.source_sha256),
        )


def test_duplicate_grain_detected_without_trusting_source_receipts() -> None:
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            "CREATE TEMP TABLE baseline AS SELECT 'A001' AS area_code, "
            "DATE '2025-01-01' AS reference_date FROM range(2)"
        )
        with pytest.raises(ComparisonError, match="duplicate"):
            _check_unique_grain(conn, "baseline")
        conn.execute(
            "CREATE TEMP TABLE candidate AS SELECT NULL::VARCHAR AS area_code, "
            "DATE '2025-01-01' AS reference_date"
        )
        with pytest.raises(ComparisonError, match="null"):
            _check_unique_grain(conn, "candidate")
    finally:
        conn.close()


def test_comparison_does_not_mutate_any_published_files(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, baseline, candidate = pair
    before = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}
    compare_ons_pay_vintages(data_root=root, baseline=baseline, candidate=candidate)
    after = {path: path.stat().st_mtime_ns for path in root.rglob("*") if path.is_file()}
    assert before == after


def test_cli_status_json_and_exit_codes(
    pair: tuple[Path, VintageKey, VintageKey], capsys: pytest.CaptureFixture[str]
) -> None:
    root, baseline, candidate = pair
    args = [
        "ons-pay-compare",
        "--data-root",
        str(root),
        "--baseline-registry-commit",
        baseline.registry_commit,
        "--baseline-source-sha256",
        baseline.source_sha256,
        "--candidate-registry-commit",
        candidate.registry_commit,
        "--candidate-source-sha256",
        candidate.source_sha256,
    ]
    platform_main([*args, "--examples-per-change", "1"])
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "pass"
    assert len(report["examples"]["revised"]) == 1
    with pytest.raises(SystemExit) as breach:
        platform_main([*args, "--max-removed-rows", "0"])
    assert breach.value.code == 1
    assert json.loads(capsys.readouterr().out)["status"] == "breach"
    with pytest.raises(SystemExit) as invalid:
        platform_main([*args, "--max-revised-rows", "-1"])
    assert invalid.value.code == 2
