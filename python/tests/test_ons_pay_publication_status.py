"""Archive-only publication alerts, exact selection and freshness boundaries."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import asdict
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from test_ons_pay_publish_baseline import _baseline
from test_ons_pay_publish_cli import SECRET, _snapshot

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability import ons_pay_publication_status_cli as status_cli
from data_platform_lab.observability.ons_pay_publication_status import (
    PublicationStatusError,
    publication_status,
)
from data_platform_lab.serving import publish_cli, publish_evidence
from data_platform_lab.serving.publish_evidence import PublicationArchive, PublicationEvidenceError

NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)
VINTAGE = {"model_version": 1, "registry_commit": "a" * 40, "source_sha256": "b" * 64}


@pytest.fixture
def archived(tmp_path: Path) -> tuple[Any, ...]:
    """Seed production acceptance bytes, independently of the status evaluator."""
    archive, plan, evidence, *_ = _baseline(tmp_path)
    return archive, plan, evidence


def _record(
    archived: Any,
    *,
    outcome: str = "accepted",
    ended_at: datetime = NOW - timedelta(hours=1),
    **changes: Any,
) -> str:
    """Append a schema-valid historical fixture without relying on POSIX writers."""
    archive, _, evidence = archived
    raw = evidence.to_dict()
    raw.update(
        run_id=str(uuid4()),
        created_at=ended_at.isoformat(),
        ended_at=ended_at.isoformat(),
        started_at=(ended_at - timedelta(seconds=1)).isoformat(),
        duration_seconds=1.0,
        outcome=outcome,
        status="success" if outcome == "accepted" else "failed",
    )
    if outcome != "accepted":
        raw["acceptance"] = None
        raw["failure_code"] = "database_error" if outcome == "failed" else "acceptance_rejected"
        if outcome == "rejected":
            raw["reconciliation"].update(
                passed=False, changed_record_count=1, changed_field_count=1
            )
            raw["fingerprints"]["observed_sink_sha256"] = "f" * 64
    for name, value in changes.items():
        if name == "target_id":
            if value is None:
                raw.pop(name, None)
                raw["schema_version"] = 1
            else:
                raw[name] = value
        else:
            raw[name] = value
            for embedded in (raw["receipt"], raw["acceptance"]):
                if embedded is not None:
                    embedded[name] = value
    raw["source"] = f"sha256:{raw['vintage']['source_sha256']}"
    raw["output"] = f"{raw['sink']}:ons_pay_{raw['table']}"
    payload = publish_evidence._canonical(raw)
    evidence = publish_evidence.PublicationEvidence(payload)
    evidence.to_dict()  # Ensure this fixture can pass archive validation on its own.
    (archive.root / f"{evidence.record_sha256}.json").write_bytes(payload)
    return raw["run_id"]


def _status(archived: Any, **changes: Any) -> Any:
    archive, plan, _ = archived
    return publication_status(
        **(
            {
                "data_root": archive.data_root,
                "sink": "mongodb",
                "table": plan.table,
                "vintage": asdict(plan.vintage),
                "target_id": "local-target",
                "checked_at": NOW,
            }
            | changes
        )
    )


def _args(root: Path, vintage: dict[str, Any] = VINTAGE) -> list[str]:
    return [
        "--data-root",
        str(root),
        "--model-version",
        "1",
        "--registry-commit",
        vintage["registry_commit"],
        "--source-sha256",
        vintage["source_sha256"],
        "--sink",
        "mongodb",
        "--table",
        "local_authority_yoy",
    ]


def _invoke(capsys: Any, args: list[str], *, exit_code: int = 0) -> dict[str, Any]:
    if exit_code:
        with pytest.raises(SystemExit) as error:
            platform_main(["ons-pay-publication-status", *args])
        assert error.value.code == exit_code
    else:
        platform_main(["ons-pay-publication-status", *args])
    captured = capsys.readouterr()
    assert not captured.err and SECRET not in captured.out
    return json.loads(captured.out)


@pytest.mark.parametrize("outcome", ["failed", "rejected"])
def test_latest_failure_is_not_hidden_by_older_acceptance_and_recovery_is_explicit(
    archived: Any, outcome: str
) -> None:
    failed = _record(archived, outcome=outcome)
    report = _status(archived)
    assert report.status == "alert" and report.reason == f"latest_attempt_{outcome}"
    assert report.latest_attempt.attempt_id == failed
    assert report.counts["accepted"] == report.counts[outcome] == 1
    before = _snapshot(archived[0].root)
    recovered = _record(archived, ended_at=NOW)
    report = _status(archived)
    assert report.status == "ok" and report.latest_attempt.attempt_id == recovered
    assert report.counts["accepted"] == 2 and report.counts[outcome] == 1
    after = _snapshot(archived[0].root)
    assert all(after[name] == content for name, content in before.items())


@pytest.mark.parametrize("age, expected", [(0, "ok"), (1, "ok"), (1 + 1 / 3600, "alert")])
def test_freshness_is_inclusive_at_the_threshold(archived: Any, age: float, expected: str) -> None:
    selected = _record(archived, ended_at=NOW - timedelta(hours=age))
    report = _status(archived, max_age_hours=1)
    assert report.status == expected
    assert report.reason == ("stale" if expected == "alert" else "latest_attempt_accepted")
    assert report.latest_attempt.attempt_id == selected
    assert report.latest_attempt.age_hours == pytest.approx(age)
    assert _status(archived).status == "ok"


def test_future_time_and_tied_latest_attempts_are_unavailable(archived: Any) -> None:
    _record(archived, ended_at=NOW + timedelta(seconds=1))
    report = _status(archived)
    assert report.status == "unavailable" and report.reason == "future_timestamp"
    assert report.latest_attempt.age_hours < 0
    _record(archived, outcome="failed", ended_at=NOW + timedelta(seconds=1))
    report = _status(archived, checked_at=NOW + timedelta(hours=1))
    assert report.status == "unavailable" and report.reason == "ambiguous_latest_attempt"
    assert report.latest_attempt is None and report.latest_attempt_count == 2


@pytest.mark.parametrize("selection", ["sink", "table", "vintage", "target_id"])
def test_other_publications_cannot_hide_the_selected_failure(archived: Any, selection: str) -> None:
    failed = _record(archived, outcome="failed", ended_at=NOW - timedelta(hours=2))
    change = {
        "sink": "postgresql",
        "table": "geography_year",
        "vintage": VINTAGE,
        "target_id": "other-target",
    }[selection]
    _record(archived, **{selection: change})
    report = _status(archived)
    assert report.status == "alert" and report.latest_attempt.attempt_id == failed
    assert report.counts == {"accepted": 1, "rejected": 0, "failed": 1}


def test_unlabeled_selection_never_combines_labeled_targets(archived: Any) -> None:
    unlabeled = _record(archived, outcome="failed", target_id=None)
    assert _status(archived).status == "ok"
    report = _status(archived, target_id=None)
    assert report.status == "alert" and report.latest_attempt.attempt_id == unlabeled
    assert report.counts == {"accepted": 0, "rejected": 0, "failed": 1}


def test_full_history_is_selected_before_any_recent_window(archived: Any) -> None:
    failed = _record(archived, outcome="failed", ended_at=NOW - timedelta(hours=2))
    for _ in range(51):
        _record(archived, target_id="other-target")
    report = _status(archived)
    assert report.latest_attempt.attempt_id == failed and report.status == "alert"
    assert sum(report.counts.values()) == 2


def test_status_needs_no_source_clients_locks_or_writes(archived: Any, capsys: Any) -> None:
    archive, plan, _ = archived
    for path in archive.data_root.rglob("*.parquet"):
        path.unlink()
    before = _snapshot(archive.data_root)
    with (
        patch.object(PublicationArchive, "_locked", side_effect=AssertionError),
        patch.object(publish_cli, "_open_sink", side_effect=AssertionError),
        patch.object(publish_cli, "_source_plan", side_effect=AssertionError),
    ):
        result = _invoke(
            capsys, [*_args(archive.data_root, asdict(plan.vintage)), "--target-id", "local-target"]
        )
    assert result["status"] == "ok" and result["operation"] == "publication_status"
    assert result["latest_attempt"]["outcome"] == "accepted"
    assert "acceptance" not in result and _snapshot(archive.data_root) == before


def test_empty_archive_is_unavailable_and_stays_absent(tmp_path: Path, capsys: Any) -> None:
    root = tmp_path / "absent"
    report = _invoke(capsys, _args(root), exit_code=1)
    assert report["status"] == "unavailable" and report["reason"] == "no_attempts"
    assert report["latest_attempt"] is None and report["latest_attempt_count"] == 0
    assert report["counts"] == {"accepted": 0, "rejected": 0, "failed": 0}
    assert not root.exists()


@pytest.mark.parametrize("state", ["ok", "failed", "rejected", "stale", "future", "tied"])
def test_cli_status_exit_codes_follow_real_history(archived: Any, capsys: Any, state: str) -> None:
    archive, plan, _ = archived
    ended = (
        NOW + timedelta(seconds=1)
        if state == "future"
        else NOW - timedelta(hours=2)
        if state == "stale"
        else NOW
    )
    _record(
        archived, outcome=state if state in ("failed", "rejected") else "accepted", ended_at=ended
    )
    if state == "tied":
        _record(archived, ended_at=ended)
    with patch.object(
        status_cli,
        "publication_status",
        side_effect=lambda **kwargs: publication_status(**kwargs, checked_at=NOW),
    ):
        result = _invoke(
            capsys,
            [
                *_args(archive.data_root, asdict(plan.vintage)),
                "--target-id",
                "local-target",
                "--max-age-hours",
                "1",
            ],
            exit_code=0 if state == "ok" else 1,
        )
    assert result["status"] == (
        "ok" if state == "ok" else "unavailable" if state in ("future", "tied") else "alert"
    )


@pytest.mark.parametrize("mutation", ["bytes", "schema", "duplicate", "cap"])
def test_unrelated_corruption_or_archive_limits_cannot_be_filtered_out(
    archived: Any, capsys: Any, mutation: str
) -> None:
    archive, plan, evidence = archived
    _record(archived, target_id="other-target")
    if mutation in ("bytes", "schema"):
        payload = (
            b"bad json with secret"
            if mutation == "bytes"
            else publish_evidence._canonical(evidence.to_dict() | {"status": "failed"})
        )
        (archive.root / f"{hashlib.sha256(payload).hexdigest()}.json").write_bytes(payload)
    elif mutation == "duplicate":
        payload = publish_evidence._canonical(evidence.to_dict() | {"target_id": "other-target"})
        (archive.root / f"{hashlib.sha256(payload).hexdigest()}.json").write_bytes(payload)
    with patch.object(publish_evidence, "_MAX_RECORDS", 1 if mutation == "cap" else 10_000):
        with pytest.raises(PublicationEvidenceError):
            _status(archived)
        result = _invoke(
            capsys,
            [*_args(archive.data_root, asdict(plan.vintage)), "--target-id", "local-target"],
            exit_code=5,
        )
    assert result["error"]["code"] == "evidence_error" and "status" not in result


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, "1", 10**1000])
def test_invalid_age_is_rejected_before_archive_io(tmp_path: Path, value: Any) -> None:
    with (
        patch.object(PublicationArchive, "history", side_effect=AssertionError),
        pytest.raises(PublicationStatusError),
    ):
        publication_status(
            data_root=tmp_path,
            sink="mongodb",
            table="local_authority_yoy",
            vintage=VINTAGE,
            max_age_hours=value,
        )


@pytest.mark.parametrize("time", [datetime(2026, 11, 9), "2026-11-09", True])
def test_invalid_reference_time_is_rejected_before_archive_io(tmp_path: Path, time: Any) -> None:
    with (
        patch.object(PublicationArchive, "history", side_effect=AssertionError),
        pytest.raises(PublicationStatusError),
    ):
        publication_status(
            data_root=tmp_path,
            sink="mongodb",
            table="local_authority_yoy",
            vintage=VINTAGE,
            checked_at=time,
        )


def test_aware_time_is_normalized_and_report_is_a_defensive_copy(archived: Any) -> None:
    report = _status(archived, checked_at=NOW.astimezone(timezone(timedelta(hours=2))))
    assert report.checked_at_utc == NOW.isoformat()
    raw = report.to_dict()
    raw["counts"]["accepted"] = 100
    raw["vintage"]["registry_commit"] = "f" * 40
    assert report.to_dict()["counts"]["accepted"] == 1
    assert report.to_dict()["vintage"]["registry_commit"] != "f" * 40


@pytest.mark.parametrize(
    "flag, value",
    [
        ("--max-age-hours", "nan"),
        ("--max-age-hours", "0"),
        ("--registry-commit", SECRET),
        ("--source-sha256", SECRET),
        ("--model-version", "2"),
        ("--sink", SECRET),
        ("--table", SECRET),
        ("--target-id", SECRET),
    ],
)
def test_cli_invalid_selection_is_sanitized_before_io(
    tmp_path: Path, capsys: Any, flag: str, value: str
) -> None:
    with patch.object(PublicationArchive, "history", side_effect=AssertionError):
        result = _invoke(capsys, [*_args(tmp_path), flag, value], exit_code=2)
    assert result["error"]["code"] == "invalid_arguments"
    assert not list(tmp_path.iterdir())


def test_command_runs_without_optional_analytics_or_database_clients(tmp_path: Path) -> None:
    code = (
        "import sys; "
        "sys.modules.update({name: None for name in ('duckdb', 'psycopg', 'pymongo', 'fastapi')}); "
        "from data_platform_lab.cli.main import main; main(sys.argv[1:])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, "ons-pay-publication-status", *_args(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1 and not result.stderr
    assert json.loads(result.stdout)["reason"] == "no_attempts"
