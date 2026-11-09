"""Fresh, read-only verification linked to immutable accepted publication evidence."""

from __future__ import annotations

import copy
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from test_ons_pay_publish_cli import SECRET, CliMongo, CliPostgres, _args, _invoke, _snapshot
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.serving import publish_cli, publish_evidence, publish_sinks
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_evidence import PublicationArchive, PublicationEvidenceError
from data_platform_lab.serving.publish_plan import _TABLES, plan_ons_pay_publishing
from data_platform_lab.serving.publish_reconcile import reconcile_mongodb, reconcile_postgres

ATTEMPT = "12345678-1234-4234-8234-123456789abc"


def _baseline(
    tmp_path: Path,
    *,
    sink: str = "mongodb",
    table: Any = "local_authority_yoy",
    target_id: str | None = "local-target",
) -> tuple[Any, ...]:
    """Build real gate evidence; seed its file directly to test readers on any OS."""
    root, curated = _curate(tmp_path)
    pins = _pin(curated)
    vintage = VintageKey(1, pins["registry_commit"], pins["source_sha256"])
    plan = plan_ons_pay_publishing(
        data_root=root,
        vintage=vintage,
        table=table,
        shape="relational" if sink == "postgresql" else "document",
    )
    handle: Any = CliPostgres(table) if sink == "postgresql" else CliMongo()
    receipt = (
        publish_sinks.publish_postgres(plan, handle)
        if sink == "postgresql"
        else publish_sinks.publish_mongodb(plan, handle)
    )
    report = (
        reconcile_postgres(plan, handle)
        if sink == "postgresql"
        else reconcile_mongodb(plan, handle)
    )
    started = datetime(2026, 11, 9, tzinfo=UTC)
    evidence = publish_evidence._build(
        plan,
        sink=sink,
        attempt_id=ATTEMPT,
        started_at=started,
        ended_at=started + timedelta(seconds=1),
        receipt=receipt,
        report=report,
        failure_code=None,
        target_id=target_id,
    )
    archive = PublicationArchive(root)
    archive.root.mkdir(parents=True)
    (archive.root / f"{evidence.record_sha256}.json").write_bytes(evidence.payload)
    args = [*_args((root, vintage), sink, table), "--verify", "--baseline-attempt-id", ATTEMPT]
    if target_id is not None:
        args.extend(["--target-id", target_id])
    kwargs = {"postgres_connection" if sink == "postgresql" else "mongo_collection": handle}
    return archive, plan, evidence, handle, args, kwargs


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
@pytest.mark.parametrize("target_id", [None, "local-target"])
def test_baseline_verification_reads_live_rows_without_writes_or_new_acceptance(
    tmp_path: Path, capsys: Any, sink: str, table: str, target_id: str | None
) -> None:
    archive, plan, evidence, handle, args, kwargs = _baseline(
        tmp_path, sink=sink, table=table, target_id=target_id
    )
    before = _snapshot(archive.data_root)
    state = copy.deepcopy(handle.persisted if sink == "postgresql" else handle.documents)
    writes = handle.commits if sink == "postgresql" else len(handle.calls)
    reads = handle.reads
    with (
        patch.object(PublicationArchive, "_locked", side_effect=AssertionError),
        patch.object(publish_sinks, "publish_postgres", side_effect=AssertionError),
        patch.object(publish_sinks, "publish_mongodb", side_effect=AssertionError),
    ):
        result = _invoke(capsys, args, **kwargs)
    assert result["status"] == "verified" and result["operation"] == "verify"
    assert result["acceptance"] is None and "evidence" not in result
    assert result["counts"] == {
        "source": plan.source_rows,
        "applied": None,
        "observed": plan.source_rows,
    }
    assert result["baseline"] == {
        "attempt_id": ATTEMPT,
        "record_sha256": evidence.record_sha256,
        "accepted_at": evidence.to_dict()["ended_at"],
    }
    assert handle.reads == reads + 1
    assert (handle.commits if sink == "postgresql" else len(handle.calls)) == writes
    assert (handle.persisted if sink == "postgresql" else handle.documents) == state
    assert _snapshot(archive.data_root) == before


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
@pytest.mark.parametrize("drift", ["missing", "changed", "unexpected"])
def test_baseline_detects_current_drift_without_replaying_historical_acceptance(
    tmp_path: Path, capsys: Any, sink: str, drift: str
) -> None:
    archive, _, evidence, handle, args, kwargs = _baseline(tmp_path, sink=sink)
    rows = handle.persisted if sink == "postgresql" else handle.documents
    key = next(iter(rows))
    if drift == "missing":
        del rows[key]
    elif sink == "postgresql":
        changed = list(rows[key])
        if drift == "changed":
            columns = ("record_id", "model_version", *_TABLES["local_authority_yoy"].columns)
            changed[columns.index("area_name")] = "Changed area"
            rows[key] = tuple(changed)
        else:
            changed[0] = "f" * 64
            rows["f" * 64] = tuple(changed)
    elif drift == "changed":
        rows[key]["data"]["area_name"] = "Changed area"
    else:
        extra = copy.deepcopy(rows[key])
        extra["_id"] = "f" * 64
        rows["f" * 64] = extra
    state = copy.deepcopy(rows)
    before = _snapshot(archive.data_root)
    result = _invoke(capsys, args, expected_exit=3, **kwargs)
    assert result["error"]["code"] == "reconciliation_failed"
    assert result["status"] == "rejected" and result["acceptance"] is None
    assert result["baseline"]["record_sha256"] == evidence.record_sha256
    assert not result["reconciliation"]["passed"]
    assert rows == state and _snapshot(archive.data_root) == before
    assert archive.get_attempt(ATTEMPT).to_dict()["outcome"] == "accepted"


@pytest.mark.parametrize(
    "failure",
    [
        "missing",
        "corrupt",
        "failed",
        "rejected",
        "target",
        "omitted_target",
        "extra_target",
        "sink",
        "table",
        "vintage",
        "count",
        "source_hash",
        "plan_hash",
        "snapshot_hash",
    ],
)
def test_invalid_baseline_fails_before_connecting_and_preserves_the_archive(
    tmp_path: Path, capsys: Any, failure: str
) -> None:
    archive, _, evidence, _, args, _ = _baseline(
        tmp_path, target_id=None if failure == "extra_target" else "local-target"
    )
    path = archive.root / f"{evidence.record_sha256}.json"
    if failure == "missing":
        path.unlink()
        archive.root.rmdir()
    elif failure == "corrupt":
        path.write_bytes(b"corrupted")
    elif failure in ("target", "extra_target", "sink", "table"):
        flag, value = {
            "target": ("--target-id", "other-target"),
            "extra_target": ("--target-id", "other-target"),
            "sink": ("--sink", "postgresql"),
            "table": ("--table", "geography_year"),
        }[failure]
        args.extend([flag, value])
    elif failure == "omitted_target":
        args = args[:-2]
    else:
        raw = evidence.to_dict()
        if failure in ("failed", "rejected"):
            raw.update(status="failed", outcome=failure, acceptance=None)
            raw["failure_code"] = "database_error" if failure == "failed" else "acceptance_rejected"
            if failure == "rejected":
                raw["reconciliation"].update(
                    passed=False, changed_record_count=1, changed_field_count=1
                )
                raw["fingerprints"]["observed_sink_sha256"] = "f" * 64
        elif failure == "source_hash":
            raw["fingerprints"]["source_artifact_sha256"] = "f" * 64
        elif failure == "plan_hash":
            raw["fingerprints"]["source_plan_sha256"] = "f" * 64
            raw["receipt"]["records_sha256"] = "f" * 64
            raw["acceptance"]["source_plan_sha256"] = "f" * 64
        elif failure == "vintage":
            for value in (raw, raw["receipt"], raw["acceptance"]):
                value["vintage"]["registry_commit"] = "f" * 40
        elif failure == "count":
            raw["counts"] = {"source": 7, "applied": 7, "observed": 7}
            raw["row_count"] = raw["receipt"]["records_applied"] = 7
            raw["acceptance"]["records_accepted"] = 7
        else:
            raw["fingerprints"]["expected_sink_sha256"] = "f" * 64
            raw["fingerprints"]["observed_sink_sha256"] = "f" * 64
            raw["acceptance"]["sink_snapshot_sha256"] = "f" * 64
        payload = publish_evidence._canonical(raw)
        path.unlink()
        (archive.root / f"{hashlib.sha256(payload).hexdigest()}.json").write_bytes(payload)
        # These are structurally valid records; matching them to this source must fail.
        assert archive.get_attempt(ATTEMPT) is not None
    before = _snapshot(archive.data_root)
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        result = _invoke(capsys, args, expected_exit=5, environ={})
    assert result["error"]["code"] == "evidence_error"
    assert result["acceptance"] is None and "baseline" not in result
    assert _snapshot(archive.data_root) == before
    if failure == "missing":
        assert not archive.root.exists()


@pytest.mark.parametrize(
    "extra",
    [
        ["--baseline-attempt-id", ATTEMPT],
        ["--apply", "--baseline-attempt-id", ATTEMPT],
        ["--verify", "--baseline-attempt-id", SECRET],
        ["--verify", "--baseline-attempt-id", ""],
        ["--verify", "--baseline-attempt-id", ATTEMPT, "--record-evidence"],
        ["--verify", "--baseline-attempt-id", ATTEMPT, "--attempt-id", ATTEMPT],
    ],
)
def test_invalid_baseline_options_do_not_read_source_or_archive(
    tmp_path: Path, capsys: Any, extra: list[str]
) -> None:
    args = _args((tmp_path, VintageKey(1, "a" * 40, "b" * 64)))
    with (
        patch.object(publish_cli, "_source_plan", side_effect=AssertionError),
        patch.object(PublicationArchive, "get_attempt", side_effect=AssertionError),
    ):
        result = _invoke(capsys, [*args, *extra], expected_exit=2)
    assert result["error"]["code"] == "invalid_arguments"
    assert not list(tmp_path.iterdir())


def test_baseline_still_validates_unrelated_archive_records(tmp_path: Path, capsys: Any) -> None:
    archive, _, _, _, args, _ = _baseline(tmp_path)
    (archive.root / f"{'f' * 64}.json").write_bytes(b"corrupted unrelated attempt")
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        _invoke(capsys, args, expected_exit=5)


@pytest.mark.parametrize("stage", ["before_connect", "after_connect"])
def test_baseline_cannot_replace_missing_or_changed_source_artifacts(
    tmp_path: Path, capsys: Any, stage: str
) -> None:
    archive, _, _, handle, args, kwargs = _baseline(tmp_path)
    before = _snapshot(archive.root)
    reads = handle.reads

    def corrupt_source() -> Any:
        for path in archive.data_root.rglob("*.parquet"):
            path.write_bytes(b"modified source")
        return handle

    with patch.object(publish_cli, "_open_sink") as opened:
        if stage == "before_connect":
            corrupt_source()
            opened.side_effect = AssertionError("invalid source reached a sink")
        else:
            opened.return_value.__enter__.side_effect = corrupt_source
        result = _invoke(capsys, args, expected_exit=2, **kwargs)
    assert result["error"]["code"] == "source_validation_failed"
    assert result["acceptance"] is None and handle.reads == reads
    assert _snapshot(archive.root) == before


def test_baseline_api_is_read_only_and_rejects_invalid_labels(tmp_path: Path) -> None:
    archive, plan, evidence, _, _, _ = _baseline(tmp_path)
    before = _snapshot(archive.data_root)
    with patch.object(PublicationArchive, "_locked", side_effect=AssertionError):
        assert (
            archive.accepted_baseline(
                plan, sink="mongodb", attempt_id=ATTEMPT, target_id="local-target"
            )
            == evidence
        )
        with pytest.raises(PublicationEvidenceError):
            archive.accepted_baseline(
                plan, sink="mongodb", attempt_id=str(uuid4()), target_id="local-target"
            )
        with pytest.raises(PublicationEvidenceError):
            archive.accepted_baseline(plan, sink="mongodb", attempt_id=ATTEMPT, target_id=SECRET)
    assert _snapshot(archive.data_root) == before


@pytest.mark.parametrize("failure", ["read", "connection_close"])
def test_baseline_database_errors_never_claim_verification_or_acceptance(
    tmp_path: Path, capsys: Any, failure: str
) -> None:
    archive, _, evidence, connection, args, _ = _baseline(tmp_path, sink="postgresql")
    connection.fail_on = failure
    before = _snapshot(archive.data_root)
    with patch.object(
        publish_cli.importlib,
        "import_module",
        return_value=SimpleNamespace(connect=lambda *a, **kw: connection),
    ):
        result = _invoke(capsys, args, expected_exit=4, environ={"ONS_PAY_POSTGRES_DSN": SECRET})
    assert result["status"] == "failed" and result["acceptance"] is None
    assert result["baseline"]["record_sha256"] == evidence.record_sha256
    assert connection.read_only and connection.closed
    assert _snapshot(archive.data_root) == before
