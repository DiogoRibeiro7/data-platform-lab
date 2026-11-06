"""Durable outcomes, integrity, interrupted writes and concurrent publication attempts."""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest
from test_ons_pay_publish_cli import CliMongo, CliPostgres, _args, _invoke, _snapshot
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.manifest import validate_manifest
from data_platform_lab.serving import publish_cli, publish_evidence
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_evidence import PublicationArchive, PublicationEvidenceError
from data_platform_lab.serving.publish_gate import accept_publication as publish_evidence_accept
from data_platform_lab.serving.publish_plan import PublishPlan, plan_ons_pay_publishing
from data_platform_lab.serving.publish_reconcile import reconcile_mongodb
from data_platform_lab.serving.publish_sinks import publish_mongodb
from data_platform_lab.serving.release_store import ReleaseStore

pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="durable recording uses POSIX locks/fsync"
)
ATTEMPT = "12345678-1234-4234-8234-123456789abc"
START = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, VintageKey]:
    """Use real Silver/Gold outputs; never bypass source validation."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    return root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])


@pytest.fixture
def prepared(
    curated: tuple[Path, VintageKey],
) -> tuple[PublicationArchive, PublishPlan, dict[str, Any]]:
    root, vintage = curated
    plan = plan_ons_pay_publishing(
        data_root=root, vintage=vintage, table="local_authority_yoy", shape="document"
    )
    collection = CliMongo()
    receipt = publish_mongodb(plan, collection)
    report = reconcile_mongodb(plan, collection)
    return (
        PublicationArchive(root),
        plan,
        {
            "sink": "mongodb",
            "attempt_id": ATTEMPT,
            "started_at": START,
            "ended_at": START + timedelta(seconds=2),
            "receipt": receipt,
            "report": report,
        },
    )


def _record(prepared: Any, **overrides: Any) -> Any:
    archive, plan, kwargs = prepared
    return archive.record(plan, **(kwargs | overrides))


def test_accepted_manifest_reuses_contract_and_is_content_addressed(prepared: Any) -> None:
    archive, plan, _ = prepared
    assert not archive.root.exists()
    with patch(
        "data_platform_lab.serving.publish_gate.accept_publication", wraps=publish_evidence_accept
    ) as gate:
        evidence = _record(prepared)
    gate.assert_called_once()
    raw = evidence.to_dict()
    assert validate_manifest(raw) == []
    assert raw["status"] == "success" and raw["outcome"] == "accepted"
    assert raw["run_id"] == ATTEMPT
    assert raw["duration_seconds"] == 2.0
    assert raw["counts"] == {"source": 6, "applied": 6, "observed": 6}
    assert raw["acceptance"]["source_plan_sha256"] == plan.records_sha256
    assert raw["acceptance"]["sink_snapshot_sha256"] == raw["fingerprints"]["observed_sink_sha256"]
    path = archive.root / f"{evidence.record_sha256}.json"
    assert path.read_bytes() == evidence.payload
    assert hashlib.sha256(path.read_bytes()).hexdigest() == path.stem
    assert b'"records"' not in evidence.payload
    assert all(str(row["_id"]).encode() not in evidence.payload for row in plan.records)
    raw["outcome"] = "failed"
    assert evidence.to_dict()["outcome"] == "accepted"


def test_passing_reconciliation_without_a_write_receipt_never_creates_acceptance(
    prepared: Any,
) -> None:
    archive, _, _ = prepared
    with pytest.raises(PublicationEvidenceError):
        _record(prepared, receipt=None)
    assert not archive.root.exists()


def test_rejected_and_failed_outcomes_remain_distinct(prepared: Any) -> None:
    archive, plan, kwargs = prepared
    collection = CliMongo()
    receipt = publish_mongodb(plan, collection)
    collection.drift = True
    rejected = _record(prepared, receipt=receipt, report=reconcile_mongodb(plan, collection))
    assert rejected.to_dict()["outcome"] == "rejected"
    assert rejected.to_dict()["reconciliation"]["changed_record_count"] == 1
    assert rejected.to_dict()["acceptance"] is None
    failed = _record(
        prepared, attempt_id=str(uuid4()), receipt=None, report=None, failure_code="database_error"
    )
    assert failed.to_dict()["outcome"] == "failed"
    assert failed.to_dict()["counts"]["applied"] is None
    assert failed.to_dict()["reconciliation"] is failed.to_dict()["acceptance"] is None
    assert len(archive.history()) == 2
    assert kwargs["report"].passed


def test_execution_failure_after_passing_readback_still_withholds_acceptance(prepared: Any) -> None:
    record = _record(prepared, failure_code="database_error").to_dict()
    assert record["outcome"] == "failed"
    assert record["reconciliation"]["passed"]
    assert record["acceptance"] is None


def test_duplicate_attempt_is_idempotent_and_conflicting_outcome_is_rejected(prepared: Any) -> None:
    archive, _, _ = prepared
    first = _record(prepared)
    before = _snapshot(archive.root)
    assert _record(prepared) == first
    assert _snapshot(archive.root) == before
    with pytest.raises(PublicationEvidenceError, match="immutable outcome"):
        _record(prepared, failure_code="database_error")
    with pytest.raises(PublicationEvidenceError, match="immutable outcome"):
        _record(prepared, ended_at=START + timedelta(seconds=3))
    assert archive.get_attempt(ATTEMPT) == first
    assert _snapshot(archive.root) == before


def test_failed_new_attempt_does_not_change_acceptance_or_release_pointer(prepared: Any) -> None:
    archive, plan, _ = prepared
    release = ReleaseStore(archive.data_root)
    release.activate(
        candidate=plan.vintage, expected_generation=0, bootstrap=True, reason="Fixture"
    )
    release_before = _snapshot(release.root)
    accepted = _record(prepared)
    accepted_bytes = (archive.root / f"{accepted.record_sha256}.json").read_bytes()
    _record(
        prepared, attempt_id=str(uuid4()), failure_code="database_error", receipt=None, report=None
    )
    assert (archive.root / f"{accepted.record_sha256}.json").read_bytes() == accepted_bytes
    assert _snapshot(release.root) == release_before


@pytest.mark.parametrize("stage", ["file_sync", "link", "directory_sync"])
def test_interrupted_publication_exposes_only_complete_records_and_can_retry(
    prepared: Any, stage: str
) -> None:
    archive, plan, kwargs = prepared
    archive.prepare(plan, sink="mongodb", attempt_id=ATTEMPT)
    target = {"file_sync": "os.fsync", "link": "os.link", "directory_sync": "_sync_directory"}[
        stage
    ]
    with (
        patch(
            f"data_platform_lab.serving.publish_evidence.{target}",
            side_effect=OSError("interrupted"),
        ),
        pytest.raises(PublicationEvidenceError),
    ):
        archive.record(plan, **kwargs)
    assert not list(archive.root.glob("*.tmp"))
    history = archive.history()
    assert len(history) == (1 if stage == "directory_sync" else 0)
    recovered = archive.record(plan, **kwargs)
    assert len(archive.history()) == 1
    assert archive.get_attempt(ATTEMPT) == recovered


def test_concurrent_identical_writers_produce_one_record(prepared: Any) -> None:
    archive, plan, kwargs = prepared

    def record(_: int) -> Any:
        return PublicationArchive(archive.data_root).record(plan, **kwargs)

    with ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(record, range(8)))
    assert len({record.record_sha256 for record in records}) == 1
    assert len(archive.history()) == 1


def test_concurrent_conflicting_writers_cannot_publish_two_outcomes(prepared: Any) -> None:
    archive, plan, kwargs = prepared

    def record(failure: str | None) -> str:
        try:
            PublicationArchive(archive.data_root).record(plan, **kwargs, failure_code=failure)
            return "written"
        except PublicationEvidenceError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(record, [None, "database_error"]))
    assert sorted(results) == ["conflict", "written"]
    assert len(archive.history()) == 1


def _tamper_record(archive: PublicationArchive, evidence: Any, mutation: str) -> None:
    """Rehash altered JSON to test internal consistency beyond a filename checksum."""
    raw = evidence.to_dict()
    if mutation == "accepted_without_receipt":
        raw["receipt"] = None
    elif mutation == "bad_status":
        raw["status"] = "failed"
    elif mutation == "missing_acceptance":
        raw["acceptance"] = None
    elif mutation == "bool_count":
        raw["counts"]["source"] = True
    elif mutation == "receipt_pin_type":
        raw["receipt"]["vintage"]["model_version"] = True
    elif mutation == "acceptance_pin_type":
        raw["acceptance"]["vintage"]["model_version"] = 1.0
    elif mutation == "bad_timestamp":
        raw["ended_at"] = START.replace(tzinfo=None).isoformat()
    elif mutation == "negative_duration":
        raw["duration_seconds"] = -1.0
    elif mutation == "wrong_hash":
        raw["fingerprints"]["observed_sink_sha256"] = "f" * 64
    elif mutation == "extra_secret":
        raw["password"] = "not-a-real-secret"
    elif mutation == "invalid_verdict":
        raw["reconciliation"]["passed"] = False
    elif mutation == "missing_count":
        raw["reconciliation"]["missing_count"] = 1
    elif mutation == "invalid_attempt":
        raw["run_id"] = "../../escape"
    elif mutation == "unknown_schema":
        raw["schema_version"] = True
    payload = publish_evidence._canonical(raw)
    (archive.root / f"{evidence.record_sha256}.json").unlink()
    (archive.root / f"{hashlib.sha256(payload).hexdigest()}.json").write_bytes(payload)


@pytest.mark.parametrize(
    "mutation",
    [
        "accepted_without_receipt",
        "bad_status",
        "missing_acceptance",
        "bool_count",
        "receipt_pin_type",
        "acceptance_pin_type",
        "bad_timestamp",
        "negative_duration",
        "wrong_hash",
        "extra_secret",
        "invalid_verdict",
        "missing_count",
        "invalid_attempt",
        "unknown_schema",
    ],
)
def test_rehashed_inconsistent_evidence_is_rejected(prepared: Any, mutation: str) -> None:
    archive, _, _ = prepared
    evidence = _record(prepared)
    _tamper_record(archive, evidence, mutation)
    with pytest.raises(PublicationEvidenceError):
        archive.history()


@pytest.mark.parametrize(
    "mutation", ["bytes", "noncanonical", "oversized", "json", "duplicate_keys"]
)
def test_malformed_or_modified_evidence_is_rejected(prepared: Any, mutation: str) -> None:
    archive, _, _ = prepared
    evidence = _record(prepared)
    payload = {
        "bytes": evidence.payload + b" ",
        "noncanonical": json.dumps(evidence.to_dict(), indent=2).encode(),
        "oversized": b" " * (publish_evidence._MAX_BYTES + 1),
        "json": b"{bad json",
        "duplicate_keys": evidence.payload.replace(
            b'"schema_version":1', b'"schema_version":1,"schema_version":1'
        ),
    }[mutation]
    path = archive.root / f"{evidence.record_sha256}.json"
    path.write_bytes(payload)
    if mutation != "bytes":
        path.rename(archive.root / f"{hashlib.sha256(payload).hexdigest()}.json")
    with pytest.raises(PublicationEvidenceError):
        archive.history()


def test_duplicate_attempt_records_are_detected_before_history_filtering(prepared: Any) -> None:
    archive, _, _ = prepared
    evidence = _record(prepared)
    raw = evidence.to_dict()
    raw["ended_at"] = raw["created_at"] = (START + timedelta(seconds=3)).isoformat()
    raw["duration_seconds"] = 3.0
    payload = publish_evidence._canonical(raw)
    (archive.root / f"{hashlib.sha256(payload).hexdigest()}.json").write_bytes(payload)
    with pytest.raises(PublicationEvidenceError, match="conflicting outcomes"):
        archive.history(last=1)


def test_symlink_record_and_archive_are_rejected_without_following(
    prepared: Any, tmp_path: Path
) -> None:
    archive, _, _ = prepared
    evidence = _record(prepared)
    path = archive.root / f"{evidence.record_sha256}.json"
    other = tmp_path / "untouched.json"
    path.rename(other)
    path.symlink_to(other)
    with pytest.raises(PublicationEvidenceError):
        archive.history()
    assert other.read_bytes() == evidence.payload
    alias = tmp_path / "alias"
    alias.symlink_to(archive.data_root, target_is_directory=True)
    with pytest.raises(PublicationEvidenceError, match="unsafe"):
        PublicationArchive(alias).history()


def test_empty_history_is_read_only_and_needs_no_sources_or_clients(
    tmp_path: Path, capsys: Any
) -> None:
    root = tmp_path / "absent"
    platform_main(["ons-pay-publication-history", "--data-root", str(root)])
    assert json.loads(capsys.readouterr().out)["records"] == []
    assert not root.exists()


def test_history_remains_read_only_after_source_artifact_is_removed(
    prepared: Any, capsys: Any
) -> None:
    archive, _, _ = prepared
    evidence = _record(prepared)
    for path in archive.data_root.rglob("*.parquet"):
        path.unlink()
    before = _snapshot(archive.data_root)
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        platform_main(
            [
                "ons-pay-publication-history",
                "--data-root",
                str(archive.data_root),
                "--attempt-id",
                ATTEMPT,
            ]
        )
    records = json.loads(capsys.readouterr().out)["records"]
    assert records[0]["record_sha256"] == evidence.record_sha256
    assert _snapshot(archive.data_root) == before


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_cli_recording_and_historical_retry_do_not_repeat_sink_io(
    curated: Any, capsys: Any, sink: str
) -> None:
    args = [*_args(curated, sink), "--apply", "--record-evidence", "--attempt-id", ATTEMPT]
    kwargs = (
        {"postgres_connection": CliPostgres("local_authority_yoy")}
        if sink == "postgresql"
        else {"mongo_collection": CliMongo()}
    )
    first = _invoke(capsys, args, **kwargs)
    assert first["status"] == "accepted" and not first["evidence"]["replayed"]
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        repeated = _invoke(capsys, args, environ={})
    assert repeated["evidence"]["replayed"]
    assert repeated["evidence"]["record_sha256"] == first["evidence"]["record_sha256"]
    assert repeated["acceptance"] == first["acceptance"]


@pytest.mark.parametrize("failure", ["partial", "drift", "read", "malformed", "configuration"])
def test_cli_records_failure_or_rejection_without_acceptance(
    curated: Any, capsys: Any, failure: str
) -> None:
    mongo = CliMongo(fail_at=2 if failure == "partial" else 0, fail_read=failure == "read")
    mongo.drift = failure == "drift"
    mongo.malformed = failure == "malformed"
    args = [*_args(curated, "mongodb"), "--apply", "--record-evidence", "--attempt-id", ATTEMPT]
    kwargs = {} if failure == "configuration" else {"mongo_collection": mongo}
    exit_code = 2 if failure == "configuration" else 3 if failure in ("drift", "malformed") else 4
    result = _invoke(capsys, args, expected_exit=exit_code, environ={}, **kwargs)
    assert result["acceptance"] is None
    record = PublicationArchive(curated[0]).get_attempt(ATTEMPT).to_dict()
    assert record["outcome"] == ("rejected" if failure == "drift" else "failed")
    assert record["acceptance"] is None
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        repeated = _invoke(capsys, args, expected_exit=exit_code, environ={})
    assert repeated["evidence"]["record_sha256"] == result["evidence"]["record_sha256"]


def test_lost_postgres_commit_ack_records_failure_before_explicit_recovery(
    curated: Any,
    capsys: Any,
) -> None:
    """Visible SQL rows cannot create accepted evidence without a completed write receipt."""
    connection = CliPostgres("local_authority_yoy")
    commit = connection.commit

    def commit_then_disconnect() -> None:
        commit()
        raise OSError("connection lost after server commit")

    args = [*_args(curated), "--apply", "--record-evidence"]
    with patch.object(connection, "commit", side_effect=commit_then_disconnect):
        failed = _invoke(capsys, args, postgres_connection=connection, expected_exit=4)
    assert connection.persisted and connection.rollbacks == 1
    assert failed["acceptance"] is None and failed["counts"]["applied"] is None
    archive = PublicationArchive(curated[0])
    prior = archive.get_attempt(failed["evidence"]["attempt_id"])
    assert prior.to_dict()["outcome"] == "failed"
    assert prior.to_dict()["receipt"] is None
    snapshot = dict(connection.persisted)

    recovered = _invoke(capsys, args, postgres_connection=connection)
    assert recovered["status"] == "accepted"
    assert recovered["evidence"]["attempt_id"] != failed["evidence"]["attempt_id"]
    assert connection.persisted == snapshot
    assert archive.get_attempt(failed["evidence"]["attempt_id"]).payload == prior.payload


def test_cli_new_attempt_explicitly_recovers_partial_publication(curated: Any, capsys: Any) -> None:
    mongo = CliMongo(fail_at=2)
    args = [*_args(curated, "mongodb"), "--apply", "--record-evidence"]
    failed = _invoke(capsys, args, mongo_collection=mongo, expected_exit=4)
    mongo.fail_at = 0
    accepted = _invoke(capsys, args, mongo_collection=mongo)
    assert failed["evidence"]["attempt_id"] != accepted["evidence"]["attempt_id"]
    assert [item.to_dict()["outcome"] for item in PublicationArchive(curated[0]).history()] == [
        "failed",
        "accepted",
    ]


@pytest.mark.parametrize(
    "extra",
    [
        ["--record-evidence"],
        ["--verify", "--record-evidence"],
        ["--attempt-id", ATTEMPT],
        ["--apply", "--record-evidence", "--attempt-id", "invalid"],
    ],
)
def test_evidence_flags_cannot_mutate_preview_or_verification(
    curated: Any, capsys: Any, extra: list[str]
) -> None:
    before = _snapshot(curated[0])
    _invoke(capsys, [*_args(curated), *extra], expected_exit=2)
    assert _snapshot(curated[0]) == before


def test_cli_rejects_conflicting_id_or_corrupt_archive_before_connecting(
    curated: Any, capsys: Any
) -> None:
    args = [*_args(curated, "mongodb"), "--apply", "--record-evidence", "--attempt-id", ATTEMPT]
    _invoke(capsys, args, mongo_collection=CliMongo())
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        _invoke(capsys, [*args, "--sink", "postgresql"], expected_exit=5)
        archive = PublicationArchive(curated[0])
        next(archive.root.glob("*.json")).write_bytes(b"corrupted")
        _invoke(capsys, args, expected_exit=5)


def test_cli_failed_evidence_write_does_not_claim_durable_acceptance(
    curated: Any, capsys: Any
) -> None:
    args = [*_args(curated, "mongodb"), "--apply", "--record-evidence", "--attempt-id", ATTEMPT]
    mongo = CliMongo()
    with patch.object(PublicationArchive, "_publish", side_effect=OSError("disk full")):
        result = _invoke(capsys, args, mongo_collection=mongo, expected_exit=5)
    assert result["status"] == "failed" and result["acceptance"] is None
    assert result["evidence"]["record_sha256"] is None
    assert len(mongo.documents) == 6
    assert PublicationArchive(curated[0]).history() == ()


def test_archive_cap_fails_before_adding_an_unreadable_record(prepared: Any) -> None:
    archive, plan, _ = prepared
    _record(prepared)
    with patch.object(publish_evidence, "_MAX_RECORDS", 1):
        with pytest.raises(PublicationEvidenceError, match="limit"):
            _record(prepared, attempt_id=str(uuid4()))
        with pytest.raises(PublicationEvidenceError, match="limit"):
            archive.prepare(plan, sink="mongodb", attempt_id=str(uuid4()))
    assert len(archive.history()) == 1


def test_inconsistent_input_evidence_never_creates_an_accepted_record(prepared: Any) -> None:
    archive, _, kwargs = prepared
    with pytest.raises(PublicationEvidenceError):
        _record(prepared, receipt=replace(kwargs["receipt"], records_sha256="f" * 64))
    with pytest.raises(PublicationEvidenceError):
        _record(prepared, report=replace(kwargs["report"], expected_sink_sha256="f" * 64))
    assert not archive.root.exists()
