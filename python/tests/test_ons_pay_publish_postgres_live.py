"""Opt-in real PostgreSQL smoke for exact-vintage Gold upsert semantics."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from test_ons_pay_publish_cli import _args, _invoke
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.serving import publish_reconcile, publish_sinks
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_cli import main as publish_main
from data_platform_lab.serving.publish_evidence import PublicationArchive
from data_platform_lab.serving.publish_gate import (
    PublicationAcceptanceError,
    accept_publication,
    publish_and_accept_postgres,
)
from data_platform_lab.serving.publish_plan import TableName, plan_ons_pay_publishing
from data_platform_lab.serving.publish_reconcile import reconcile_postgres
from data_platform_lab.serving.publish_sinks import publish_postgres

pytestmark = pytest.mark.skipif(
    os.environ.get("ONS_PAY_LIVE_POSTGRES") != "1",
    reason="requires explicit opt-in and a running local PostgreSQL instance",
)


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_live_postgres_upsert_preserves_pins_and_replays(tmp_path: Path, table: TableName) -> None:
    """Exercise the real psycopg protocol with NULLs, keys and SQL types."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    vintage = VintageKey(1, pins["registry_commit"], pins["source_sha256"])
    plan = plan_ons_pay_publishing(
        data_root=root,
        vintage=vintage,
        table=table,
        shape="relational",
    )

    # This DSN is supplied by an operator or the ephemeral CI compose service.
    with psycopg.connect(os.environ["ONS_PAY_POSTGRES_DSN"], autocommit=False) as connection:
        first = publish_postgres(plan, connection, batch_size=2)
        second = publish_postgres(plan, connection, batch_size=3)
        assert first == second
        reconciled = reconcile_postgres(plan, connection)
        assert reconciled.passed
        assert reconciled.expected_count == reconciled.observed_count == plan.source_rows
        assert reconciled.expected_sink_sha256 == reconciled.observed_sink_sha256
        accepted = accept_publication(plan, second, reconciled)
        assert accepted.sink_snapshot_sha256 == reconciled.observed_sink_sha256
        assert accepted.records_accepted == plan.source_rows
        # The identifier is fixed by a validated table enumeration, not user SQL.
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT record_id FROM ons_pay_{table} "
                "WHERE registry_commit = %s AND source_sha256 = %s",
                (vintage.registry_commit, vintage.source_sha256),
            )
            rows = cursor.fetchall()
        assert len(rows) == plan.source_rows
        assert {row[0] for row in rows} == {record["record_id"] for record in plan.records}
        if table == "local_authority_yoy":
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT reference_date, weekly_pay_gbp, status_code "
                    "FROM ons_pay_local_authority_yoy "
                    "WHERE area_code = %s AND pay_year = %s "
                    "AND registry_commit = %s AND source_sha256 = %s",
                    ("A001", 2024, vintage.registry_commit, vintage.source_sha256),
                )
                suppressed = cursor.fetchone()
            assert suppressed is not None
            assert suppressed[0].isoformat() == "2024-04-01"
            assert suppressed[1] is None
            assert suppressed[2] == "x"
            # An uncommitted mutation must show up as drift; rollback restores
            # the original pinned records without changing the source.
            identifier = plan.records[0]["record_id"]
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE ons_pay_local_authority_yoy SET status_code = %s WHERE record_id = %s",
                    ("tampered", identifier),
                )
            drift = reconcile_postgres(plan, connection)
            assert not drift.passed
            assert drift.changed_fields == {str(identifier): ("status_code",)}
            with pytest.raises(PublicationAcceptanceError, match="sink read-back"):
                accept_publication(plan, second, drift)
            connection.rollback()
            assert reconcile_postgres(plan, connection).passed

        # The combined opt-in path publishes, reads back and requires a gate.
        verified = publish_and_accept_postgres(plan, writer=connection, reader=connection)
        assert verified == accepted


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_live_operator_cli_apply_verify_drift_and_explicit_recovery(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], table: TableName
) -> None:
    """Use CLI-owned psycopg connections against the disposable integration database."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    args = [
        "--data-root",
        str(root),
        "--model-version",
        "1",
        "--registry-commit",
        pins["registry_commit"],
        "--source-sha256",
        pins["source_sha256"],
        "--table",
        table,
        "--sink",
        "postgresql",
    ]
    settings = {"ONS_PAY_POSTGRES_DSN": os.environ["ONS_PAY_POSTGRES_DSN"]}
    publish_main(args, environ={})
    preview = json.loads(capsys.readouterr().out)
    assert preview["status"] == "previewed"
    for _ in range(2):
        publish_main([*args, "--apply"], environ=settings)
        accepted = json.loads(capsys.readouterr().out)
        assert accepted["status"] == "accepted"
        assert accepted["counts"]["source"] == accepted["counts"]["applied"]
        assert accepted["counts"]["source"] == accepted["counts"]["observed"]
        assert (
            accepted["fingerprints"]["source_plan_sha256"]
            == (preview["fingerprints"]["source_plan_sha256"])
        )
    publish_main([*args, "--verify"], environ=settings)
    verified = json.loads(capsys.readouterr().out)
    assert verified["status"] == "verified"
    assert verified["acceptance"] is None
    assert verified["counts"]["applied"] is None

    # Only mutate this synthetic pinned vintage in the explicitly opted-in DB.
    field = "geography" if table == "geography_year" else "area_name"
    with psycopg.connect(settings["ONS_PAY_POSTGRES_DSN"]) as connection:
        connection.execute(
            f"UPDATE ons_pay_{table} SET {field} = %s "
            "WHERE model_version = %s AND registry_commit = %s AND source_sha256 = %s",
            ("controlled-cli-drift", 1, pins["registry_commit"], pins["source_sha256"]),
        )
    with pytest.raises(SystemExit) as failed:
        publish_main([*args, "--verify"], environ=settings)
    assert failed.value.code == 3
    rejected = json.loads(capsys.readouterr().out)
    assert rejected["acceptance"] is None
    assert rejected["reconciliation"]["changed_record_count"] == rejected["counts"]["source"]
    publish_main([*args, "--apply"], environ=settings)
    assert json.loads(capsys.readouterr().out)["status"] == "accepted"


@pytest.mark.skipif(os.name != "posix", reason="durable recording requires POSIX locks/fsync")
@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_live_operator_records_durable_acceptance_and_reuses_attempt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], table: TableName
) -> None:
    """Persist actual psycopg write/read-back evidence and retry without another connection."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    args = [
        "--data-root",
        str(root),
        "--model-version",
        "1",
        "--registry-commit",
        pins["registry_commit"],
        "--source-sha256",
        pins["source_sha256"],
        "--table",
        table,
        "--sink",
        "postgresql",
        "--apply",
        "--record-evidence",
    ]
    publish_main(args, environ={"ONS_PAY_POSTGRES_DSN": os.environ["ONS_PAY_POSTGRES_DSN"]})
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "accepted"
    evidence = PublicationArchive(root).get_attempt(result["evidence"]["attempt_id"])
    assert evidence is not None
    assert evidence.record_sha256 == result["evidence"]["record_sha256"]
    assert evidence.to_dict()["acceptance"] == result["acceptance"]
    publish_main([*args, "--attempt-id", result["evidence"]["attempt_id"]], environ={})
    repeated = json.loads(capsys.readouterr().out)
    assert repeated["evidence"]["replayed"]
    assert repeated["evidence"]["record_sha256"] == evidence.record_sha256
    assert len(PublicationArchive(root).history()) == 1


@pytest.fixture
def timeout_target(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> Iterator[tuple[Path, list[str], dict[str, str]]]:
    """Isolate timeout tests in a schema with fallback limits against regressions."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    args = _args((root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])))
    dsn = os.environ["ONS_PAY_POSTGRES_DSN"]
    schema = f"ons_timeout_{uuid4().hex}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    settings = {
        "ONS_PAY_POSTGRES_DSN": make_conninfo(
            dsn,
            options=f"-c search_path={schema} -c statement_timeout=5000 -c lock_timeout=3000",
        )
    }
    try:
        evidence_flags = ["--record-evidence"] if os.name == "posix" else []
        seeded = _invoke(capsys, [*args, "--apply", *evidence_flags], environ=settings)
        assert seeded["status"] == "accepted"
        yield root, args, settings
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.mark.parametrize("operation", ["--apply", "--verify"])
@pytest.mark.parametrize(
    ("flag", "sqlstate"),
    [("--statement-timeout-ms", "57014"), ("--lock-timeout-ms", "55P03")],
)
def test_live_timeouts_cancel_blocked_operations_and_allow_explicit_recovery(
    timeout_target: tuple[Path, list[str], dict[str, str]],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    flag: str,
    sqlstate: str,
) -> None:
    """Check server cancellation codes, SQL rollback and immutable failure evidence."""
    root, args, settings = timeout_target
    evidence_flags = ["--record-evidence"] if os.name == "posix" else []
    previous = PublicationArchive(root).history() if evidence_flags else []
    module = publish_sinks if operation == "--apply" else publish_reconcile
    name = "publish_postgres" if operation == "--apply" else "reconcile_postgres"
    original = getattr(module, name)
    failures = []

    def capture_failure(*arguments: Any, **kwargs: Any) -> Any:
        try:
            return original(*arguments, **kwargs)
        except (publish_sinks.PublishSinkError, publish_reconcile.ReconciliationReadError) as exc:
            assert isinstance(exc.__cause__, psycopg.Error)
            failures.append(exc.__cause__.sqlstate)
            if operation == "--apply":
                assert arguments[1].info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            raise

    monkeypatch.setattr(module, name, capture_failure)
    with psycopg.connect(settings["ONS_PAY_POSTGRES_DSN"]) as blocker:
        blocker.execute("LOCK TABLE ons_pay_local_authority_yoy IN ACCESS EXCLUSIVE MODE")
        failed = _invoke(
            capsys,
            [*args, operation, flag, "500", *(evidence_flags if operation == "--apply" else [])],
            environ=settings,
            expected_exit=4,
        )
    assert failures == [sqlstate]
    assert failed["error"]["code"] == "database_error"
    assert failed["acceptance"] is None and failed["counts"]["applied"] is None
    assert settings["ONS_PAY_POSTGRES_DSN"] not in json.dumps(failed)
    if evidence_flags and operation == "--apply":
        evidence = PublicationArchive(root).get_attempt(failed["evidence"]["attempt_id"])
        assert evidence is not None and evidence.to_dict()["outcome"] == "failed"
        assert evidence.to_dict()["acceptance"] is None

    recovered = _invoke(capsys, [*args, "--apply", *evidence_flags], environ=settings)
    assert recovered["status"] == "accepted"
    assert recovered["counts"]["source"] == recovered["counts"]["applied"] == 6
    for evidence in previous:
        stored = PublicationArchive(root).get_attempt(evidence.to_dict()["run_id"])
        assert stored is not None and stored.payload == evidence.payload


@pytest.mark.parametrize(
    ("flag", "sqlstate"),
    [("--statement-timeout-ms", "57014"), ("--lock-timeout-ms", "55P03")],
)
def test_live_timeout_survives_commit_and_withholds_acceptance_on_readback_failure(
    timeout_target: tuple[Path, list[str], dict[str, str]],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    flag: str,
    sqlstate: str,
) -> None:
    """A committed write still requires successful read-back under the same timeout."""
    root, args, settings = timeout_target
    evidence_flags = ["--record-evidence"] if os.name == "posix" else []
    original_publish = publish_sinks.publish_postgres
    original_reconcile = publish_reconcile.reconcile_postgres
    failures = []

    def capture_read_failure(*arguments: Any, **kwargs: Any) -> Any:
        try:
            return original_reconcile(*arguments, **kwargs)
        except publish_reconcile.ReconciliationReadError as exc:
            assert isinstance(exc.__cause__, psycopg.Error)
            failures.append(exc.__cause__.sqlstate)
            raise

    monkeypatch.setattr(publish_reconcile, "reconcile_postgres", capture_read_failure)
    with psycopg.connect(settings["ONS_PAY_POSTGRES_DSN"]) as blocker:

        def publish_then_block(*arguments: Any, **kwargs: Any) -> Any:
            receipt = original_publish(*arguments, **kwargs)
            blocker.execute("LOCK TABLE ons_pay_local_authority_yoy IN ACCESS EXCLUSIVE MODE")
            return receipt

        monkeypatch.setattr(publish_sinks, "publish_postgres", publish_then_block)
        failed = _invoke(
            capsys,
            [*args, "--apply", flag, "500", *evidence_flags],
            environ=settings,
            expected_exit=4,
        )
    assert failures == [sqlstate]
    assert failed["counts"]["applied"] == 6
    assert failed["counts"]["observed"] is None and failed["acceptance"] is None
    if evidence_flags:
        evidence = PublicationArchive(root).get_attempt(failed["evidence"]["attempt_id"])
        assert evidence is not None and evidence.to_dict()["outcome"] == "failed"
        assert evidence.to_dict()["acceptance"] is None

    # Read-only verification sees the committed rows but cannot fabricate acceptance.
    verified = _invoke(capsys, [*args, "--verify"], environ=settings)
    assert verified["status"] == "verified" and verified["acceptance"] is None
