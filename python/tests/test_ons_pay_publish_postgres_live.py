"""Opt-in real PostgreSQL smoke for exact-vintage Gold upsert semantics."""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg
import pytest
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_cli import main as publish_main
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
