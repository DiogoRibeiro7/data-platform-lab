"""Opt-in real PostgreSQL smoke for exact-vintage Gold upsert semantics."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.serving.catalog import VintageKey
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
            connection.rollback()
            assert reconcile_postgres(plan, connection).passed
