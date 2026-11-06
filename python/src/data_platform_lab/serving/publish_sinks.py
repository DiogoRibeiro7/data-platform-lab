"""Explicit, idempotent PostgreSQL and MongoDB-style Gold publishing adapters.

Connections are injected: importing this module neither opens a connection
nor reads credentials. Callers must first prepare and review an exact-vintage
PublishPlan using the read-only curation pipeline.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from typing import Literal, Protocol, cast

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import (
    _NULLABLE_FIELDS,
    _TABLES,
    ProjectedRecord,
    PublishPlan,
    PublishPlanError,
    TableName,
    validate_publishing_plan,
)

SinkName = Literal["postgresql", "mongodb"]

_SQL_TYPES: dict[str, str] = {
    "record_id": "CHAR(64)",
    "model_version": "INTEGER",
    "geography": "TEXT",
    "pay_year": "INTEGER",
    "areas_total": "INTEGER",
    "areas_with_pay": "INTEGER",
    "areas_without_pay": "INTEGER",
    "min_area_weekly_pay_gbp": "DOUBLE PRECISION",
    "median_area_weekly_pay_gbp": "DOUBLE PRECISION",
    "max_area_weekly_pay_gbp": "DOUBLE PRECISION",
    "area_code": "TEXT",
    "area_name": "TEXT",
    "reference_date": "DATE",
    "weekly_pay_gbp": "DOUBLE PRECISION",
    "status_code": "TEXT",
    "previous_year": "INTEGER",
    "previous_weekly_pay_gbp": "DOUBLE PRECISION",
    "yoy_nominal_pct": "DOUBLE PRECISION",
    "source_sha256": "CHAR(64)",
    "registry_commit": "CHAR(40)",
}


class PublishSinkError(RuntimeError):
    """An adapter cannot safely complete its requested batch."""


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    """Result returned only after a whole sink batch completes successfully."""

    sink: SinkName
    table: TableName
    vintage: VintageKey
    records_applied: int
    records_sha256: str


class PostgresCursor(Protocol):
    """The narrow psycopg cursor interface needed for batched upserts."""

    def executemany(self, query: str, params_seq: Sequence[tuple[object, ...]]) -> object:
        """Execute the same parameterized command for a bounded batch."""
        ...

    def close(self) -> None:
        """Release this cursor without closing the caller's connection."""
        ...


class PostgresConnection(Protocol):
    """Injected PostgreSQL transaction boundary (psycopg-compatible)."""

    def execute(self, query: str) -> object:
        """Execute a trusted static schema statement."""
        ...

    def cursor(self) -> PostgresCursor:
        """Create a cursor for data upserts."""
        ...

    def commit(self) -> None:
        """Commit the entire publication."""
        ...

    def rollback(self) -> None:
        """Discard every statement in a failed publication."""
        ...


class ReplaceResult(Protocol):
    """Acknowledgment returned by pymongo Collection.replace_one."""

    @property
    def acknowledged(self) -> bool:
        """Whether the server confirmed the replacement."""
        ...


class MongoCollection(Protocol):
    """Injected MongoDB-like collection supporting idempotent replacement."""

    def replace_one(
        self,
        filter: dict[str, str],
        replacement: dict[str, object],
        *,
        upsert: bool,
    ) -> ReplaceResult:
        """Replace a single stable-key document or insert it if absent."""
        ...


def _table_name(table: TableName) -> str:
    """Map one checked logical name to a fixed, unquoted SQL identifier."""
    if table not in _TABLES:
        raise PublishPlanError("unsupported Gold table")
    return f"ons_pay_{table}"


def postgres_schema_sql(table: TableName) -> str:
    """Emit safe PostgreSQL DDL for exactly one versioned Gold contract."""
    contract = _TABLES.get(table)
    if contract is None:
        raise PublishPlanError("unsupported Gold table")
    columns = ("record_id", "model_version", *contract.columns)
    definitions = [
        f"{name} {_SQL_TYPES[name]}{' NOT NULL' if name not in _NULLABLE_FIELDS else ''}"
        for name in columns
    ]
    definitions.append("PRIMARY KEY (record_id)")
    if table == "geography_year":
        definitions.append("CHECK (areas_total = areas_with_pay + areas_without_pay)")
    return (
        f"CREATE TABLE IF NOT EXISTS {_table_name(table)} (\n  " + ",\n  ".join(definitions) + "\n)"
    )


def postgres_upsert_sql(table: TableName) -> str:
    """Emit a parameterized and repeatable upsert for a fixed table contract."""
    contract = _TABLES.get(table)
    if contract is None:
        raise PublishPlanError("unsupported Gold table")
    columns = ("record_id", "model_version", *contract.columns)
    placeholders = ", ".join("%s" for _ in columns)
    updates = ", ".join(f"{column} = EXCLUDED.{column}" for column in columns[1:])
    return (
        f"INSERT INTO {_table_name(table)} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT (record_id) DO UPDATE SET {updates}"
    )


def _sql_values(record: ProjectedRecord, table: TableName) -> tuple[object, ...]:
    """Adapt ISO date values to PostgreSQL date parameters; retain SQL NULL."""
    columns = ("record_id", "model_version", *_TABLES[table].columns)
    values: list[object] = []
    for column in columns:
        value: object = record[column]
        if column == "reference_date":
            value = date.fromisoformat(cast(str, value))
        values.append(value)
    return tuple(values)


def publish_postgres(
    plan: PublishPlan, connection: PostgresConnection, *, batch_size: int = 500
) -> PublishReceipt:
    """Transactionally upsert one validated relational vintage into PostgreSQL.

    This function does not own or close the connection. Pass a connection with
    autocommit disabled; DDL and all row writes share one transaction. A failed
    commit or statement triggers a rollback attempt and never returns a success
    receipt. A lost commit acknowledgment can leave committed writes despite
    a successful subsequent rollback; inspect the sink before explicit replay.
    """
    if type(batch_size) is not int or not 1 <= batch_size <= 5000:
        raise PublishPlanError("PostgreSQL batch_size must be an integer from 1 to 5000")
    records = validate_publishing_plan(plan, expected_shape="relational")
    if getattr(connection, "autocommit", False):
        raise PublishSinkError("PostgreSQL publishing requires autocommit=False")
    try:
        connection.execute(postgres_schema_sql(plan.table))
        cursor = connection.cursor()
        try:
            query = postgres_upsert_sql(plan.table)
            for start in range(0, len(records), batch_size):
                params = [
                    _sql_values(record, plan.table)
                    for record in records[start : start + batch_size]
                ]
                cursor.executemany(query, params)
        finally:
            cursor.close()
        connection.commit()
    except Exception as exc:
        try:
            connection.rollback()
        except Exception as rollback_exc:
            raise PublishSinkError("PostgreSQL publication and rollback both failed") from (
                rollback_exc
            )
        raise PublishSinkError(
            "PostgreSQL publication failed; rollback attempted, commit outcome may be unknown"
        ) from exc
    return PublishReceipt(
        sink="postgresql",
        table=plan.table,
        vintage=plan.vintage,
        records_applied=len(records),
        records_sha256=plan.records_sha256,
    )


def publish_mongodb(plan: PublishPlan, collection: MongoCollection) -> PublishReceipt:
    """Replace each validated document by stable _id (upsert, no deletion).

    Individual replace_one calls are atomic, but this batch is NOT one Mongo
    transaction. On failure some records may already be visible. Repeating
    the same fully pinned plan is idempotent and safely resumes the batch.
    """
    records = validate_publishing_plan(plan, expected_shape="document")
    try:
        for record in records:
            identifier = cast(str, record["_id"])
            result = collection.replace_one(
                {"_id": identifier}, cast(dict[str, object], record), upsert=True
            )
            if result.acknowledged is not True:
                raise PublishSinkError("MongoDB did not acknowledge a document replacement")
    except Exception as exc:
        raise PublishSinkError(
            "MongoDB publication incomplete; retry the exact same pinned plan"
        ) from exc
    return PublishReceipt(
        sink="mongodb",
        table=plan.table,
        vintage=plan.vintage,
        records_applied=len(records),
        records_sha256=plan.records_sha256,
    )
