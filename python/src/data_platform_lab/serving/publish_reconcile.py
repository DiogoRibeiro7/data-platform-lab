"""Read-only, exact-vintage reconciliation for PostgreSQL and MongoDB Gold sinks.

Compare a complete sink vintage against an independently validated publishing
plan. No writes, transaction changes, schema creation, or external connections
are made by this module; clients inject their own read-capable handles.
"""

from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, Protocol, TypeAlias, cast

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import (
    _MAX_ROWS,
    _NUMERIC_FIELDS,
    _TABLES,
    PublishPlan,
    TableName,
    _canonical_json,
    validate_publishing_plan,
)
from data_platform_lab.serving.publish_sinks import SinkName

_SHA = re.compile(r"^[0-9a-f]{64}$")
SafeValue: TypeAlias = str | int | float | list["SafeValue"] | dict[str, "SafeValue"] | None
Snapshot: TypeAlias = dict[str, dict[str, SafeValue]]


class ReconciliationError(RuntimeError):
    """A sink read could not produce a trustworthy, bounded comparison."""


@dataclass(frozen=True, slots=True)
class ReconciliationReport:
    """Complete, deterministic comparison for one pinned table and vintage."""

    sink: SinkName
    table: TableName
    vintage: VintageKey
    passed: bool
    expected_count: int
    observed_count: int
    source_plan_sha256: str
    expected_sink_sha256: str
    observed_sink_sha256: str
    missing_ids: tuple[str, ...]
    unexpected_ids: tuple[str, ...]
    changed_fields: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        """Return portable JSON-ready evidence without sink credentials."""
        return asdict(self)


class PostgresRows(Protocol):
    """Minimal SQL cursor return type for one result set."""

    def fetchall(self) -> list[tuple[object, ...]]:
        """Return the bounded snapshot as row tuples."""
        ...


class PostgresReader(Protocol):
    """Injected PostgreSQL reader; the caller owns its connection."""

    def execute(self, query: str, params: tuple[object, ...]) -> PostgresRows:
        """Execute a parameterized SELECT without committing."""
        ...


class MongoCursor(Protocol):
    """Minimal PyMongo cursor interface with a server-side row cap."""

    def limit(self, limit: int) -> MongoCursor:
        """Limit the number of documents returned."""
        ...

    def __iter__(self) -> Iterator[dict[str, object]]:
        """Iterate over the bounded result set."""
        ...


class MongoReader(Protocol):
    """Injected MongoDB-compatible collection; no writes are required."""

    def find(self, filter: dict[str, object]) -> MongoCursor:
        """Select documents by their complete nested vintage pins."""
        ...


def _safe(value: object, *, allow_date: bool = False) -> SafeValue:
    """Normalize JSON-compatible sink fields and reject unsafe driver types."""
    if value is None or type(value) in (str, int):
        return cast(str | int | None, value)
    if type(value) is float:
        if not math.isfinite(value):
            raise ReconciliationError("sink returned a non-finite numeric value")
        return value
    if type(value) is date:
        if allow_date:
            return value.isoformat()
        raise ReconciliationError("sink returned an unexpected date scalar")
    if type(value) is list:
        return [_safe(item, allow_date=allow_date) for item in value]
    if type(value) is dict:
        if not all(type(key) is str for key in value):
            raise ReconciliationError("sink returned a document with non-text keys")
        return {key: _safe(item, allow_date=allow_date) for key, item in value.items()}
    raise ReconciliationError("sink returned an unsupported or unsafe data type")


def _normalized_flat(record: Mapping[str, object], *, table: TableName) -> dict[str, SafeValue]:
    """Coerce PostgreSQL DOUBLE PRECISION and DATE into comparable JSON types.

    SQL DOUBLE PRECISION returns a float even when an accepted Gold numeric
    source happened to be an integer. Preserve the int when its magnitude
    would exceed binary64's exact-integer range; such loss must show as drift.
    """
    fields = ("record_id", "model_version", *_TABLES[table].columns)
    if set(record) != set(fields):
        raise ReconciliationError("SQL sink returned unexpected or missing columns")
    normalized: dict[str, SafeValue] = {}
    for name in fields:
        value = _safe(record[name], allow_date=name == "reference_date")
        if name in _NUMERIC_FIELDS and type(value) is int and abs(value) <= 2**53:
            value = float(value)
        normalized[name] = value
    return normalized


def _identifier(record: Mapping[str, SafeValue], sink: SinkName) -> str:
    """Extract a SHA-256 key; reject documents without valid, unique identities."""
    key = "record_id" if sink == "postgresql" else "_id"
    identifier = record.get(key)
    if type(identifier) is not str or _SHA.fullmatch(identifier) is None:
        raise ReconciliationError("sink returned a missing or invalid record ID")
    return identifier


def _by_id(records: list[dict[str, SafeValue]], sink: SinkName) -> Snapshot:
    """Construct a finite snapshot; duplicates never silently overwrite rows."""
    if len(records) > _MAX_ROWS:
        raise ReconciliationError("sink vintage exceeds the 100000-row reconciliation limit")
    result: Snapshot = {}
    for record in records:
        identifier = _identifier(record, sink)
        if identifier in result:
            raise ReconciliationError("sink returned duplicate record IDs")
        result[identifier] = record
    return result


def _digest(snapshot: Snapshot) -> str:
    """Hash records in ID order, regardless of database physical row ordering."""
    records = [snapshot[identifier] for identifier in sorted(snapshot)]
    return hashlib.sha256(_canonical_json(records)).hexdigest()


def _changed_paths(expected: object, actual: object, prefix: str = "") -> tuple[str, ...]:
    """List field paths without revealing sensitive values in drift reports."""
    if type(expected) is dict and type(actual) is dict:
        differences: list[str] = []
        names = sorted(set(expected) | set(actual))
        for name in names:
            path = f"{prefix}.{name}" if prefix else str(name)
            if name not in expected or name not in actual:
                differences.append(path)
            else:
                differences.extend(_changed_paths(expected[name], actual[name], path))
        return tuple(differences)
    # Binary64 sign of zero and int-vs-float differences matter to the digest.
    if type(expected) is not type(actual) or expected != actual:
        return (prefix,)
    if (
        type(expected) is float
        and type(actual) is float
        and math.copysign(1, expected) != math.copysign(1, actual)
    ):
        return (prefix,) if expected == actual == 0.0 else ()
    return ()


def _report(
    *,
    plan: PublishPlan,
    sink: SinkName,
    expected_records: list[dict[str, SafeValue]],
    observed_records: list[dict[str, SafeValue]],
) -> ReconciliationReport:
    """Compare complete source and sink populations without mutating either."""
    expected = _by_id(expected_records, sink)
    observed = _by_id(observed_records, sink)
    missing = tuple(sorted(expected.keys() - observed.keys()))
    unexpected = tuple(sorted(observed.keys() - expected.keys()))
    shared = sorted(expected.keys() & observed.keys())
    changed = {
        identifier: fields
        for identifier in shared
        if (fields := _changed_paths(expected[identifier], observed[identifier]))
    }
    expected_sha = _digest(expected)
    observed_sha = _digest(observed)
    passed = (
        not missing
        and not unexpected
        and not changed
        and len(expected) == len(observed)
        and expected_sha == observed_sha
    )
    return ReconciliationReport(
        sink=sink,
        table=plan.table,
        vintage=plan.vintage,
        passed=passed,
        expected_count=len(expected),
        observed_count=len(observed),
        source_plan_sha256=plan.records_sha256,
        expected_sink_sha256=expected_sha,
        observed_sink_sha256=observed_sha,
        missing_ids=missing,
        unexpected_ids=unexpected,
        changed_fields=changed,
    )


def _postgres_select(table: TableName) -> str:
    """Build a bounded SELECT from fixed source-contract SQL identifiers."""
    columns = ("record_id", "model_version", *_TABLES[table].columns)
    return (
        f"SELECT {', '.join(columns)} FROM ons_pay_{table} "
        "WHERE model_version = %s AND registry_commit = %s AND source_sha256 = %s "
        "ORDER BY record_id LIMIT %s"
    )


def reconcile_postgres(plan: PublishPlan, connection: PostgresReader) -> ReconciliationReport:
    """Read all records for one SQL vintage and identify any drift.

    Runs one bounded, parameterized SELECT. The caller is responsible for
    snapshot isolation if concurrent writers might change the sink mid-read.
    """
    validated = validate_publishing_plan(plan, expected_shape="relational")
    expected_records = [_normalized_flat(record, table=plan.table) for record in validated]
    columns = ("record_id", "model_version", *_TABLES[plan.table].columns)
    try:
        rows = connection.execute(
            _postgres_select(plan.table),
            (
                plan.vintage.model_version,
                plan.vintage.registry_commit,
                plan.vintage.source_sha256,
                _MAX_ROWS + 1,
            ),
        ).fetchall()
    except Exception as exc:
        raise ReconciliationError("PostgreSQL reconciliation SELECT failed") from exc
    if len(rows) > _MAX_ROWS:
        raise ReconciliationError("sink vintage exceeds the 100000-row reconciliation limit")
    observed: list[dict[str, SafeValue]] = []
    for row in rows:
        if type(row) is not tuple or len(row) != len(columns):
            raise ReconciliationError("SQL sink returned an invalid result row")
        observed.append(_normalized_flat(dict(zip(columns, row, strict=True)), table=plan.table))
    return _report(
        plan=plan, sink="postgresql", expected_records=expected_records, observed_records=observed
    )


def reconcile_mongodb(plan: PublishPlan, collection: MongoReader) -> ReconciliationReport:
    """Read a bounded document vintage with a full, exact nested pin filter."""
    validated = validate_publishing_plan(plan, expected_shape="document")
    expected_records = [cast(dict[str, SafeValue], _safe(record)) for record in validated]
    pins: dict[str, object] = {
        "vintage.model_version": plan.vintage.model_version,
        "vintage.registry_commit": plan.vintage.registry_commit,
        "vintage.source_sha256": plan.vintage.source_sha256,
    }
    try:
        cursor = collection.find(pins).limit(_MAX_ROWS + 1)
        observed_raw: list[dict[str, object]] = []
        for record in cursor:
            observed_raw.append(record)
            if len(observed_raw) > _MAX_ROWS:
                raise ReconciliationError(
                    "sink vintage exceeds the 100000-row reconciliation limit"
                )
    except Exception as exc:
        if isinstance(exc, ReconciliationError):
            raise
        raise ReconciliationError("MongoDB reconciliation find failed") from exc
    observed: list[dict[str, SafeValue]] = []
    for raw in observed_raw:
        normalized = _safe(raw)
        if type(normalized) is not dict:
            raise ReconciliationError("MongoDB sink returned a non-document row")
        observed.append(normalized)
    return _report(
        plan=plan, sink="mongodb", expected_records=expected_records, observed_records=observed
    )
