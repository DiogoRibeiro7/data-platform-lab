"""Offline read-after-write reconciliation using real curated ONS Gold plans."""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from test_ons_pay_serving import COMMIT_B, _curate, _pin

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import (
    _TABLES,
    PublishPlan,
    PublishPlanError,
    SinkShape,
    TableName,
    _canonical_json,
    plan_ons_pay_publishing,
)
from data_platform_lab.serving.publish_reconcile import (
    ReconciliationError,
    ReconciliationReport,
    reconcile_mongodb,
    reconcile_postgres,
)


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, VintageKey]:
    """Create verified local Parquet and receipts, without live databases."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    return root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])


def _plan(curated: tuple[Path, VintageKey], *, table: TableName, shape: SinkShape) -> PublishPlan:
    root, vintage = curated
    return plan_ons_pay_publishing(data_root=root, vintage=vintage, table=table, shape=shape)


def _pg_values(plan: PublishPlan) -> list[tuple[object, ...]]:
    """Simulate typed psycopg fetch results, including date and float casts."""
    columns = ("record_id", "model_version", *_TABLES[plan.table].columns)
    values: list[tuple[object, ...]] = []
    for record in plan.records:
        row: list[object] = []
        for name in columns:
            value: object = record[name]
            if name == "reference_date":
                value = date.fromisoformat(str(value))
            if name in {"weekly_pay_gbp", "median_area_weekly_pay_gbp"} and type(value) is int:
                value = float(value)
            row.append(value)
        values.append(tuple(row))
    return values


class FakeResults:
    """Minimal cursor result supporting fetchall."""

    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple[object, ...]]:
        return self.rows


class FakePostgresReader:
    """Honor the exact vintage where-clause and LIMIT; perform no writes."""

    def __init__(
        self,
        rows: list[tuple[object, ...]],
        table: TableName,
        *,
        fail: bool = False,
    ) -> None:
        self.rows = rows
        self.table = table
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.fail = fail

    def execute(self, query: str, params: tuple[object, ...]) -> FakeResults:
        self.calls.append((query, params))
        if self.fail:
            raise OSError("database unavailable")
        assert f"FROM ons_pay_{self.table}" in query
        assert "ORDER BY record_id LIMIT %s" in query
        # row fields are record_id (0), model_version (1), and source pins (-2,-1).
        rows = [
            row
            for row in self.rows
            if row[1] == params[0] and row[-1] == params[1] and row[-2] == params[2]
        ]
        return FakeResults(sorted(rows, key=lambda row: str(row[0]))[: int(params[3])])


class FakeMongoCursor:
    """Enforce PyMongo's cursor limit behavior for an exact-vintage query."""

    def __init__(self, documents: list[dict[str, object]], *, fail: bool = False) -> None:
        self.documents = documents
        self.maximum: int | None = None
        self.fail = fail

    def limit(self, limit: int) -> FakeMongoCursor:
        if self.fail:
            raise OSError("cursor unavailable")
        self.maximum = limit
        return self

    def __iter__(self) -> Iterator[dict[str, object]]:
        if self.fail:
            raise OSError("cursor unavailable")
        return iter(self.documents[: self.maximum])


class FakeMongoReader:
    """Support exact nested-key filters and bounded, read-only results."""

    def __init__(self, documents: list[dict[str, object]], *, fail: bool = False) -> None:
        self.documents = documents
        self.calls: list[dict[str, object]] = []
        self.fail = fail
        self.cursor: FakeMongoCursor | None = None

    def find(self, filter: dict[str, object]) -> FakeMongoCursor:
        self.calls.append(dict(filter))
        if self.fail:
            raise OSError("database unavailable")
        found = [
            document
            for document in self.documents
            if isinstance(document.get("vintage"), dict)
            and all(
                document["vintage"].get(key.removeprefix("vintage.")) == value
                for key, value in filter.items()
            )
        ]
        self.cursor = FakeMongoCursor(found)
        return self.cursor


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_postgres_exact_vintage_reconciles_independent_of_sink_row_order(
    curated: tuple[Path, VintageKey], table: TableName
) -> None:
    plan = _plan(curated, table=table, shape="relational")
    rows = _pg_values(plan)[::-1]
    connection = FakePostgresReader(rows, table)
    report = reconcile_postgres(plan, connection)
    assert report.passed
    assert report.sink == "postgresql"
    assert report.table == table
    assert report.vintage == plan.vintage
    assert report.expected_count == report.observed_count == plan.source_rows
    assert report.source_plan_sha256 == plan.records_sha256
    assert report.expected_sink_sha256 == report.observed_sink_sha256
    assert report.missing_ids == report.unexpected_ids == ()
    assert report.changed_fields == {}
    assert len(connection.calls) == 1
    assert connection.calls[0][1] == (
        plan.vintage.model_version,
        plan.vintage.registry_commit,
        plan.vintage.source_sha256,
        100001,
    )
    assert connection.rows == rows
    assert json.loads(json.dumps(report.to_dict()))["passed"] is True


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_mongo_exact_vintage_reconciles_independent_of_document_order(
    curated: tuple[Path, VintageKey], table: TableName
) -> None:
    plan = _plan(curated, table=table, shape="document")
    documents = copy.deepcopy(list(plan.records))[::-1]
    collection = FakeMongoReader(documents)
    report = reconcile_mongodb(plan, collection)
    assert report.passed
    assert report.sink == "mongodb"
    assert report.expected_count == report.observed_count == plan.source_rows
    assert report.source_plan_sha256 == plan.records_sha256
    assert report.expected_sink_sha256 == report.observed_sink_sha256
    assert collection.calls == [
        {
            "vintage.model_version": plan.vintage.model_version,
            "vintage.registry_commit": plan.vintage.registry_commit,
            "vintage.source_sha256": plan.vintage.source_sha256,
        }
    ]
    assert collection.cursor is not None and collection.cursor.maximum == 100001
    assert collection.documents == documents


def test_postgres_detects_missing_rows(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="relational")
    rows = _pg_values(plan)
    report = reconcile_postgres(plan, FakePostgresReader(rows[:-1], plan.table))
    assert not report.passed
    assert report.expected_count == 6
    assert report.observed_count == 5
    assert report.missing_ids == (str(plan.records[-1]["record_id"]),)
    assert report.unexpected_ids == ()
    assert report.expected_sink_sha256 != report.observed_sink_sha256


def test_postgres_detects_unexpected_rows_for_same_pins(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="relational")
    rows = _pg_values(plan)
    extra = ("f" * 64, *rows[0][1:])
    report = reconcile_postgres(plan, FakePostgresReader([*rows, extra], plan.table))
    assert not report.passed
    assert report.expected_count == 5
    assert report.observed_count == 6
    assert report.unexpected_ids == ("f" * 64,)
    assert report.missing_ids == ()


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("weekly_pay_gbp", 0.0),
        ("area_name", "Tampered name"),
        ("status_code", "not-suppressed"),
    ],
)
def test_postgres_reports_exact_changed_columns(
    curated: tuple[Path, VintageKey], column: str, value: object
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="relational")
    columns = ("record_id", "model_version", *_TABLES[plan.table].columns)
    rows = _pg_values(plan)
    changed = list(rows[0])
    changed[columns.index(column)] = value
    rows[0] = tuple(changed)
    report = reconcile_postgres(plan, FakePostgresReader(rows, plan.table))
    assert not report.passed
    assert report.expected_count == report.observed_count == plan.source_rows
    assert report.changed_fields == {str(plan.records[0]["record_id"]): (column,)}
    assert report.expected_sink_sha256 != report.observed_sink_sha256


def test_postgres_wrong_row_lineage_is_detected_as_missing(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="relational")
    rows = _pg_values(plan)
    changed = list(rows[0])
    changed[-1] = COMMIT_B
    rows[0] = tuple(changed)
    report = reconcile_postgres(plan, FakePostgresReader(rows, plan.table))
    assert not report.passed
    assert report.missing_ids == (str(plan.records[0]["record_id"]),)


def test_mongo_detects_missing_extra_and_nested_drift(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="document")
    documents = copy.deepcopy(list(plan.records))
    removed = documents.pop(0)
    changed = documents[0]
    changed["data"]["weekly_pay_gbp"] = -999.0
    extra = copy.deepcopy(documents[-1])
    extra["_id"] = "f" * 64
    extra["data"]["new_metric"] = 42
    report = reconcile_mongodb(plan, FakeMongoReader([*documents, extra]))
    assert not report.passed
    assert report.missing_ids == (str(removed["_id"]),)
    assert report.unexpected_ids == ("f" * 64,)
    assert report.changed_fields == {str(changed["_id"]): ("data.weekly_pay_gbp",)}
    assert report.expected_sink_sha256 != report.observed_sink_sha256


def test_mongo_changed_lineage_filters_out_entire_document(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["vintage"]["registry_commit"] = COMMIT_B
    report = reconcile_mongodb(plan, FakeMongoReader(documents))
    assert not report.passed
    assert report.observed_count == plan.source_rows - 1
    assert report.missing_ids == (str(plan.records[0]["_id"]),)


def test_mongo_additional_fields_are_reported_not_ignored(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["new_field"] = ["unapproved", 42]
    report = reconcile_mongodb(plan, FakeMongoReader(documents))
    assert not report.passed
    assert report.changed_fields == {str(plan.records[0]["_id"]): ("new_field",)}


def test_mongo_numeric_type_change_is_content_drift(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["data"]["weekly_pay_gbp"] = 600
    report = reconcile_mongodb(plan, FakeMongoReader(documents))
    assert not report.passed
    assert "data.weekly_pay_gbp" in next(iter(report.changed_fields.values()))


def test_postgres_accepts_safe_int_to_double_cast_without_false_drift(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="relational")
    records = copy.deepcopy(plan.records)
    original = records[0]["weekly_pay_gbp"]
    assert type(original) is float and original.is_integer()
    records[0]["weekly_pay_gbp"] = int(original)
    cast_plan = replace(
        plan,
        records=records,
        records_sha256=hashlib.sha256(_canonical_json(records)).hexdigest(),
    )
    report = reconcile_postgres(cast_plan, FakePostgresReader(_pg_values(plan), plan.table))
    assert report.passed
    assert cast_plan.records_sha256 != report.expected_sink_sha256


def test_postgres_detects_lossy_large_integer_to_double_cast(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="relational")
    records = copy.deepcopy(plan.records)
    records[0]["weekly_pay_gbp"] = 2**53 + 1
    large_plan = replace(
        plan,
        records=records,
        records_sha256=hashlib.sha256(_canonical_json(records)).hexdigest(),
    )
    report = reconcile_postgres(large_plan, FakePostgresReader(_pg_values(large_plan), plan.table))
    assert not report.passed
    assert report.changed_fields == {str(records[0]["record_id"]): ("weekly_pay_gbp",)}


def test_report_digest_distinguishes_signed_zero_from_positive_zero(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="relational")
    records = copy.deepcopy(plan.records)
    records[0]["min_area_weekly_pay_gbp"] = 0.0
    zero_plan = replace(
        plan,
        records=records,
        records_sha256=hashlib.sha256(_canonical_json(records)).hexdigest(),
    )
    rows = _pg_values(zero_plan)
    names = ("record_id", "model_version", *_TABLES[plan.table].columns)
    current = list(rows[0])
    current[names.index("min_area_weekly_pay_gbp")] = -0.0
    rows[0] = tuple(current)
    report = reconcile_postgres(zero_plan, FakePostgresReader(rows, plan.table))
    assert not report.passed
    assert "min_area_weekly_pay_gbp" in next(iter(report.changed_fields.values()))


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_invalid_source_plan_fails_before_any_sink_read(
    curated: tuple[Path, VintageKey], sink: str
) -> None:
    shape: SinkShape = "relational" if sink == "postgresql" else "document"
    plan = _plan(curated, table="geography_year", shape=shape)
    tampered = replace(plan, records_sha256="0" * 64)
    sql = FakePostgresReader(_pg_values(plan) if shape == "relational" else [], plan.table)
    mongo = FakeMongoReader(copy.deepcopy(list(plan.records)) if shape == "document" else [])
    with pytest.raises(PublishPlanError, match="fingerprint"):
        if sink == "postgresql":
            reconcile_postgres(tampered, sql)
        else:
            reconcile_mongodb(tampered, mongo)
    assert not sql.calls and not mongo.calls


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_wrong_sink_shape_fails_before_any_database_read(
    curated: tuple[Path, VintageKey], sink: str
) -> None:
    plan = _plan(curated, table="geography_year", shape="document")
    if sink == "mongodb":
        plan = _plan(curated, table="geography_year", shape="relational")
    with pytest.raises(PublishPlanError, match="sink requires"):
        if sink == "postgresql":
            reconcile_postgres(plan, FakePostgresReader([], plan.table))
        else:
            reconcile_mongodb(plan, FakeMongoReader([]))


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_unavailable_backend_produces_a_safe_reconciliation_error(
    curated: tuple[Path, VintageKey], sink: str
) -> None:
    shape: SinkShape = "relational" if sink == "postgresql" else "document"
    plan = _plan(curated, table="geography_year", shape=shape)
    with pytest.raises(ReconciliationError, match=r"reconciliation .* failed"):
        if sink == "postgresql":
            reconcile_postgres(plan, FakePostgresReader([], plan.table, fail=True))
        else:
            reconcile_mongodb(plan, FakeMongoReader([], fail=True))


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_duplicate_sink_record_ids_are_rejected(
    curated: tuple[Path, VintageKey], sink: str
) -> None:
    shape: SinkShape = "relational" if sink == "postgresql" else "document"
    plan = _plan(curated, table="geography_year", shape=shape)
    with pytest.raises(ReconciliationError, match="duplicate record IDs"):
        if sink == "postgresql":
            rows = _pg_values(plan)
            reconcile_postgres(plan, FakePostgresReader([*rows, rows[0]], plan.table))
        else:
            documents = copy.deepcopy(list(plan.records))
            reconcile_mongodb(plan, FakeMongoReader([*documents, documents[0]]))


def test_postgres_invalid_result_row_is_rejected(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="relational")
    rows = _pg_values(plan)
    rows[0] = (*rows[0][:3], *rows[0][4:])
    with pytest.raises(ReconciliationError, match="invalid result row"):
        reconcile_postgres(plan, FakePostgresReader(rows, plan.table))


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), object(), True])
def test_mongo_rejects_unsafe_sink_values(
    curated: tuple[Path, VintageKey], invalid: object
) -> None:
    plan = _plan(curated, table="geography_year", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["data"]["min_area_weekly_pay_gbp"] = invalid
    with pytest.raises(ReconciliationError, match=r"non-finite|unsafe|unsupported"):
        reconcile_mongodb(plan, FakeMongoReader(documents))


def test_mongo_rejects_non_string_mapping_keys(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["data"][9] = "untyped field"
    with pytest.raises(ReconciliationError, match="non-text keys"):
        reconcile_mongodb(plan, FakeMongoReader(documents))


def test_mongo_rejects_unexpected_native_date_values(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["data"]["reference_date"] = date(2022, 4, 1)
    with pytest.raises(ReconciliationError, match="unexpected date scalar"):
        reconcile_mongodb(plan, FakeMongoReader(documents))


def test_reconciliation_rejects_over_budget_document_vintage(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="document")
    # A buggy driver could return far more than a plan's expected population.
    # The adapter must stop at the fixed cap, before materializing a hash.
    duplicate = copy.deepcopy(plan.records[0])
    collection = FakeMongoReader([duplicate] * 100001)
    with pytest.raises(ReconciliationError, match="100000-row"):
        reconcile_mongodb(plan, collection)
    assert collection.cursor is not None and collection.cursor.maximum == 100001


def test_reconciliation_rejects_over_budget_sql_vintage(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, table="geography_year", shape="relational")
    duplicate = _pg_values(plan)[0]
    reader = FakePostgresReader([duplicate] * 100001, plan.table)
    with pytest.raises(ReconciliationError, match="100000-row"):
        reconcile_postgres(plan, reader)
    assert len(reader.calls) == 1
    assert reader.calls[0][1][-1] == 100001


def test_report_has_no_secrets_or_record_values(curated: tuple[Path, VintageKey]) -> None:
    plan = _plan(curated, table="local_authority_yoy", shape="document")
    documents = copy.deepcopy(list(plan.records))
    documents[0]["data"]["area_name"] = "confidential altered value"
    report = reconcile_mongodb(plan, FakeMongoReader(documents))
    assert isinstance(report, ReconciliationReport)
    output = json.dumps(report.to_dict())
    assert "data.area_name" in output
    assert "confidential altered value" not in output
    assert report.changed_fields == {str(plan.records[0]["_id"]): ("data.area_name",)}
