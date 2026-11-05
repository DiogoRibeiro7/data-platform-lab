"""Offline contract and failure-injection tests for Gold database adapters."""

from __future__ import annotations

import copy
import hashlib
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from test_ons_pay_serving import COMMIT_A, COMMIT_B, _curate, _pin

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import (
    ProjectedRecord,
    PublishPlan,
    PublishPlanError,
    SinkShape,
    TableName,
    _canonical_json,
    plan_ons_pay_publishing,
    validate_publishing_plan,
)
from data_platform_lab.serving.publish_sinks import (
    PublishSinkError,
    postgres_schema_sql,
    postgres_upsert_sql,
    publish_mongodb,
    publish_postgres,
)


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, VintageKey]:
    """Build real Parquet Gold outputs and their exact successful receipt."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    return root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])


def _plan(
    curated: tuple[Path, VintageKey],
    *,
    shape: SinkShape,
    table: TableName = "local_authority_yoy",
) -> PublishPlan:
    """Project a test vintage into one validated target shape."""
    root, vintage = curated
    return plan_ons_pay_publishing(data_root=root, vintage=vintage, table=table, shape=shape)


class FakePgCursor:
    """Track bounded batched statements without a running SQL server."""

    def __init__(self, connection: FakePgConnection) -> None:
        self.connection = connection
        self.closed = False

    def executemany(self, query: str, params_seq: list[tuple[object, ...]]) -> None:
        if self.connection.fail_on == "upsert":
            raise OSError("upsert refused")
        self.connection.batches.append((query, list(params_seq)))
        self.connection.staged.extend(params_seq)

    def close(self) -> None:
        self.closed = True
        if self.connection.fail_on == "close":
            raise OSError("cursor close failed")


class FakePgConnection:
    """Simulate commit/rollback to verify transaction boundaries and replay."""

    def __init__(self, *, fail_on: str = "", autocommit: bool = False) -> None:
        self.fail_on = fail_on
        self.autocommit = autocommit
        self.queries: list[str] = []
        self.batches: list[tuple[str, list[tuple[object, ...]]]] = []
        self.staged: list[tuple[object, ...]] = []
        self.persisted: dict[object, tuple[object, ...]] = {}
        self.commits = 0
        self.rollbacks = 0
        self.cursors: list[FakePgCursor] = []

    def execute(self, query: str) -> None:
        self.queries.append(query)
        if self.fail_on == "ddl":
            raise OSError("schema unavailable")

    def cursor(self) -> FakePgCursor:
        if self.fail_on == "cursor":
            raise OSError("cursor unavailable")
        cursor = FakePgCursor(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self) -> None:
        self.commits += 1
        if self.fail_on == "commit":
            raise OSError("commit rejected")
        for values in self.staged:
            self.persisted[values[0]] = values
        self.staged.clear()

    def rollback(self) -> None:
        self.rollbacks += 1
        self.staged.clear()
        if self.fail_on == "rollback":
            raise OSError("rollback rejected")


class FakeReplaceResult:
    """Expose only the acknowledged result property of pymongo."""

    def __init__(self, acknowledged: bool = True) -> None:
        self.acknowledged = acknowledged


class FakeMongoCollection:
    """Persist documents by _id and simulate partial, retryable failures."""

    def __init__(self, *, fail_at: int = 0, acknowledged: bool = True) -> None:
        self.documents: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[dict[str, str], dict[str, object], bool]] = []
        self.fail_at = fail_at
        self.acknowledged = acknowledged

    def replace_one(
        self, filter: dict[str, str], replacement: dict[str, object], *, upsert: bool
    ) -> FakeReplaceResult:
        self.calls.append((copy.deepcopy(filter), copy.deepcopy(replacement), upsert))
        if self.fail_at and len(self.calls) == self.fail_at:
            raise OSError("remote interruption")
        if self.acknowledged:
            self.documents[filter["_id"]] = copy.deepcopy(replacement)
        return FakeReplaceResult(acknowledged=self.acknowledged)


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_sql_statements_use_pinned_table_names_and_bound_values(table: TableName) -> None:
    """DDL and DML must be fixed contracts, not caller SQL fragments."""
    ddl = postgres_schema_sql(table)
    upsert = postgres_upsert_sql(table)
    assert f"ons_pay_{table}" in ddl
    assert "PRIMARY KEY (record_id)" in ddl
    assert "record_id CHAR(64) NOT NULL" in ddl
    assert "source_sha256 CHAR(64) NOT NULL" in ddl
    assert "registry_commit CHAR(40) NOT NULL" in ddl
    assert "ON CONFLICT (record_id) DO UPDATE" in upsert
    assert upsert.count("%s") == (12 if table == "geography_year" else 13)
    if table == "local_authority_yoy":
        assert "reference_date DATE NOT NULL" in ddl
        assert "weekly_pay_gbp DOUBLE PRECISION," in ddl
    else:
        assert "CHECK (areas_total = areas_with_pay + areas_without_pay)" in ddl
    with pytest.raises(PublishPlanError, match="unsupported Gold table"):
        postgres_schema_sql("malicious; DROP TABLE data")  # type: ignore[arg-type]
    with pytest.raises(PublishPlanError, match="unsupported Gold table"):
        postgres_upsert_sql("other")  # type: ignore[arg-type]


def test_postgres_transactional_upserts_preserve_dates_nulls_and_lineage(
    curated: tuple[Path, VintageKey],
) -> None:
    """Execute a pinned row batch in bounded chunks and commit exactly once."""
    plan = _plan(curated, shape="relational")
    conn = FakePgConnection()
    receipt = publish_postgres(plan, conn, batch_size=2)
    assert receipt.sink == "postgresql"
    assert receipt.records_applied == 6
    assert receipt.records_sha256 == plan.records_sha256
    assert receipt.vintage == plan.vintage
    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert len(conn.queries) == 1
    assert [len(batch) for _, batch in conn.batches] == [2, 2, 2]
    assert all(cursor.closed for cursor in conn.cursors)
    rows = [values for _, batch in conn.batches for values in batch]
    assert all(
        values[0] == record["record_id"] for values, record in zip(rows, plan.records, strict=True)
    )
    assert all(values[1] == plan.vintage.model_version for values in rows)
    assert rows[0][4].isoformat() == "2022-04-01"
    assert rows[0][-2:] == (plan.vintage.source_sha256, plan.vintage.registry_commit)
    assert any(None in values for values in rows)


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_postgres_replaying_exact_plan_is_idempotent(
    curated: tuple[Path, VintageKey], table: TableName
) -> None:
    plan = _plan(curated, table=table, shape="relational")
    conn = FakePgConnection()
    publish_postgres(plan, conn)
    snapshot = dict(conn.persisted)
    publish_postgres(plan, conn)
    assert conn.persisted == snapshot
    assert len(conn.persisted) == plan.source_rows
    assert conn.commits == 2
    assert conn.rollbacks == 0


@pytest.mark.parametrize("stage", ["ddl", "cursor", "upsert", "close", "commit"])
def test_postgres_failures_always_attempt_rollback(
    curated: tuple[Path, VintageKey], stage: str
) -> None:
    plan = _plan(curated, shape="relational")
    conn = FakePgConnection(fail_on=stage)
    with pytest.raises(PublishSinkError, match="transaction rolled back"):
        publish_postgres(plan, conn)
    assert conn.rollbacks == 1
    assert not conn.staged
    assert not conn.persisted


def test_postgres_rollback_failure_is_reported(curated: tuple[Path, VintageKey]) -> None:
    plan = _plan(curated, shape="relational")
    conn = FakePgConnection(fail_on="rollback")

    def failed_cursor() -> FakePgCursor:
        raise OSError("query failure")

    with (
        patch.object(conn, "cursor", side_effect=failed_cursor),
        pytest.raises(PublishSinkError, match="rollback both failed"),
    ):
        publish_postgres(plan, conn)
    assert conn.rollbacks == 1


def test_postgres_autocommit_is_rejected_without_touching_connection(
    curated: tuple[Path, VintageKey],
) -> None:
    conn = FakePgConnection(autocommit=True)
    with pytest.raises(PublishSinkError, match="autocommit=False"):
        publish_postgres(_plan(curated, shape="relational"), conn)
    assert not conn.queries and not conn.batches and conn.commits == 0


@pytest.mark.parametrize("size", [0, -1, 5001, True, 1.5, "100"])
def test_invalid_postgres_chunk_sizes_fail_before_any_io(
    curated: tuple[Path, VintageKey], size: Any
) -> None:
    conn = FakePgConnection()
    with pytest.raises(PublishPlanError, match="batch_size"):
        publish_postgres(_plan(curated, shape="relational"), conn, batch_size=size)
    assert not conn.queries


def test_mongodb_replacement_preserves_nested_data_and_full_source_pins(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, shape="document")
    collection = FakeMongoCollection()
    receipt = publish_mongodb(plan, collection)
    assert receipt.sink == "mongodb"
    assert receipt.records_applied == 6
    assert receipt.records_sha256 == plan.records_sha256
    assert len(collection.documents) == 6
    assert len(collection.calls) == 6
    assert all(
        filter == {"_id": document["_id"]} and upsert
        for filter, document, upsert in collection.calls
    )
    assert all(
        document["vintage"]["registry_commit"] == COMMIT_A
        for document in collection.documents.values()
    )
    rows = [document["data"] for document in collection.documents.values()]
    assert any(row["weekly_pay_gbp"] is None and row["status_code"] == "x" for row in rows)
    assert all(
        document["dataset_id"] == plan.dataset_id for document in collection.documents.values()
    )


def test_mongodb_partial_failure_retries_without_duplication(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, shape="document")
    collection = FakeMongoCollection(fail_at=3)
    with pytest.raises(PublishSinkError, match="retry the exact same pinned plan"):
        publish_mongodb(plan, collection)
    assert len(collection.documents) == 2
    collection.fail_at = 0
    publish_mongodb(plan, collection)
    snapshot = copy.deepcopy(collection.documents)
    publish_mongodb(plan, collection)
    assert collection.documents == snapshot
    assert len(collection.documents) == plan.source_rows


def test_mongodb_unacknowledged_replacement_is_not_success(
    curated: tuple[Path, VintageKey],
) -> None:
    collection = FakeMongoCollection(acknowledged=False)
    with pytest.raises(PublishSinkError, match="incomplete"):
        publish_mongodb(_plan(curated, shape="document"), collection)
    assert len(collection.calls) == 1
    assert not collection.documents


def test_distinct_vintages_can_coexist_in_mongodb(tmp_path: Path) -> None:
    root, first_receipt = _curate(tmp_path, commit=COMMIT_A)
    _, second_receipt = _curate(tmp_path, commit=COMMIT_B)
    first_pins, second_pins = _pin(first_receipt), _pin(second_receipt)
    collection = FakeMongoCollection()
    for pins in (first_pins, second_pins):
        key = VintageKey(1, pins["registry_commit"], pins["source_sha256"])
        publish_mongodb(
            plan_ons_pay_publishing(
                data_root=root, vintage=key, table="geography_year", shape="document"
            ),
            collection,
        )
    assert len(collection.documents) == 10
    assert {
        document["vintage"]["registry_commit"] for document in collection.documents.values()
    } == {COMMIT_A, COMMIT_B}


def _altered(plan: PublishPlan, name: str, value: object) -> PublishPlan:
    """Produce one poisoned preview without changing the original fixture."""
    return replace(plan, **{name: value})


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("schema_version", 2),
        ("schema_version", True),
        ("dataset_id", "another-data-source"),
        ("source_rows", 99),
        ("source_rows", True),
        ("source_artifact_sha256", "bad"),
        ("records_sha256", "0" * 64),
        ("records", []),
    ],
)
def test_tampered_plan_metadata_fails_before_any_postgres_statement(
    curated: tuple[Path, VintageKey], name: str, value: object
) -> None:
    plan = _altered(_plan(curated, shape="relational"), name, value)
    conn = FakePgConnection()
    with pytest.raises(PublishPlanError, match=r"metadata or row count|fingerprint"):
        publish_postgres(plan, conn)
    assert not conn.queries


@pytest.mark.parametrize(
    ("shape", "wrong_sink"),
    [("relational", "mongodb"), ("document", "postgresql")],
)
def test_sink_shape_mismatch_fails_before_io(
    curated: tuple[Path, VintageKey], shape: SinkShape, wrong_sink: str
) -> None:
    plan = _plan(curated, shape=shape)
    conn = FakePgConnection()
    collection = FakeMongoCollection()
    with pytest.raises(PublishPlanError, match="sink requires"):
        if wrong_sink == "postgresql":
            publish_postgres(plan, conn)
        else:
            publish_mongodb(plan, collection)
    assert not conn.queries and not collection.calls


@pytest.mark.parametrize(
    ("shape", "tamper"),
    [
        ("relational", ("pay_year", True)),
        ("relational", ("area_code", "")),
        ("relational", ("reference_date", "2024-02-30")),
        ("relational", ("weekly_pay_gbp", float("inf"))),
        ("relational", ("registry_commit", COMMIT_B)),
        ("relational", ("record_id", "0" * 64)),
        ("document", ("_id", "0" * 64)),
        ("document", ("schema_version", True)),
    ],
)
def test_invalid_record_fails_closed_even_when_fingerprint_is_recomputed(
    curated: tuple[Path, VintageKey],
    shape: SinkShape,
    tamper: tuple[str, object],
) -> None:
    plan = _plan(curated, shape=shape)
    records = copy.deepcopy(plan.records)
    records[0][tamper[0]] = tamper[1]
    poisoned = replace(
        plan,
        records=records,
        records_sha256=hashlib.sha256(_canonical_json(records)).hexdigest()
        if tamper[1] != float("inf")
        else "0" * 64,
    )
    conn, collection = FakePgConnection(), FakeMongoCollection()
    with pytest.raises(PublishPlanError):
        if shape == "relational":
            publish_postgres(poisoned, conn)
        else:
            publish_mongodb(poisoned, collection)
    assert not conn.queries and not collection.calls


def test_nested_document_mutation_fails_even_if_digest_matches(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, shape="document")
    records = copy.deepcopy(plan.records)
    records[0]["data"]["previous_year"] = "not an integer"
    poisoned = replace(
        plan,
        records=records,
        records_sha256=hashlib.sha256(_canonical_json(records)).hexdigest(),
    )
    collection = FakeMongoCollection()
    with pytest.raises(PublishPlanError, match="unexpected type"):
        publish_mongodb(poisoned, collection)
    assert not collection.calls


def test_fingerprint_catches_mutation_after_plan_creation(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, shape="document")
    plan.records[0]["data"]["area_name"] = "Changed after preview"
    with pytest.raises(PublishPlanError, match="fingerprint"):
        publish_mongodb(plan, FakeMongoCollection())


def test_validation_returns_an_isolated_copy(curated: tuple[Path, VintageKey]) -> None:
    plan = _plan(curated, shape="document")
    copy_of_records = validate_publishing_plan(plan, expected_shape="document")
    assert copy_of_records == plan.records
    assert copy_of_records is not plan.records
    assert copy_of_records[0] is not plan.records[0]
    assert copy_of_records[0]["data"] is not plan.records[0]["data"]


def test_publish_does_not_reuse_mutated_source_records_after_validation(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, shape="document")
    original_validate = validate_publishing_plan

    def validated_then_changed(
        candidate: PublishPlan, *, expected_shape: SinkShape
    ) -> tuple[ProjectedRecord, ...]:
        snapshot = original_validate(candidate, expected_shape=expected_shape)
        candidate.records[0]["data"]["area_name"] = "mutated concurrently"
        return snapshot

    collection = FakeMongoCollection()
    with patch(
        "data_platform_lab.serving.publish_sinks.validate_publishing_plan",
        side_effect=validated_then_changed,
    ):
        publish_mongodb(plan, collection)
    assert all(
        document["data"]["area_name"] != "mutated concurrently"
        for document in collection.documents.values()
    )
