"""Tests for PostgreSQL run metadata persistence."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from dataexcept import (
    DatabaseConnectionError,
    DependencyError,
    QueryExecutionError,
    TransactionError,
)

from data_platform_lab.metadata import PostgresRunStore, RunStore
from data_platform_lab.observability import RunMetadata


class FakeCursor:
    def __init__(
        self,
        rows: list[tuple[Any, ...]],
        *,
        fetchone_error: Exception | None = None,
        fetchall_error: Exception | None = None,
    ) -> None:
        self.rows = rows
        self.fetchone_error = fetchone_error
        self.fetchall_error = fetchall_error

    def fetchone(self) -> tuple[Any, ...] | None:
        if self.fetchone_error is not None:
            raise self.fetchone_error
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        if self.fetchall_error is not None:
            raise self.fetchall_error
        return self.rows


class FakeConnection:
    def __init__(
        self,
        *,
        execute_error: Exception | None = None,
        commit_error: Exception | None = None,
        rollback_error: Exception | None = None,
        fetchone_error: Exception | None = None,
        fetchall_error: Exception | None = None,
    ) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.rows: list[tuple[Any, ...]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.execute_error = execute_error
        self.commit_error = commit_error
        self.rollback_error = rollback_error
        self.fetchone_error = fetchone_error
        self.fetchall_error = fetchall_error

    def execute(self, query: str, params: tuple[object, ...] = ()) -> FakeCursor:
        self.calls.append((query, params))
        if self.execute_error is not None:
            raise self.execute_error
        return FakeCursor(
            self.rows,
            fetchone_error=self.fetchone_error,
            fetchall_error=self.fetchall_error,
        )

    def commit(self) -> None:
        self.commits += 1
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollbacks += 1
        if self.rollback_error is not None:
            raise self.rollback_error

    def close(self) -> None:
        self.closed = True


def _metadata() -> RunMetadata:
    return RunMetadata(
        pipeline_name="demo",
        run_id="r1",
        status="success",
        started_at="2026-09-01T00:00:00+00:00",
        ended_at="2026-09-01T00:00:01+00:00",
        duration_seconds=1.0,
        rows_read=10,
        rows_written=9,
        rows_rejected=1,
        files_processed=1,
        extra={"source": "test"},
    )


def test_postgres_store_satisfies_contract_and_upserts() -> None:
    connection = FakeConnection()
    store = PostgresRunStore(connection)
    assert isinstance(store, RunStore)
    store.save(_metadata())
    assert connection.commits == 1
    assert "ON CONFLICT" in connection.calls[0][0]
    assert connection.calls[0][1][0:3] == ("demo", "r1", "success")


def test_postgres_store_get_and_list_recent() -> None:
    connection = FakeConnection()
    connection.rows = [
        (
            "demo",
            "r1",
            "success",
            "2026-09-01T00:00:00+00:00",
            "2026-09-01T00:00:01+00:00",
            1.0,
            10,
            9,
            1,
            1,
            0,
            [],
            [],
            {"source": "test"},
        )
    ]
    store = PostgresRunStore(connection)
    loaded = store.get("demo", "r1")
    assert loaded is not None
    assert loaded.rows_written == 9
    assert store.list_recent(5)[0].extra == {"source": "test"}


def test_postgres_store_classifies_query_failures() -> None:
    cause = OSError("database unavailable")
    store = PostgresRunStore(FakeConnection(execute_error=cause))

    with pytest.raises(QueryExecutionError, match="SELECT") as error:
        store.get("demo", "r1")

    assert error.value.original is cause
    assert error.value.__cause__ is cause


def test_postgres_store_classifies_fetch_failures() -> None:
    cause = OSError("cursor fetch failed")
    store = PostgresRunStore(FakeConnection(fetchone_error=cause))

    with pytest.raises(QueryExecutionError, match="SELECT") as error:
        store.get("demo", "r1")

    assert error.value.original is cause
    assert error.value.__cause__ is cause


def test_postgres_store_classifies_commit_failures() -> None:
    cause = OSError("commit rejected")
    store = PostgresRunStore(FakeConnection(commit_error=cause))

    with pytest.raises(TransactionError, match="commit rejected") as error:
        store.save(_metadata())

    assert error.value.__cause__ is cause
    assert error.value.transaction_id == "r1"


def test_postgres_store_classifies_rollback_failures() -> None:
    cause = OSError("rollback rejected")
    store = PostgresRunStore(FakeConnection(rollback_error=cause))

    with pytest.raises(TransactionError, match="rollback rejected") as error:
        store.release_claim("demo", "r1")

    assert error.value.__cause__ is cause


def test_postgres_connect_classifies_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing_module(_name: str) -> object:
        raise ModuleNotFoundError("psycopg")

    monkeypatch.setattr("data_platform_lab.metadata.postgres.import_module", missing_module)

    with pytest.raises(DependencyError, match="psycopg"):
        PostgresRunStore.connect("postgresql://localhost/db")


def test_postgres_connect_classifies_connection_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    cause = OSError("connection refused")

    def failing_connect(_dsn: str) -> object:
        raise cause

    monkeypatch.setattr(
        "data_platform_lab.metadata.postgres.import_module",
        lambda _name: SimpleNamespace(connect=failing_connect),
    )

    with pytest.raises(DatabaseConnectionError, match="connection refused") as error:
        PostgresRunStore.connect("postgresql://user:secret@localhost/db")

    assert error.value.__cause__ is cause
    assert "secret" not in str(error.value)


def test_postgres_connect_preserves_invalid_dsn_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def invalid_connect(_dsn: str) -> object:
        raise ValueError("invalid DSN")

    monkeypatch.setattr(
        "data_platform_lab.metadata.postgres.import_module",
        lambda _name: SimpleNamespace(connect=invalid_connect),
    )

    with pytest.raises(ValueError, match="invalid DSN"):
        PostgresRunStore.connect("not-a-dsn")


def test_postgres_store_acquires_and_releases_advisory_claim() -> None:
    connection = FakeConnection()
    connection.rows = [(True,)]
    store = PostgresRunStore(connection)
    assert store.acquire_claim("broker_to_iceberg", "events:0:7") is True
    store.release_claim("broker_to_iceberg", "events:0:7")
    acquire_query, acquire_params = connection.calls[0]
    release_query, release_params = connection.calls[1]
    assert "pg_try_advisory_lock" in acquire_query
    assert "pg_advisory_unlock" in release_query
    assert acquire_params == ("17:broker_to_iceberg10:events:0:7",)
    assert release_params == acquire_params
    assert connection.rollbacks == 1
    assert connection.commits == 2


def test_advisory_claim_key_is_unambiguous_when_values_contain_colons() -> None:
    assert PostgresRunStore._claim_key("a:b", "c") != PostgresRunStore._claim_key("a", "b:c")
