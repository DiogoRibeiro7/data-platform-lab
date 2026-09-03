"""Tests for PostgreSQL run metadata persistence."""

from __future__ import annotations

from typing import Any

from data_platform_lab.metadata import PostgresRunStore, RunStore
from data_platform_lab.observability import RunMetadata


class FakeCursor:
    """Minimal cursor used by metadata-store unit tests."""

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


class FakeConnection:
    """In-memory connection that records SQL calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.rows: list[tuple[Any, ...]] = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def execute(self, query: str, params: tuple[object, ...] = ()) -> FakeCursor:
        self.calls.append((query, params))
        return FakeCursor(self.rows)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

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
