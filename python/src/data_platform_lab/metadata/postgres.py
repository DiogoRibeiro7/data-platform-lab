"""PostgreSQL implementation of the run-metadata persistence boundary."""

from __future__ import annotations

import json
from importlib import import_module
from typing import Any, Protocol, cast

from dataexcept import (
    DatabaseConnectionError,
    DependencyError,
    QueryExecutionError,
    TransactionError,
)

from data_platform_lab.observability import RunMetadata


class Cursor(Protocol):
    def fetchone(self) -> tuple[Any, ...] | None: ...
    def fetchall(self) -> list[tuple[Any, ...]]: ...


class Connection(Protocol):
    def execute(self, query: str, params: tuple[object, ...] = ()) -> Cursor: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


_COLUMNS = (
    "pipeline_name",
    "run_id",
    "status",
    "started_at",
    "ended_at",
    "duration_seconds",
    "rows_read",
    "rows_written",
    "rows_rejected",
    "files_processed",
    "files_rejected",
    "warnings",
    "errors",
    "extra",
)


class PostgresRunStore:
    """Persist :class:`RunMetadata` snapshots in PostgreSQL."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    @staticmethod
    def _claim_key(pipeline_name: str, run_id: str) -> str:
        return f"{len(pipeline_name)}:{pipeline_name}{len(run_id)}:{run_id}"

    def _execute(self, query: str, params: tuple[object, ...] = ()) -> Cursor:
        try:
            return self._connection.execute(query, params)
        except Exception as exc:
            raise QueryExecutionError(query, exc) from exc

    def _fetchone(
        self,
        query: str,
        params: tuple[object, ...] = (),
    ) -> tuple[Any, ...] | None:
        try:
            return self._connection.execute(query, params).fetchone()
        except Exception as exc:
            raise QueryExecutionError(query, exc) from exc

    def _fetchall(
        self,
        query: str,
        params: tuple[object, ...] = (),
    ) -> list[tuple[Any, ...]]:
        try:
            return self._connection.execute(query, params).fetchall()
        except Exception as exc:
            raise QueryExecutionError(query, exc) from exc

    def _commit(self, transaction_id: str) -> None:
        try:
            self._connection.commit()
        except Exception as exc:
            raise TransactionError(transaction_id, f"Database commit failed: {exc}") from exc

    def _rollback(self, transaction_id: str) -> None:
        try:
            self._connection.rollback()
        except Exception as exc:
            raise TransactionError(transaction_id, f"Database rollback failed: {exc}") from exc

    def acquire_claim(self, pipeline_name: str, run_id: str) -> bool:
        query = "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))"
        row = self._fetchone(query, (self._claim_key(pipeline_name, run_id),))
        acquired = bool(row and row[0])
        self._commit(run_id)
        return acquired

    def release_claim(self, pipeline_name: str, run_id: str) -> None:
        self._rollback(run_id)
        query = "SELECT pg_advisory_unlock(hashtextextended(%s, 0))"
        self._execute(query, (self._claim_key(pipeline_name, run_id),))
        self._commit(run_id)

    def save(self, metadata: RunMetadata) -> None:
        query = """
        INSERT INTO pipeline_runs (
            pipeline_name, run_id, status, started_at, ended_at, duration_seconds,
            rows_read, rows_written, rows_rejected, files_processed, files_rejected,
            warnings, errors, extra
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb)
        ON CONFLICT (pipeline_name, run_id) DO UPDATE SET
            status=EXCLUDED.status,
            started_at=EXCLUDED.started_at,
            ended_at=EXCLUDED.ended_at,
            duration_seconds=EXCLUDED.duration_seconds,
            rows_read=EXCLUDED.rows_read,
            rows_written=EXCLUDED.rows_written,
            rows_rejected=EXCLUDED.rows_rejected,
            files_processed=EXCLUDED.files_processed,
            files_rejected=EXCLUDED.files_rejected,
            warnings=EXCLUDED.warnings,
            errors=EXCLUDED.errors,
            extra=EXCLUDED.extra,
            updated_at=now()
        """
        self._execute(
            query,
            (
                metadata.pipeline_name,
                metadata.run_id,
                metadata.status,
                metadata.started_at or None,
                metadata.ended_at,
                metadata.duration_seconds,
                metadata.rows_read,
                metadata.rows_written,
                metadata.rows_rejected,
                metadata.files_processed,
                metadata.files_rejected,
                json.dumps(metadata.warnings),
                json.dumps(metadata.errors),
                json.dumps(metadata.extra),
            ),
        )
        self._commit(metadata.run_id)

    def get(self, pipeline_name: str, run_id: str) -> RunMetadata | None:
        query = (
            f"SELECT {', '.join(_COLUMNS)} FROM pipeline_runs WHERE pipeline_name=%s AND run_id=%s"
        )
        row = self._fetchone(query, (pipeline_name, run_id))
        return self._to_metadata(row) if row is not None else None

    def list_recent(self, limit: int = 20) -> list[RunMetadata]:
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if limit < 1:
            raise ValueError("limit must be positive")
        query = (
            f"SELECT {', '.join(_COLUMNS)} FROM pipeline_runs "
            "ORDER BY started_at DESC NULLS LAST, recorded_at DESC LIMIT %s"
        )
        rows = self._fetchall(query, (limit,))
        return [self._to_metadata(row) for row in rows]

    def close(self) -> None:
        self._connection.close()

    @classmethod
    def connect(cls, dsn: str) -> PostgresRunStore:
        try:
            psycopg = import_module("psycopg")
        except ModuleNotFoundError as exc:
            raise DependencyError(
                "psycopg",
                "psycopg is required for PostgreSQL metadata storage",
            ) from exc
        factory = getattr(psycopg, "connect", None)
        if not callable(factory):
            raise DependencyError("psycopg", "psycopg installation does not expose connect()")
        try:
            connection = factory(dsn)
        except (TypeError, ValueError):
            raise
        except Exception as exc:
            raise DatabaseConnectionError(dsn, f"PostgreSQL connection failed: {exc}") from exc
        return cls(cast(Connection, connection))

    @staticmethod
    def _to_metadata(row: tuple[Any, ...]) -> RunMetadata:
        warnings = row[11] if isinstance(row[11], list) else json.loads(row[11])
        errors = row[12] if isinstance(row[12], list) else json.loads(row[12])
        extra = row[13] if isinstance(row[13], dict) else json.loads(row[13])
        started_at = row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3] or "")
        ended_at = (
            row[4].isoformat()
            if hasattr(row[4], "isoformat")
            else (str(row[4]) if row[4] else None)
        )
        return RunMetadata(
            pipeline_name=str(row[0]),
            run_id=str(row[1]),
            status=str(row[2]),
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=float(row[5]),
            rows_read=int(row[6]),
            rows_written=int(row[7]),
            rows_rejected=int(row[8]),
            files_processed=int(row[9]),
            files_rejected=int(row[10]),
            warnings=list(warnings),
            errors=list(errors),
            extra=dict(extra),
        )
