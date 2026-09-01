"""PostgreSQL implementation of the run-metadata persistence boundary."""

from __future__ import annotations

import json
from importlib import import_module
from typing import Any, Protocol, cast

from data_platform_lab.observability import RunMetadata


class Cursor(Protocol):
    """Minimal cursor operations required by the metadata store."""

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return one result row, if present."""

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all result rows."""


class Connection(Protocol):
    """Minimal database connection contract used by the adapter."""

    def execute(self, query: str, params: tuple[object, ...] = ()) -> Cursor:
        """Execute SQL and return a cursor-like result."""

    def commit(self) -> None:
        """Commit the current transaction."""

    def close(self) -> None:
        """Close the connection."""


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
        """Create a metadata store using an injected database connection."""
        self._connection = connection

    def save(self, metadata: RunMetadata) -> None:
        """Insert or replace one run snapshot by pipeline name and run ID."""
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
        self._connection.execute(
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
        self._connection.commit()

    def get(self, pipeline_name: str, run_id: str) -> RunMetadata | None:
        """Return one persisted run snapshot, if it exists."""
        query = (
            f"SELECT {', '.join(_COLUMNS)} FROM pipeline_runs WHERE pipeline_name=%s AND run_id=%s"
        )
        row = self._connection.execute(query, (pipeline_name, run_id)).fetchone()
        return self._to_metadata(row) if row is not None else None

    def list_recent(self, limit: int = 20) -> list[RunMetadata]:
        """Return the most recent persisted run snapshots."""
        if not isinstance(limit, int) or isinstance(limit, bool):
            raise TypeError("limit must be an integer")
        if limit < 1:
            raise ValueError("limit must be positive")

        query = (
            f"SELECT {', '.join(_COLUMNS)} FROM pipeline_runs "
            "ORDER BY started_at DESC NULLS LAST, recorded_at DESC LIMIT %s"
        )
        rows = self._connection.execute(query, (limit,)).fetchall()
        return [self._to_metadata(row) for row in rows]

    def close(self) -> None:
        """Close the underlying database connection."""
        self._connection.close()

    @classmethod
    def connect(cls, dsn: str) -> PostgresRunStore:
        """Create the adapter from a psycopg DSN."""
        try:
            psycopg = import_module("psycopg")
        except ModuleNotFoundError as exc:
            raise RuntimeError("psycopg is required for PostgreSQL metadata storage") from exc

        factory = getattr(psycopg, "connect", None)
        if not callable(factory):
            raise RuntimeError("psycopg installation does not expose connect()")
        return cls(cast(Connection, factory(dsn)))

    @staticmethod
    def _to_metadata(row: tuple[Any, ...]) -> RunMetadata:
        """Convert one database row back into the shared RunMetadata model."""
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
