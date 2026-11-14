"""Explicit, transactional migrations and read-only readiness for curated PostgreSQL."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from importlib.resources import files
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from psycopg import Connection

_MIGRATIONS = ((1, "0001_curated_dataset_service.sql"),)
_BOOTSTRAP = """
CREATE SCHEMA IF NOT EXISTS dpl_curated;
CREATE TABLE IF NOT EXISTS dpl_curated.schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  sha256 CHAR(64) NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""
_ROLES = ("dpl_curated_reader", "dpl_curated_writer")
_RELATIONS = (
    "public.ons_pay_geography_year",
    "public.ons_pay_local_authority_yoy",
    "dpl_curated.publications",
    "dpl_curated.schema_migrations",
)
_PUBLICATION_COLUMNS = (
    ("evidence_sha256", "character(64)"),
    ("attempt_id", "uuid"),
    ("dataset_id", "text"),
    ("table_name", "text"),
    ("target_id", "text"),
    ("registry_commit", "character(40)"),
    ("source_sha256", "character(64)"),
    ("model_version", "integer"),
    ("records_accepted", "bigint"),
    ("source_plan_sha256", "character(64)"),
    ("sink_snapshot_sha256", "character(64)"),
    ("published_at", "timestamp with time zone"),
    ("evidence_path", "text"),
)


class PostgresServiceError(RuntimeError):
    """The schema, migration history or privilege boundary is not safe to use."""


@dataclass(frozen=True, slots=True)
class Migration:
    """An immutable numbered SQL artifact with a platform-independent digest."""

    version: int
    name: str
    sql: str

    @property
    def sha256(self) -> str:
        """Bind the ledger to canonical LF SQL bytes, including bootstrap DDL."""
        return hashlib.sha256((_BOOTSTRAP + self.sql).encode("utf-8")).hexdigest()

    def describe(self) -> dict[str, object]:
        """Return metadata without database settings or SQL parameters."""
        return {"version": self.version, "name": self.name, "sha256": self.sha256}


def migrations() -> tuple[Migration, ...]:
    """Read packaged migration SQL without connecting or creating files."""
    directory = files("data_platform_lab.serving").joinpath("sql")
    return tuple(
        Migration(
            version,
            name,
            directory.joinpath(name).read_text(encoding="utf-8").replace("\r\n", "\n"),
        )
        for version, name in _MIGRATIONS
    )


def preview_service() -> dict[str, object]:
    """Describe the additive migration plan without reading connection settings."""
    return {
        "operation": "preview",
        "writes": False,
        "migrations": [migration.describe() for migration in migrations()],
        "roles": list(_ROLES),
        "relations": list(_RELATIONS),
    }


def _require_idle(connection: Connection[Any]) -> None:
    """Do not commit or roll back an injected caller's pre-existing transaction."""
    if connection.autocommit or connection.info.transaction_status != 0:
        raise PostgresServiceError("An idle connection with autocommit=False is required.")


def _configure(connection: Connection[Any], *, readonly: bool) -> None:
    """Bound migration locks/statements and avoid user-controlled schema resolution."""
    connection.execute("SET TRANSACTION READ ONLY" if readonly else "SET TRANSACTION READ WRITE")
    connection.execute("SET LOCAL search_path = pg_catalog, public")
    connection.execute("SET LOCAL statement_timeout = '30000ms'")
    connection.execute("SET LOCAL lock_timeout = '5000ms'")


def _ledger(connection: Connection[Any]) -> list[tuple[Any, ...]]:
    """Bound reads and reject unknown or edited migration history."""
    return list(
        connection.execute(
            "SELECT version, name, sha256 FROM dpl_curated.schema_migrations "
            "ORDER BY version LIMIT %s",
            (len(_MIGRATIONS) + 1,),
        ).fetchall()
    )


def _expected_history() -> list[tuple[object, ...]]:
    return [(item.version, item.name, item.sha256) for item in migrations()]


def _columns(connection: Connection[Any], relation: str) -> list[tuple[Any, ...]]:
    return list(
        connection.execute(
            "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull "
            "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "WHERE a.attrelid = pg_catalog.to_regclass(%s) AND a.attnum > 0 "
            "AND NOT a.attisdropped AND c.relkind = 'r' AND c.relpersistence = 'p' "
            "AND NOT c.relrowsecurity ORDER BY a.attnum",
            (relation,),
        ).fetchall()
    )


def _schemas_match(connection: Connection[Any]) -> bool:
    """Refuse incompatible pre-existing tables instead of silently accepting IF NOT EXISTS."""
    # Keep CLI help and metadata-only previews independent of optional analytics imports.
    from data_platform_lab.serving.publish_plan import _NULLABLE_FIELDS, _TABLES
    from data_platform_lab.serving.publish_sinks import _SQL_TYPES

    for table, contract in _TABLES.items():
        expected = [
            (
                name,
                _SQL_TYPES[name].lower().replace("char(", "character("),
                name not in _NULLABLE_FIELDS,
            )
            for name in ("record_id", "model_version", *contract.columns)
        ]
        relation = f"public.ons_pay_{table}"
        if _columns(connection, relation) != expected:
            return False
        if not _primary_key(connection, relation):
            return False
    return (
        _columns(connection, "dpl_curated.publications")
        == [(name, kind, True) for name, kind in _PUBLICATION_COLUMNS]
        and _primary_key(connection, "dpl_curated.publications")
        and _columns(connection, "dpl_curated.schema_migrations")
        == [
            ("version", "integer", True),
            ("name", "text", True),
            ("sha256", "character(64)", True),
            ("applied_at", "timestamp with time zone", True),
        ]
        and _primary_key(connection, "dpl_curated.schema_migrations")
    )


def _primary_key(connection: Connection[Any], relation: str) -> bool:
    """All managed relations use their first column as the sole primary key."""
    return connection.execute(
        "SELECT count(*) FROM pg_catalog.pg_constraint "
        "WHERE conrelid = pg_catalog.to_regclass(%s) AND contype = 'p' "
        "AND conkey = ARRAY[1]::smallint[]",
        (relation,),
    ).fetchone() == (1,)


def _roles_safe(connection: Connection[Any]) -> bool:
    """Group roles must not log in, elevate privileges or inherit another role."""
    roles = connection.execute(
        "SELECT rolname FROM pg_catalog.pg_roles r WHERE rolname = ANY(%s) "
        "AND NOT (rolcanlogin OR rolsuper OR rolcreatedb OR rolcreaterole "
        "OR rolreplication OR rolbypassrls) "
        "AND NOT EXISTS (SELECT FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid) "
        "ORDER BY rolname",
        (list(_ROLES),),
    ).fetchall()
    return roles == [(role,) for role in _ROLES]


def _grants_safe(connection: Connection[Any]) -> bool:
    """Check effective service privileges, including unwanted PUBLIC grants."""
    for relation in _RELATIONS:
        row = connection.execute(
            "SELECT has_table_privilege('dpl_curated_reader', %s, 'SELECT'), "
            "has_table_privilege('dpl_curated_reader', %s, "
            "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'), "
            "has_any_column_privilege('dpl_curated_reader', %s, 'INSERT,UPDATE,REFERENCES')",
            (relation, relation, relation),
        ).fetchone()
        if row != (True, False, False):
            return False
    for schema in ("public", "dpl_curated"):
        if connection.execute(
            "SELECT has_schema_privilege('dpl_curated_reader', %s, 'USAGE'), "
            "has_schema_privilege('dpl_curated_reader', %s, 'CREATE')",
            (schema, schema),
        ).fetchone() != (True, False):
            return False
    for relation, privileges in (
        *[(relation, ("SELECT", "INSERT", "UPDATE")) for relation in _RELATIONS[:2]],
        ("dpl_curated.publications", ("SELECT", "INSERT")),
        ("dpl_curated.schema_migrations", ("SELECT",)),
    ):
        for privilege in privileges:
            if connection.execute(
                "SELECT has_table_privilege('dpl_curated_writer', %s, %s)",
                (relation, privilege),
            ).fetchone() != (True,):
                return False
    return connection.execute(
        "SELECT has_schema_privilege('dpl_curated_writer', 'public', 'CREATE'), "
        "has_schema_privilege('dpl_curated_writer', 'public', 'USAGE'), "
        "has_schema_privilege('dpl_curated_writer', 'dpl_curated', 'USAGE'), "
        "has_database_privilege('dpl_curated_reader', current_database(), 'CREATE'), "
        "has_table_privilege('dpl_curated_writer', 'dpl_curated.schema_migrations', "
        "'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'), "
        "has_any_column_privilege('dpl_curated_writer', 'dpl_curated.schema_migrations', "
        "'INSERT,UPDATE,REFERENCES')"
    ).fetchone() == (True, True, True, False, False, False)


def _checks(connection: Connection[Any]) -> dict[str, bool]:
    present = connection.execute(
        "SELECT count(*) FROM unnest(%s::text[]) name "
        "WHERE pg_catalog.to_regclass(name) IS NOT NULL",
        (list(_RELATIONS),),
    ).fetchone() == (len(_RELATIONS),)
    roles = _roles_safe(connection)
    return {
        "relations_present": present,
        "roles_safe": roles,
        "migration_history_matches": present and _ledger(connection) == _expected_history(),
        "table_contracts_match": present and _schemas_match(connection),
        "privileges_match": present and roles and _grants_safe(connection),
    }


def verify_service(connection: Connection[Any]) -> dict[str, object]:
    """Inspect readiness in a read-only transaction; never create or repair objects."""
    _require_idle(connection)
    try:
        _configure(connection, readonly=True)
        checks = _checks(connection)
        return {
            "operation": "verify",
            "writes": False,
            "status": "ready" if all(checks.values()) else "not_ready",
            "checks": checks,
        }
    finally:
        connection.rollback()


def apply_migrations(connection: Connection[Any]) -> dict[str, object]:
    """Apply known pending migrations atomically, preserving rows and existing metadata.

    The caller explicitly opts into this operation and owns the idle connection.
    A nonblocking advisory transaction lock prevents concurrent migration runners.
    Schema, role or history drift rolls back the entire application.
    """
    _require_idle(connection)
    try:
        _configure(connection, readonly=False)
        if connection.execute("SELECT pg_try_advisory_xact_lock(1685089329, 1)").fetchone() != (
            True,
        ):
            raise PostgresServiceError("Another dataset migration is running.")
        connection.execute(_BOOTSTRAP)
        history = _ledger(connection)
        if history != _expected_history()[: len(history)]:
            raise PostgresServiceError("Migration history is unknown or has changed.")
        applied = []
        for item in migrations()[len(history) :]:
            connection.execute(item.sql)
            connection.execute(
                "INSERT INTO dpl_curated.schema_migrations (version, name, sha256) "
                "VALUES (%s, %s, %s)",
                (item.version, item.name, item.sha256),
            )
            applied.append(item.version)
        checks = _checks(connection)
        if not all(checks.values()):
            raise PostgresServiceError("Dataset schema or roles require operator inspection.")
        connection.commit()
        return {
            "operation": "apply",
            "writes": bool(applied),
            "status": "ready",
            "applied": applied,
            "checks": checks,
        }
    except Exception:
        connection.rollback()
        raise
