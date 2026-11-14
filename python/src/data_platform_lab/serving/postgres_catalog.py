"""Read-only PostgreSQL queries anchored to existing local publication evidence."""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import psycopg
from psycopg import Connection

from data_platform_lab.curation.ons_pay import DATASET_ID
from data_platform_lab.serving.catalog import (
    CatalogError,
    CuratedCatalog,
    CuratedVintage,
    QueryPage,
    VintageKey,
    VintageNotFound,
)
from data_platform_lab.serving.publish_evidence import PublicationArchive, validate_target_id
from data_platform_lab.serving.publish_plan import (
    _TABLES,
    PublishPlan,
    TableName,
    plan_ons_pay_publishing,
)
from data_platform_lab.serving.publish_reconcile import reconcile_postgres

_INDEX_COLUMNS = (
    "evidence_sha256,attempt_id,dataset_id,table_name,target_id,registry_commit,"
    "source_sha256,model_version,records_accepted,source_plan_sha256,"
    "sink_snapshot_sha256,published_at,evidence_path"
)
_MAX_PUBLICATIONS = 1000
_RELATIONS = (
    "public.ons_pay_geography_year",
    "public.ons_pay_local_authority_yoy",
    "dpl_curated.publications",
    "dpl_curated.schema_migrations",
)


@dataclass(frozen=True, slots=True)
class PublicationReference:
    attempt_id: str
    evidence_sha256: str
    accepted_at: str


@dataclass(frozen=True, slots=True)
class PublishedTable:
    table: TableName
    grain: tuple[str, ...]
    first_year: int | None
    last_year: int | None
    records_accepted: int
    source_plan_sha256: str
    sink_snapshot_sha256: str
    evidence: tuple[PublicationReference, ...]


@dataclass(frozen=True, slots=True)
class PublishedVintage(CuratedVintage):
    dataset_id: str
    target_id: str
    tables: tuple[PublishedTable, ...]


def _key(key: VintageKey) -> None:
    if (
        type(key.model_version) is not int
        or key.model_version != 1
        or not isinstance(key.registry_commit, str)
        or re.fullmatch(r"[0-9a-f]{40}", key.registry_commit) is None
        or not isinstance(key.source_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", key.source_sha256) is None
    ):
        raise CatalogError("Exact model, registry and source pins are required")


def _reader(connection: Connection[Any]) -> None:
    """Reject elevated/writer identities and effective writes to service relations."""
    identity = connection.execute(
        "SELECT NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls), "
        "pg_has_role(current_user, 'dpl_curated_reader', 'USAGE'), "
        "pg_has_role(current_user, 'dpl_curated_writer', 'MEMBER'), "
        "has_database_privilege(current_database(), 'CREATE'), "
        "has_schema_privilege('public', 'CREATE'), "
        "has_schema_privilege('dpl_curated', 'CREATE') "
        "FROM pg_catalog.pg_roles WHERE rolname = current_user"
    ).fetchone()
    if identity != (True, True, False, False, False, False):
        raise CatalogError("A dedicated least-privilege reader identity is required")
    for relation in _RELATIONS:
        permissions = connection.execute(
            "SELECT has_table_privilege(%s, 'SELECT'), "
            "has_table_privilege(%s, 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'), "
            "has_any_column_privilege(%s, 'INSERT,UPDATE,REFERENCES')",
            (relation, relation, relation),
        ).fetchone()
        if permissions != (True, False, False):
            raise CatalogError("Reader service privileges are invalid")


class PostgresCatalog(CuratedCatalog):
    """Keep Parquet provenance; serve accepted SQL rows with bounded snapshot reads.

    Construction is inert. Each operation owns one connection and rolls back its
    read-only transaction. Credentials must identify a separately configured reader.
    """

    def __init__(self, data_root: Path, *, dsn: str, target_id: str) -> None:
        super().__init__(data_root)
        if not isinstance(dsn, str) or not dsn.strip():
            raise CatalogError("A dedicated PostgreSQL reader DSN is required")
        validate_target_id(target_id)
        self._dsn = dsn
        self.target_id = target_id
        self._local = CuratedCatalog(data_root)

    @contextmanager
    def _session(self) -> Iterator[Connection[Any]]:
        try:
            with psycopg.connect(self._dsn, autocommit=False, connect_timeout=10) as connection:
                try:
                    connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    connection.execute("SET LOCAL search_path = pg_catalog, public")
                    connection.execute("SET LOCAL statement_timeout = '30000ms'")
                    connection.execute("SET LOCAL lock_timeout = '5000ms'")
                    _reader(connection)
                    yield connection
                finally:
                    connection.rollback()
        except (CatalogError, VintageNotFound):
            raise
        except Exception:
            # Driver/validation errors can carry credentials, paths or sensitive row values.
            raise CatalogError("PostgreSQL accepted data is unavailable or invalid") from None

    def _index(
        self,
        connection: Connection[Any],
        key: VintageKey | None = None,
    ) -> list[tuple[Any, ...]]:
        clauses = "dataset_id = %s AND target_id = %s"
        values: list[object] = [DATASET_ID, self.target_id]
        if key is not None:
            _key(key)
            clauses += " AND model_version = %s AND registry_commit = %s AND source_sha256 = %s"
            values.extend((key.model_version, key.registry_commit, key.source_sha256))
        rows = connection.execute(
            f"SELECT {_INDEX_COLUMNS} FROM dpl_curated.publications WHERE {clauses} "
            "ORDER BY model_version, registry_commit, source_sha256, table_name, evidence_sha256 "
            "LIMIT %s",
            (*values, _MAX_PUBLICATIONS + 1),
        ).fetchall()
        if len(rows) > _MAX_PUBLICATIONS:
            raise CatalogError("Publication discovery exceeds its 1000-reference bound")
        return rows

    def _accepted(
        self,
        key: VintageKey,
        rows: list[tuple[Any, ...]],
    ) -> tuple[PublishedVintage, dict[TableName, PublishPlan]]:
        """Match every indexed reference to an accepted baseline; never choose latest."""
        if not rows:
            raise VintageNotFound("Requested vintage has no indexed acceptance")
        if {row[3] for row in rows} != set(_TABLES):
            raise CatalogError("Both Gold tables must have accepted publication references")
        vintage = self._local.get_vintage(key)
        archive = PublicationArchive(self.data_root)
        plans: dict[TableName, PublishPlan] = {}
        tables: list[PublishedTable] = []
        for table, contract in _TABLES.items():
            plan = plan_ons_pay_publishing(
                data_root=self.data_root,
                vintage=key,
                table=table,
                shape="relational",
            )
            plans[table] = plan
            references = []
            snapshot_sha256 = ""
            for row in rows:
                if row[3] != table:
                    continue
                evidence = archive.accepted_baseline(
                    plan,
                    sink="postgresql",
                    attempt_id=str(row[1]),
                    target_id=self.target_id,
                )
                record = evidence.to_dict()
                snapshot_sha256 = record["fingerprints"]["observed_sink_sha256"]
                expected = (
                    evidence.record_sha256,
                    record["run_id"],
                    DATASET_ID,
                    table,
                    self.target_id,
                    key.registry_commit,
                    key.source_sha256,
                    key.model_version,
                    plan.source_rows,
                    plan.records_sha256,
                    snapshot_sha256,
                    datetime.fromisoformat(record["ended_at"]),
                    (archive.root / f"{evidence.record_sha256}.json")
                    .relative_to(archive.data_root)
                    .as_posix(),
                )
                if (row[0], str(row[1]), *row[2:]) != expected:
                    raise CatalogError("Publication index contradicts immutable accepted evidence")
                references.append(
                    PublicationReference(record["run_id"], row[0], record["ended_at"])
                )
            # Equal accepted attempts are equivalent proofs, not an implicit latest selection.
            years = [cast(int, record["pay_year"]) for record in plan.records]
            tables.append(
                PublishedTable(
                    table,
                    contract.grain,
                    min(years, default=None),
                    max(years, default=None),
                    plan.source_rows,
                    plan.records_sha256,
                    snapshot_sha256,
                    tuple(references),
                )
            )
        return PublishedVintage(
            **{field.name: getattr(vintage, field.name) for field in fields(CuratedVintage)},
            dataset_id=DATASET_ID,
            target_id=self.target_id,
            tables=tuple(tables),
        ), plans

    def list_vintages(self) -> tuple[CuratedVintage, ...]:
        with self._session() as connection:
            grouped: dict[VintageKey, list[tuple[Any, ...]]] = {}
            for row in self._index(connection):
                key = VintageKey(row[7], row[5], row[6])
                _key(key)
                grouped.setdefault(key, []).append(row)
            return tuple(self._accepted(key, rows)[0] for key, rows in grouped.items())

    def get_vintage(self, key: VintageKey) -> CuratedVintage:
        _key(key)
        with self._session() as connection:
            return self._accepted(key, self._index(connection, key))[0]

    def _page(
        self,
        vintage: CuratedVintage,
        table: TableName,
        clauses: list[str],
        values: list[object],
        order: str,
        limit: int,
        offset: int,
    ) -> QueryPage:
        _key(vintage.key)
        if (
            type(limit) is not int
            or not 1 <= limit <= 100
            or type(offset) is not int
            or not 0 <= offset <= 100_000
        ):
            raise CatalogError("Invalid bounded pagination")
        with self._session() as connection:
            current, plans = self._accepted(vintage.key, self._index(connection, vintage.key))
            if current != vintage:
                raise CatalogError("Accepted metadata changed during the request")
            if not reconcile_postgres(plans[table], connection).passed:
                raise CatalogError("PostgreSQL vintage differs from its accepted baseline")
            key = vintage.key
            predicates = [
                "model_version = %s",
                "registry_commit = %s",
                "source_sha256 = %s",
                *clauses,
            ]
            parameters = [key.model_version, key.registry_commit, key.source_sha256, *values]
            where = " AND ".join(predicates)
            relation = f"public.ons_pay_{table}"
            count = connection.execute(
                f"SELECT count(*) FROM {relation} WHERE {where}", parameters
            ).fetchone()
            if count is None or type(count[0]) is not int:
                raise CatalogError("Cannot count accepted rows")
            rows = connection.execute(
                f"SELECT {', '.join(_TABLES[table].columns)} FROM {relation} WHERE {where} "
                f"ORDER BY {order}, record_id LIMIT %s OFFSET %s",
                (*parameters, limit, offset),
            ).fetchall()
            return QueryPage(
                count[0], tuple(dict(zip(_TABLES[table].columns, row, strict=True)) for row in rows)
            )

    @staticmethod
    def _year(value: int | None) -> None:
        if value is not None and (type(value) is not int or not 1900 <= value <= 2100):
            raise CatalogError("Invalid year filter")

    def geography_year(
        self,
        vintage: CuratedVintage,
        *,
        geography: str | None,
        pay_year: int | None,
        limit: int,
        offset: int,
    ) -> QueryPage:
        self._year(pay_year)
        if geography is not None and (type(geography) is not str or not 1 <= len(geography) <= 100):
            raise CatalogError("Invalid geography filter")
        clauses: list[str] = []
        values: list[object] = []
        if geography is not None:
            clauses.append("geography = %s")
            values.append(geography)
        if pay_year is not None:
            clauses.append("pay_year = %s")
            values.append(pay_year)
        return self._page(
            vintage, "geography_year", clauses, values, "geography, pay_year", limit, offset
        )

    def area_trend(
        self,
        vintage: CuratedVintage,
        *,
        area_code: str,
        start_year: int | None,
        end_year: int | None,
        limit: int,
        offset: int,
    ) -> QueryPage:
        if (
            type(area_code) is not str
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,15}", area_code) is None
        ):
            raise CatalogError("Invalid area code")
        self._year(start_year)
        self._year(end_year)
        if start_year is not None and end_year is not None and start_year > end_year:
            raise CatalogError("Invalid year range")
        clauses = ["area_code = %s"]
        values: list[object] = [area_code]
        if start_year is not None:
            clauses.append("pay_year >= %s")
            values.append(start_year)
        if end_year is not None:
            clauses.append("pay_year <= %s")
            values.append(end_year)
        return self._page(
            vintage, "local_authority_yoy", clauses, values, "pay_year, area_code", limit, offset
        )
