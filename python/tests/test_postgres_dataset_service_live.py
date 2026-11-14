"""Opt-in persistence proof in a fixture-owned Compose project and named volume."""

from __future__ import annotations

import os
import secrets
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.metadata import PostgresRunStore
from data_platform_lab.observability import RunMetadata
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.postgres_service import (
    PostgresServiceError,
    apply_migrations,
    migrations,
    verify_service,
)
from data_platform_lab.serving.publish_evidence import PublicationArchive
from data_platform_lab.serving.publish_gate import accept_publication
from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing
from data_platform_lab.serving.publish_reconcile import reconcile_postgres
from data_platform_lab.serving.publish_sinks import publish_postgres

pytestmark = pytest.mark.skipif(
    os.environ.get("DPL_POSTGRES_SERVICE_LIVE") != "1" or os.name != "posix",
    reason="explicit opt-in, local Docker Compose and POSIX evidence storage required",
)


@dataclass(repr=False)
class ServiceTarget:
    """Do not expose ephemeral connection credentials in pytest fixture diagnostics."""

    dsn: str
    compose: Any
    repository: Path


def _connect(dsn: str, stage: str = "setup") -> Any:
    """Hide generated test credentials even when a connection attempt fails."""
    __tracebackhide__ = True
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            return psycopg.connect(dsn, connect_timeout=2)
        except psycopg.OperationalError:
            time.sleep(0.25)
    pytest.fail(
        f"Cannot connect to the fixture-owned PostgreSQL service during {stage}.", pytrace=False
    )


def _create_login(admin: Any, login: str, role: str, password: str) -> None:
    """Do not expose a CREATE ROLE password in a failing driver's traceback."""
    __tracebackhide__ = True
    try:
        admin.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} IN ROLE {}").format(
                sql.Identifier(login), sql.Literal(password), sql.Identifier(role)
            )
        )
    except psycopg.Error:
        pytest.fail("Cannot create isolated test identities.", pytrace=False)


@pytest.fixture
def service_target(tmp_path: Path) -> Iterator[Any]:
    """Never accept a caller's DSN, Compose project or remote Docker connection."""
    repository = Path(__file__).resolve().parents[2]
    project = "dpl-pg-persistence-" + uuid4().hex
    password = secrets.token_hex(32)
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DOCKER_", "COMPOSE_", "DPL_PG_TEST_"))
    }
    environment["DPL_PG_TEST_PASSWORD"] = password
    base = [
        "docker",
        "--context",
        "default",
        "compose",
        "--env-file",
        os.devnull,
        "--project-name",
        project,
        "--file",
        str(repository / "infra/postgres-datasets-test/compose.yaml"),
    ]

    def compose(*arguments: str) -> str:
        result = subprocess.run(
            [*base, *arguments],
            env=environment,
            cwd=repository,
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode:
            pytest.fail("Isolated PostgreSQL Compose command failed.")
        return result.stdout.strip()

    try:
        compose("up", "-d", "--wait", "postgres")
        address = compose("port", "postgres", "5432")
        assert address.startswith("127.0.0.1:")
        dsn = make_conninfo(
            host=(
                "host.docker.internal"
                if os.environ.get("DPL_PG_TEST_CONTAINER_CLIENT") == "1"
                else "127.0.0.1"
            ),
            port=int(address.rsplit(":", 1)[1]),
            dbname="dpl_persistence_test",
            user="dpl_test_admin",
            password=password,
        )
        yield ServiceTarget(dsn, compose, repository)
    finally:
        # This random project owns only test-data; the persistent platform is never targeted.
        compose("down", "--volumes")


def test_published_vintages_roles_evidence_and_metadata_survive_restart(
    tmp_path: Path, service_target: Any
) -> None:
    admin_dsn = service_target.dsn
    compose = service_target.compose
    repository = service_target.repository
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    vintage = VintageKey(1, pins["registry_commit"], pins["source_sha256"])
    plans = [
        plan_ons_pay_publishing(data_root=root, vintage=vintage, table=table, shape="relational")
        for table in ("geography_year", "local_authority_yoy")
    ]
    run = RunMetadata(
        pipeline_name="persistent_service_test",
        run_id=uuid4().hex,
        status="success",
        started_at=datetime.now(UTC).isoformat(),
    )
    with _connect(admin_dsn) as admin:
        assert verify_service(admin)["status"] == "not_ready"
        admin.execute((repository / "sql/platform/001_pipeline_runs.sql").read_text())
        admin.commit()
        PostgresRunStore(admin).save(run)
        # Existing ONS tables and data must remain valid when adopting migrations.
        publish_postgres(plans[0], admin)
        assert apply_migrations(admin)["applied"] == [1]
        assert apply_migrations(admin)["applied"] == []
        assert verify_service(admin)["status"] == "ready"
        password = secrets.token_hex(32)
        for login, role in (
            ("test_writer", "dpl_curated_writer"),
            ("test_reader", "dpl_curated_reader"),
        ):
            _create_login(admin, login, role, password)
        admin.commit()
        before_start = admin.execute("SELECT pg_postmaster_start_time()").fetchone()
    writer_dsn = make_conninfo(admin_dsn, user="test_writer", password=password)
    reader_dsn = make_conninfo(admin_dsn, user="test_reader", password=password)
    archive = PublicationArchive(root)
    evidence = []
    for plan in plans:
        with _connect(writer_dsn, "publication") as writer:
            written = publish_postgres(plan, writer)
            assert publish_postgres(plan, writer) == written
            reconciled = reconcile_postgres(plan, writer)
            accepted = accept_publication(plan, written, reconciled)
            now = datetime.now(UTC)
            record = archive.record(
                plan,
                sink="postgresql",
                attempt_id=str(uuid4()),
                started_at=now,
                ended_at=now,
                receipt=written,
                report=reconciled,
                target_id="isolated-persistent-test",
            )
            evidence.append(record)
            # Fixture-only indexing of genuine gate-accepted evidence; no new acceptance path.
            writer.execute(
                "INSERT INTO dpl_curated.publications "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    record.record_sha256,
                    record.to_dict()["run_id"],
                    "ons-gross-median-weekly-pay",
                    plan.table,
                    "isolated-persistent-test",
                    vintage.registry_commit,
                    vintage.source_sha256,
                    1,
                    accepted.records_accepted,
                    accepted.source_plan_sha256,
                    accepted.sink_snapshot_sha256,
                    now,
                    str(archive.root / (record.record_sha256 + ".json")),
                ),
            )
    before_files = {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()
    }
    container_id = compose("ps", "-q", "postgres")
    compose("restart", "postgres")
    compose("up", "-d", "--wait", "postgres")
    assert compose("ps", "-q", "postgres") == container_id
    # Docker can reassign an ephemeral published port when restarting a container.
    address = compose("port", "postgres", "5432")
    assert address.startswith("127.0.0.1:")
    port = int(address.rsplit(":", 1)[1])
    admin_dsn = make_conninfo(admin_dsn, port=port)
    reader_dsn = make_conninfo(reader_dsn, port=port)
    with _connect(admin_dsn, "restart verification") as admin:
        assert admin.execute("SELECT pg_postmaster_start_time()").fetchone() != before_start
        loaded = PostgresRunStore(admin).get(run.pipeline_name, run.run_id)
        assert loaded is not None and loaded.status == "success"
        admin.rollback()
        assert apply_migrations(admin)["applied"] == []
        assert admin.execute("SELECT count(*) FROM dpl_curated.schema_migrations").fetchone() == (
            1,
        )
    with _connect(reader_dsn, "reader verification") as reader:
        assert verify_service(reader)["status"] == "ready"
        for plan, record in zip(plans, evidence, strict=True):
            assert reconcile_postgres(plan, reader).passed
            indexed = reader.execute(
                "SELECT sink_snapshot_sha256, records_accepted FROM dpl_curated.publications "
                "WHERE evidence_sha256=%s",
                (record.record_sha256,),
            ).fetchone()
            assert indexed == (
                record.to_dict()["acceptance"]["sink_snapshot_sha256"],
                plan.source_rows,
            )
            reader.rollback()
        for query in (
            "UPDATE public.ons_pay_geography_year SET geography = 'forbidden'",
            "DELETE FROM public.ons_pay_local_authority_yoy",
            "INSERT INTO dpl_curated.publications DEFAULT VALUES",
            "TRUNCATE public.ons_pay_geography_year",
            "CREATE TABLE public.reader_must_not_create (id integer)",
            "UPDATE public.pipeline_runs SET status = 'failed'",
            "UPDATE dpl_curated.schema_migrations SET sha256 = 'edited'",
            "SET ROLE dpl_curated_writer",
        ):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                reader.execute(query)
            reader.rollback()
    assert archive.history() == tuple(evidence)
    assert {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()
    } == before_files

    # Administrative drift must fail closed, without auto-repair or rewriting history.
    with _connect(admin_dsn) as admin:
        admin.execute("GRANT UPDATE ON public.ons_pay_geography_year TO dpl_curated_reader")
        admin.commit()
        assert verify_service(admin)["status"] == "not_ready"
        with pytest.raises(PostgresServiceError):
            apply_migrations(admin)
        admin.execute("REVOKE UPDATE ON public.ons_pay_geography_year FROM dpl_curated_reader")
        admin.execute(
            "GRANT UPDATE (geography) ON public.ons_pay_geography_year TO dpl_curated_reader"
        )
        admin.commit()
        assert verify_service(admin)["status"] == "not_ready"
        admin.execute(
            "REVOKE UPDATE (geography) ON public.ons_pay_geography_year FROM dpl_curated_reader"
        )
        admin.execute("UPDATE dpl_curated.schema_migrations SET sha256 = %s", ("0" * 64,))
        admin.commit()
        with pytest.raises(PostgresServiceError):
            apply_migrations(admin)
        admin.execute(
            "UPDATE dpl_curated.schema_migrations SET sha256 = %s", (migrations()[0].sha256,)
        )
        admin.commit()
        assert verify_service(admin)["status"] == "ready"


@pytest.mark.parametrize("conflict", ["table", "role", "metadata_key"])
def test_incompatible_existing_state_is_preserved_and_migration_rolls_back(
    service_target: ServiceTarget, conflict: str
) -> None:
    with _connect(service_target.dsn) as admin:
        if conflict == "table":
            admin.execute("CREATE TABLE public.ons_pay_geography_year (id INTEGER PRIMARY KEY)")
            admin.execute("INSERT INTO public.ons_pay_geography_year VALUES (42)")
        elif conflict == "role":
            admin.execute("CREATE ROLE dpl_curated_reader LOGIN")
        else:
            admin.execute("CREATE SCHEMA dpl_curated")
            definition = (
                migrations()[0]
                .sql.split("CREATE TABLE IF NOT EXISTS dpl_curated.publications", 1)[1]
                .split("CREATE INDEX", 1)[0]
            )
            admin.execute(
                ("CREATE TABLE dpl_curated.publications" + definition).replace(
                    "evidence_sha256 CHAR(64) PRIMARY KEY", "evidence_sha256 CHAR(64) NOT NULL"
                )
            )
        admin.commit()
        with pytest.raises(PostgresServiceError):
            apply_migrations(admin)
        assert admin.execute("SELECT to_regclass('dpl_curated.schema_migrations')").fetchone() == (
            None,
        )
        if conflict == "table":
            assert admin.execute("TABLE public.ons_pay_geography_year").fetchall() == [(42,)]
        elif conflict == "role":
            assert admin.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname='dpl_curated_reader'"
            ).fetchone() == (True,)
        else:
            assert admin.execute("SELECT count(*) FROM dpl_curated.publications").fetchone() == (0,)
