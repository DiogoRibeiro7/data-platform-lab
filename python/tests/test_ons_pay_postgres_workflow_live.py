"""Opt-in synthetic source-to-PostgreSQL tests in a fixture-owned persistent service."""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any

import pytest
from psycopg.conninfo import make_conninfo
from test_ons_pay_postgres_workflow import _snapshot, arguments
from test_ons_pay_refresh import _source
from test_postgres_dataset_service_live import _connect, _create_login
from test_postgres_dataset_service_live import service_target as service_target

from data_platform_lab.orchestration.ons_pay_postgres_cli import TABLES, main
from data_platform_lab.serving.postgres_service import apply_migrations
from data_platform_lab.serving.publish_cli import run_publication
from data_platform_lab.serving.publish_evidence import PublicationArchive

pytestmark = pytest.mark.skipif(
    os.environ.get("DPL_POSTGRES_SERVICE_LIVE") != "1" or os.name != "posix",
    reason="explicit opt-in, local Docker Compose and POSIX evidence storage required",
)


def _setup(target: Any) -> tuple[str, str]:
    password = secrets.token_hex(32)
    with _connect(target.dsn) as admin:
        admin.execute((target.repository / "sql/platform/001_pipeline_runs.sql").read_text())
        admin.commit()
        apply_migrations(admin)
        for login, role in (("writer", "dpl_curated_writer"), ("reader", "dpl_curated_reader")):
            _create_login(admin, login, role, password)
    return (
        make_conninfo(target.dsn, user="writer", password=password),
        make_conninfo(target.dsn, user="reader", password=password),
    )


def _verify(result: dict[str, Any], root: Path, table: str, dsn: str) -> tuple[int, dict[str, Any]]:
    return run_publication(
        [
            "--data-root",
            str(root),
            "--model-version",
            "1",
            "--registry-commit",
            result["vintage"]["registry_commit"],
            "--source-sha256",
            result["vintage"]["source_sha256"],
            "--table",
            table,
            "--sink",
            "postgresql",
            "--target-id",
            result["target_id"],
            "--verify",
            "--baseline-attempt-id",
            result["attempts"][table],
        ],
        environ={"ONS_PAY_POSTGRES_DSN": dsn},
    )


def test_apply_replay_restart_and_independent_reader(
    tmp_path: Path, service_target: Any, monkeypatch: Any, capsys: Any
) -> None:
    writer, reader = _setup(service_target)
    # Exercise the same explicit --env-file path used with Neon, without
    # exposing the generated fixture password through CLI output.
    monkeypatch.delenv("ONS_PAY_POSTGRES_DSN", raising=False)
    monkeypatch.delenv("DATABASE_URL_UNPOOLED", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    env_file = tmp_path / ".env.local"
    env_file.write_text(f"DATABASE_URL_UNPOOLED={writer}\n", encoding="utf-8")
    source = _source(tmp_path)
    argv = [*arguments(tmp_path, source), "--env-file", str(env_file)]
    root = tmp_path / "curated"
    main([*argv, "--apply"])
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "published"
    assert [item["status"] for item in result["publications"]] == ["accepted", "accepted"]
    evidence_before = _snapshot(root / "manifests/ons_pay_publish")
    main([*argv, "--apply"])
    replayed = json.loads(capsys.readouterr().out)
    assert replayed["status"] == "published"
    assert all(item["evidence"]["replayed"] for item in replayed["publications"])
    assert _snapshot(root / "manifests/ons_pay_publish") == evidence_before
    with _connect(reader) as connection:
        assert connection.execute("SELECT count(*) FROM dpl_curated.publications").fetchone() == (
            2,
        )
    service_target.compose("restart", "postgres")
    service_target.compose("up", "-d", "--wait", "postgres")
    port = int(service_target.compose("port", "postgres", "5432").rsplit(":", 1)[1])
    reader = make_conninfo(reader, port=port)
    writer = make_conninfo(writer, port=port)
    monkeypatch.setenv("ONS_PAY_POSTGRES_DSN", writer)
    before = _snapshot(root)
    for table in TABLES:
        code, verified = _verify(result, root, table, reader)
        assert code == 0 and verified["status"] == "verified"
    assert _snapshot(root) == before
    with _connect(make_conninfo(service_target.dsn, port=port)) as admin:
        admin.execute(
            "UPDATE public.ons_pay_geography_year "
            "SET areas_total = areas_total + 1, areas_with_pay = areas_with_pay + 1"
        )
    assert _verify(result, root, TABLES[0], reader)[0] == 3
    assert _snapshot(root) == before
    # Historical evidence replay must not hide current sink drift.
    with pytest.raises(SystemExit):
        main([*argv, "--apply"])
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    assert _snapshot(root / "manifests/ons_pay_publish") == evidence_before


def test_partial_table_failure_never_indexes_a_batch(
    tmp_path: Path, service_target: Any, monkeypatch: Any, capsys: Any
) -> None:
    writer, reader = _setup(service_target)
    monkeypatch.setenv("ONS_PAY_POSTGRES_DSN", writer)
    with _connect(service_target.dsn) as admin:
        admin.execute(
            "CREATE FUNCTION public.reject_test_row() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN RAISE EXCEPTION 'test-secret-row-value'; END $$"
        )
        admin.execute(
            "CREATE TRIGGER reject_test BEFORE INSERT ON public.ons_pay_local_authority_yoy "
            "FOR EACH ROW EXECUTE FUNCTION public.reject_test_row()"
        )
    with pytest.raises(SystemExit):
        main([*arguments(tmp_path, _source(tmp_path)), "--apply"])
    output = capsys.readouterr().out
    assert "test-secret" not in output
    result = json.loads(output)
    assert result["status"] == "failed"
    assert [item["status"] for item in result["publications"]] == ["accepted", "failed"]
    history = PublicationArchive(tmp_path / "curated").history()
    assert sorted(item.to_dict()["outcome"] for item in history) == ["accepted", "failed"]
    with _connect(reader) as connection:
        assert connection.execute("SELECT count(*) FROM dpl_curated.publications").fetchone() == (
            0,
        )
        assert connection.execute(
            "SELECT count(*) FROM public.ons_pay_local_authority_yoy"
        ).fetchone() == (0,)


def test_conflicting_index_rolls_back_the_entire_registration(
    tmp_path: Path, service_target: Any, monkeypatch: Any, capsys: Any
) -> None:
    writer, reader = _setup(service_target)
    monkeypatch.setenv("ONS_PAY_POSTGRES_DSN", writer)
    argv = arguments(tmp_path, _source(tmp_path))
    main([*argv, "--apply"])
    capsys.readouterr()
    with _connect(service_target.dsn) as admin:
        admin.execute("DELETE FROM dpl_curated.publications WHERE table_name = 'geography_year'")
        admin.execute("UPDATE dpl_curated.publications SET evidence_path = 'conflicting-reference'")
    with pytest.raises(SystemExit):
        main([*argv, "--apply"])
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    with _connect(reader) as connection:
        assert connection.execute(
            "SELECT table_name, evidence_path FROM dpl_curated.publications"
        ).fetchall() == [("local_authority_yoy", "conflicting-reference")]
