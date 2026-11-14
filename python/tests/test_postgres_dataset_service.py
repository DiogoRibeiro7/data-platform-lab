"""Offline migration planning, transaction ownership and credential-safe CLI tests."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.serving import postgres_service as service
from data_platform_lab.serving.postgres_service_cli import main
from data_platform_lab.serving.publish_sinks import postgres_schema_sql

SECRET = "postgresql://private:do-not-print@db.example/private"


def test_service_preview_does_not_import_database_or_analytics_clients() -> None:
    script = """
import importlib.abc, sys
class BlockClients(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'psycopg', 'pymongo', 'duckdb', 'fastapi'}:
            raise AssertionError('client imported during preview')
sys.meta_path.insert(0, BlockClients())
from data_platform_lab.cli.main import main
main(['postgres-service'])
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["writes"] is False


def _connection(history: list[Any] | None = None) -> MagicMock:
    connection = MagicMock(autocommit=False)
    connection.info.transaction_status = 0
    connection.execute.return_value.fetchone.return_value = (True,)
    connection.execute.return_value.fetchall.return_value = history or []
    return connection


def test_preview_reads_no_settings_driver_or_destination(tmp_path: Path, capsys: Any) -> None:
    with (
        patch("psycopg.connect", side_effect=AssertionError("database")),
        patch.object(Path, "mkdir", side_effect=AssertionError("directory")),
        patch("os.environ", {}),
    ):
        platform_main(["postgres-service"])
    report = json.loads(capsys.readouterr().out)
    assert report["operation"] == "preview" and report["writes"] is False
    assert report["migrations"] == [item.describe() for item in service.migrations()]
    assert not list(tmp_path.iterdir())


def test_numbered_sql_preserves_existing_ons_contracts_and_has_no_data_reset() -> None:
    (migration,) = service.migrations()
    for table in ("geography_year", "local_authority_yoy"):
        expected = postgres_schema_sql(table).replace("EXISTS ons_pay_", "EXISTS public.ons_pay_")
        assert expected + ";" in migration.sql
    assert "DROP " not in migration.sql.upper() and "TRUNCATE " not in migration.sql.upper()
    assert "pipeline_runs" not in migration.sql.split("-- The writer")[0]
    assert migration.sha256 == service.migrations()[0].sha256 and len(migration.sha256) == 64


@pytest.mark.parametrize("history", [[], "applied"])
def test_apply_records_only_pending_migrations_and_commits_once(history: Any) -> None:
    connection = _connection(service._expected_history() if history else [])
    with patch.object(service, "_checks", return_value={"ready": True}):
        report = service.apply_migrations(connection)
    assert report["applied"] == ([] if history else [1])
    assert report["writes"] is (not bool(history))
    connection.commit.assert_called_once()
    connection.rollback.assert_not_called()
    inserts = [
        call.args for call in connection.execute.call_args_list if call.args[0].startswith("INSERT")
    ]
    assert inserts == (
        []
        if history
        else [
            (
                "INSERT INTO dpl_curated.schema_migrations (version, name, sha256) "
                "VALUES (%s, %s, %s)",
                service._expected_history()[0],
            )
        ]
    )


@pytest.mark.parametrize("history", [[(1, "edited", "0" * 64)], [(99, "future", "f" * 64)]])
def test_unknown_or_changed_history_rolls_back_without_applying_sql(history: Any) -> None:
    connection = _connection(history)
    with pytest.raises(service.PostgresServiceError, match="history"):
        service.apply_migrations(connection)
    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()
    assert service.migrations()[0].sql not in [
        call.args[0] for call in connection.execute.call_args_list
    ]


def test_busy_migration_does_not_create_schema() -> None:
    connection = _connection()
    connection.execute.return_value.fetchone.return_value = (False,)
    with pytest.raises(service.PostgresServiceError, match="running"):
        service.apply_migrations(connection)
    assert service._BOOTSTRAP not in [call.args[0] for call in connection.execute.call_args_list]
    connection.rollback.assert_called_once()


@pytest.mark.parametrize("failure", ["sql", "insert", "checks", "commit"])
def test_partial_application_or_failed_readiness_never_returns_success(failure: str) -> None:
    connection = _connection()
    ordinary = connection.execute.return_value

    def execute(query: str, *args: Any) -> Any:
        if (failure == "sql" and query == service.migrations()[0].sql) or (
            failure == "insert" and query.startswith("INSERT")
        ):
            raise RuntimeError(SECRET)
        return ordinary

    connection.execute.side_effect = execute
    if failure == "commit":
        connection.commit.side_effect = RuntimeError(SECRET)
    with (
        patch.object(service, "_checks", return_value={"schema": failure != "checks"}),
        pytest.raises(RuntimeError),
    ):
        service.apply_migrations(connection)
    connection.rollback.assert_called_once()
    assert connection.commit.call_count == (failure == "commit")


@pytest.mark.parametrize("readonly", [False, True])
@pytest.mark.parametrize("autocommit,status", [(True, 0), (False, 2)])
def test_borrowed_active_or_autocommit_handles_are_not_touched(
    readonly: bool, autocommit: bool, status: int
) -> None:
    connection = _connection()
    connection.autocommit = autocommit
    connection.info.transaction_status = status
    with pytest.raises(service.PostgresServiceError):
        (service.verify_service if readonly else service.apply_migrations)(connection)
    connection.execute.assert_not_called()
    connection.rollback.assert_not_called()
    connection.commit.assert_not_called()


@pytest.mark.parametrize("ready", [False, True])
def test_readiness_is_read_only_even_when_not_initialized(ready: bool) -> None:
    connection = _connection()
    with patch.object(service, "_checks", return_value={"schema": ready}):
        report = service.verify_service(connection)
    assert report["status"] == ("ready" if ready else "not_ready")
    assert report["writes"] is False
    assert connection.execute.call_args_list[0].args == ("SET TRANSACTION READ ONLY",)
    connection.commit.assert_not_called()
    connection.rollback.assert_called_once()
    assert all(call.args[0].startswith("SET ") for call in connection.execute.call_args_list)


@pytest.mark.parametrize("args", [["--apply", "--verify"], ["--dsn", SECRET], ["--apply"]])
def test_cli_invalid_input_never_opens_a_database(args: list[str], capsys: Any) -> None:
    with patch("psycopg.connect") as connect, pytest.raises(SystemExit) as error:
        main(args, environ={})
    output = capsys.readouterr()
    assert error.value.code == 2 and not output.out and SECRET not in output.err
    connect.assert_not_called()


@pytest.mark.parametrize("operation", ["--apply", "--verify"])
def test_cli_connection_failures_do_not_echo_credentials(operation: str, capsys: Any) -> None:
    with (
        patch("psycopg.connect", side_effect=RuntimeError(SECRET)),
        pytest.raises(SystemExit) as error,
    ):
        main([operation], environ={"DPL_POSTGRES_SERVICE_DSN": SECRET})
    output = capsys.readouterr()
    assert error.value.code == 1 and not output.out and SECRET not in output.err


def test_cli_unready_connection_is_closed_without_acceptance(capsys: Any) -> None:
    connection = _connection()
    connection.__enter__.return_value = connection
    with (
        patch("psycopg.connect", return_value=connection),
        patch(
            "data_platform_lab.serving.postgres_service_cli.verify_service",
            return_value={
                "operation": "verify",
                "status": "not_ready",
                "writes": False,
            },
        ),
        pytest.raises(SystemExit) as error,
    ):
        main(["--verify"], environ={"DPL_POSTGRES_SERVICE_DSN": SECRET})
    assert error.value.code == 1
    report = json.loads(capsys.readouterr().out)
    assert report["writes"] is False and "acceptance" not in report
    connection.__exit__.assert_called_once()
