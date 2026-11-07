"""Offline operator workflow tests using real curated files and injected sinks."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_ons_pay_publish_reconcile import FakeMongoReader, FakePostgresReader
from test_ons_pay_publish_sinks import FakeMongoCollection, FakePgConnection
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.serving import publish_cli
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.serving.publish_plan import PublishPlan, TableName

SECRET = "postgresql://private-user:do-not-print@private-host/private-db"


class CliPostgres(FakePgConnection):
    """Read back the adapter's actual committed fake rows, not prefilled expectations."""

    def __init__(self, table: TableName, *, fail_on: str = "") -> None:
        super().__init__(fail_on=fail_on)
        self.table = table
        self.reads = 0
        self.closed = False
        self.read_only = False
        self.settings: list[tuple[tuple[object, ...], bool]] = []
        self.setting_cursors: list[Mock] = []

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> Any:
        if query == "SELECT pg_catalog.set_config(%s, %s, false)":
            assert params is not None
            self.settings.append((params, self.read_only))
            if self.fail_on == "configure":
                raise OSError(SECRET)
            cursor = Mock()
            self.setting_cursors.append(cursor)
            return cursor
        if params is None:
            return super().execute(query)
        self.reads += 1
        if self.fail_on == "read":
            raise OSError(SECRET)
        return FakePostgresReader(list(self.persisted.values()), self.table).execute(query, params)

    def close(self) -> None:
        self.closed = True
        if self.fail_on == "connection_close":
            raise OSError(SECRET)


class CliMongo(FakeMongoCollection):
    """Combine idempotent replacement and exact-vintage reads in one collection."""

    def __init__(self, *, fail_at: int = 0, fail_read: bool = False) -> None:
        super().__init__(fail_at=fail_at)
        self.fail_read = fail_read
        self.reads = 0
        self.drift = False
        self.malformed = False

    def find(self, filter: dict[str, object]) -> Any:
        self.reads += 1
        if self.fail_read:
            raise OSError(SECRET)
        documents = copy.deepcopy(list(self.documents.values()))
        if self.drift and documents:
            documents[0][SECRET] = SECRET
        if self.malformed and documents:
            documents[0]["_id"] = SECRET
        return FakeMongoReader(documents).find(filter)


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, VintageKey]:
    """Generate the real Silver/Gold outputs without a network connection."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    return root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])


def _args(
    curated: tuple[Path, VintageKey],
    sink: str = "postgresql",
    table: str = "local_authority_yoy",
) -> list[str]:
    root, vintage = curated
    return [
        "--data-root",
        str(root),
        "--model-version",
        str(vintage.model_version),
        "--registry-commit",
        vintage.registry_commit,
        "--source-sha256",
        vintage.source_sha256,
        "--table",
        table,
        "--sink",
        sink,
    ]


def _invoke(
    capsys: pytest.CaptureFixture[str],
    argv: list[str],
    *,
    expected_exit: int = 0,
    **kwargs: Any,
) -> dict[str, Any]:
    """Assert JSON-only output and no driver diagnostics on either output stream."""
    if expected_exit:
        with pytest.raises(SystemExit) as error:
            publish_cli.main(argv, **kwargs)
        assert error.value.code == expected_exit
    else:
        publish_cli.main(argv, **kwargs)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert SECRET not in captured.out
    return json.loads(captured.out)


def _snapshot(root: Path) -> dict[str, tuple[bytes, int]]:
    """Detect created, removed, modified or rewritten local source artifacts."""
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        for path in root.rglob("*")
        if path.is_file()
    }


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_preview_is_read_only_and_never_loads_credentials_or_drivers(
    curated: tuple[Path, VintageKey], capsys: pytest.CaptureFixture[str], sink: str
) -> None:
    before = _snapshot(curated[0])
    environ = Mock()
    environ.get.side_effect = AssertionError("preview read connection settings")
    with patch.object(publish_cli.importlib, "import_module", side_effect=AssertionError):
        result = _invoke(capsys, _args(curated, sink), environ=environ)
    assert result["operation"] == "preview"
    assert result["status"] == "previewed"
    assert result["counts"] == {"source": 6, "applied": None, "observed": None}
    assert result["vintage"]["source_sha256"] == curated[1].source_sha256
    assert len(result["fingerprints"]["expected_sink_sha256"]) == 64
    assert result["acceptance"] is result["reconciliation"] is None
    assert "records" not in result
    assert _snapshot(curated[0]) == before


def test_dispatcher_exposes_new_command(curated: tuple[Path, VintageKey], capsys: Any) -> None:
    platform_main(["ons-pay-publish", *_args(curated)])
    assert json.loads(capsys.readouterr().out)["status"] == "previewed"


@pytest.mark.parametrize(
    "flag", ["--model-version", "--registry-commit", "--source-sha256", "--table", "--sink"]
)
def test_all_source_and_target_arguments_are_required(
    curated: tuple[Path, VintageKey], capsys: Any, flag: str
) -> None:
    args = _args(curated)
    index = args.index(flag)
    del args[index : index + 2]
    result = _invoke(capsys, args, expected_exit=2)
    assert result["error"]["code"] == "invalid_arguments"
    assert result["vintage"] is None


@pytest.mark.parametrize(
    "extra",
    [
        ["--sink", SECRET],
        ["--unknown", SECRET],
        ["--model-version", "2"],
        ["--model-version", SECRET],
        ["--table", SECRET],
        ["--apply", "--verify"],
        ["--app"],
        ["--source-sha256", SECRET],
        ["--registry-commit", SECRET],
        ["--max-rows", "0"],
        ["--max-rows", "100001"],
        ["--max-rows", "1"],
    ],
)
def test_invalid_requests_fail_without_connecting(
    curated: tuple[Path, VintageKey], capsys: Any, extra: list[str]
) -> None:
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        result = _invoke(capsys, [*_args(curated), *extra], expected_exit=2)
    assert result["status"] == "failed"
    assert result["acceptance"] is None


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
@pytest.mark.parametrize("operation", ["--apply", "--verify"])
def test_missing_configuration_never_imports_drivers(
    curated: tuple[Path, VintageKey], capsys: Any, sink: str, operation: str
) -> None:
    with patch.object(publish_cli.importlib, "import_module", side_effect=AssertionError):
        result = _invoke(capsys, [*_args(curated, sink), operation], environ={}, expected_exit=2)
    assert result["error"]["code"] == "configuration_error"


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_apply_replays_and_verify_never_writes_or_fabricates_acceptance(
    curated: tuple[Path, VintageKey], capsys: Any, table: TableName, sink: str
) -> None:
    pg = CliPostgres(table)
    mongo = CliMongo()
    kwargs = {"postgres_connection": pg} if sink == "postgresql" else {"mongo_collection": mongo}
    args = _args(curated, sink, table)
    before = _snapshot(curated[0])
    first = _invoke(capsys, [*args, "--apply"], environ={}, **kwargs)
    second = _invoke(capsys, [*args, "--apply"], environ={}, **kwargs)
    assert first == second
    assert first["status"] == "accepted"
    assert first["acceptance"]["status"] == "accepted"
    assert first["counts"]["source"] == first["counts"]["applied"] == first["counts"]["observed"]
    fingerprints = first["fingerprints"]
    assert fingerprints["expected_sink_sha256"] == fingerprints["observed_sink_sha256"]
    assert first["acceptance"]["source_plan_sha256"] == fingerprints["source_plan_sha256"]
    assert (
        len(pg.persisted if sink == "postgresql" else mongo.documents) == first["counts"]["source"]
    )
    pg_state = copy.deepcopy((pg.persisted, pg.queries, pg.commits, pg.rollbacks))
    mongo_state = copy.deepcopy((mongo.documents, mongo.calls))
    with patch(
        "data_platform_lab.serving.publish_gate.accept_publication", side_effect=AssertionError
    ):
        verified = _invoke(capsys, [*args, "--verify"], environ={}, **kwargs)
    assert verified["status"] == "verified"
    assert verified["acceptance"] is None
    assert verified["counts"]["applied"] is None
    assert (pg.persisted, pg.queries, pg.commits, pg.rollbacks) == pg_state
    assert (mongo.documents, mongo.calls) == mongo_state
    assert not pg.closed
    assert _snapshot(curated[0]) == before


@pytest.mark.parametrize("operation", ["--apply", "--verify"])
def test_drift_rejects_without_leaking_record_values_or_field_names(
    curated: tuple[Path, VintageKey], capsys: Any, operation: str
) -> None:
    mongo = CliMongo()
    args = _args(curated, "mongodb")
    _invoke(capsys, [*args, "--apply"], mongo_collection=mongo)
    mongo.drift = True
    result = _invoke(capsys, [*args, operation], mongo_collection=mongo, expected_exit=3)
    assert result["status"] == "rejected"
    assert result["acceptance"] is None
    assert result["reconciliation"]["changed_record_count"] == 1
    assert result["reconciliation"]["changed_field_count"] == 1


def test_empty_sink_verification_reports_missing_records(curated: Any, capsys: Any) -> None:
    mongo = CliMongo()
    result = _invoke(
        capsys, [*_args(curated, "mongodb"), "--verify"], mongo_collection=mongo, expected_exit=3
    )
    assert result["reconciliation"]["missing_count"] == 6
    assert result["counts"]["observed"] == 0
    assert mongo.calls == []


def test_partial_mongo_write_fails_without_acceptance_and_can_be_replayed(
    curated: Any, capsys: Any
) -> None:
    mongo = CliMongo(fail_at=2)
    args = [*_args(curated, "mongodb"), "--apply"]
    failed = _invoke(capsys, args, mongo_collection=mongo, expected_exit=4)
    assert failed["acceptance"] is None
    assert failed["counts"]["applied"] is None
    assert len(mongo.documents) == 1
    assert mongo.reads == 0
    mongo.fail_at = 0
    assert _invoke(capsys, args, mongo_collection=mongo)["status"] == "accepted"
    assert len(mongo.documents) == 6


@pytest.mark.parametrize("failure", ["ddl", "upsert", "commit"])
def test_failed_postgres_write_rolls_back(curated: Any, capsys: Any, failure: str) -> None:
    pg = CliPostgres("local_authority_yoy", fail_on=failure)
    result = _invoke(capsys, [*_args(curated), "--apply"], postgres_connection=pg, expected_exit=4)
    assert result["acceptance"] is None
    assert pg.rollbacks == 1
    assert pg.persisted == {}
    assert pg.reads == 0


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_read_io_errors_have_database_exit_code(curated: Any, capsys: Any, sink: str) -> None:
    kwargs = (
        {"postgres_connection": CliPostgres("local_authority_yoy", fail_on="read")}
        if sink == "postgresql"
        else {"mongo_collection": CliMongo(fail_read=True)}
    )
    result = _invoke(capsys, [*_args(curated, sink), "--apply"], expected_exit=4, **kwargs)
    assert result["error"]["code"] == "database_error"
    assert result["counts"]["applied"] == 6
    assert result["acceptance"] is None


def test_malformed_sink_data_is_a_reconciliation_failure(curated: Any, capsys: Any) -> None:
    mongo = CliMongo()
    mongo.malformed = True
    result = _invoke(
        capsys, [*_args(curated, "mongodb"), "--apply"], mongo_collection=mongo, expected_exit=3
    )
    assert result["error"]["code"] == "reconciliation_failed"


def test_source_changed_while_connecting_prevents_all_sink_io(curated: Any, capsys: Any) -> None:
    pg = CliPostgres("local_authority_yoy")
    root, vintage = curated
    artifact = CuratedCatalog(root).get_vintage(vintage).artifacts["geography_year"].path

    def connect(*args: Any, **kwargs: Any) -> CliPostgres:
        artifact.write_bytes(artifact.read_bytes() + b"tampered")
        return pg

    with patch.object(
        publish_cli.importlib, "import_module", return_value=SimpleNamespace(connect=connect)
    ):
        result = _invoke(
            capsys,
            [*_args(curated), "--apply"],
            environ={"ONS_PAY_POSTGRES_DSN": SECRET},
            expected_exit=2,
        )
    assert result["error"]["code"] == "source_validation_failed"
    assert pg.queries == [] and pg.reads == 0 and pg.closed


def test_changed_plan_fingerprint_after_connecting_is_rejected(curated: Any, capsys: Any) -> None:
    original = publish_cli._source_plan
    calls = 0

    def changed(args: Any) -> PublishPlan:
        nonlocal calls
        calls += 1
        plan = original(args)
        return replace(plan, source_artifact_sha256="f" * 64) if calls == 2 else plan

    pg = CliPostgres("local_authority_yoy")
    with patch.object(publish_cli, "_source_plan", changed):
        _invoke(capsys, [*_args(curated), "--apply"], postgres_connection=pg, expected_exit=2)
    assert calls == 2
    assert pg.queries == [] and pg.reads == 0


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
@pytest.mark.parametrize("failure", ["import", "connect"])
def test_optional_driver_and_connection_errors_are_sanitized(
    curated: Any, capsys: Any, sink: str, failure: str
) -> None:
    settings = {
        "ONS_PAY_POSTGRES_DSN": SECRET,
        "ONS_PAY_MONGODB_URI": SECRET,
        "ONS_PAY_MONGODB_DATABASE": "local_test",
    }
    driver = SimpleNamespace(
        connect=Mock(side_effect=OSError(SECRET)), MongoClient=Mock(side_effect=OSError(SECRET))
    )
    with patch.object(
        publish_cli.importlib,
        "import_module",
        return_value=driver,
        side_effect=ImportError(SECRET) if failure == "import" else None,
    ):
        result = _invoke(
            capsys,
            [*_args(curated, sink), "--apply"],
            environ=settings,
            expected_exit=2 if failure == "import" else 4,
        )
    assert result["acceptance"] is None


@pytest.mark.parametrize("flag", ["--statement-timeout-ms", "--lock-timeout-ms"])
@pytest.mark.parametrize("value", ["0", "-1", "2147483648", SECRET])
def test_invalid_timeouts_fail_before_planning_or_connecting(
    curated: Any, capsys: Any, flag: str, value: str
) -> None:
    with patch.object(publish_cli, "_source_plan", side_effect=AssertionError):
        result = _invoke(capsys, [*_args(curated), "--apply", flag, value], expected_exit=2)
    assert result["error"]["code"] == "invalid_arguments"
    assert result["acceptance"] is None


@pytest.mark.parametrize("flag", ["--statement-timeout-ms", "--lock-timeout-ms"])
def test_postgres_timeout_flags_reject_mongodb(curated: Any, capsys: Any, flag: str) -> None:
    with patch.object(publish_cli, "_source_plan", side_effect=AssertionError):
        result = _invoke(capsys, [*_args(curated, "mongodb"), flag, "1000"], expected_exit=2)
    assert result["error"]["code"] == "invalid_arguments"


def test_timeout_preview_does_not_access_settings_or_borrowed_handles(
    curated: Any, capsys: Any
) -> None:
    before = _snapshot(curated[0])
    with patch.object(publish_cli, "_open_sink", side_effect=AssertionError):
        result = _invoke(
            capsys,
            [*_args(curated), "--statement-timeout-ms", "30000", "--lock-timeout-ms", "5000"],
            environ=Mock(),
            postgres_connection=Mock(),
        )
    assert result["status"] == "previewed"
    assert _snapshot(curated[0]) == before


@pytest.mark.parametrize("operation", ["--apply", "--verify"])
def test_timeout_flags_do_not_change_borrowed_connections(
    curated: Any, capsys: Any, operation: str
) -> None:
    pg = CliPostgres("local_authority_yoy")
    result = _invoke(
        capsys,
        [*_args(curated), operation, "--statement-timeout-ms", "1000"],
        postgres_connection=pg,
        expected_exit=2,
    )
    assert result["error"]["code"] == "invalid_arguments"
    assert pg.settings == pg.queries == []
    assert pg.commits == pg.rollbacks == pg.reads == 0
    assert not pg.closed


@pytest.mark.parametrize("operation", ["--apply", "--verify"])
@pytest.mark.parametrize(
    ("flags", "settings"),
    [
        (["--statement-timeout-ms", "2147483647"], [("statement_timeout", "2147483647ms")]),
        (["--lock-timeout-ms", "1"], [("lock_timeout", "1ms")]),
        (
            ["--statement-timeout-ms", "30000", "--lock-timeout-ms", "5000"],
            [("statement_timeout", "30000ms"), ("lock_timeout", "5000ms")],
        ),
    ],
)
def test_owned_postgres_timeouts_use_session_settings_and_close_cursors(
    curated: Any, capsys: Any, operation: str, flags: list[str], settings: list[tuple[str, str]]
) -> None:
    pg = CliPostgres("local_authority_yoy")
    connect = Mock(return_value=pg)
    with patch.object(
        publish_cli.importlib, "import_module", return_value=SimpleNamespace(connect=connect)
    ):
        result = _invoke(
            capsys,
            [*_args(curated), operation, *flags],
            environ={"ONS_PAY_POSTGRES_DSN": SECRET},
            expected_exit=3 if operation == "--verify" else 0,
        )
    connect.assert_called_once_with(SECRET, autocommit=False, connect_timeout=10)
    assert pg.settings == [(setting, operation == "--verify") for setting in settings]
    for cursor in pg.setting_cursors:
        cursor.close.assert_called_once_with()
    assert pg.closed and pg.reads == 1
    assert pg.commits == (1 if operation == "--apply" else 0)
    assert result["status"] == ("accepted" if operation == "--apply" else "rejected")


@pytest.mark.parametrize("operation", ["--apply", "--verify"])
def test_timeout_configuration_failure_closes_client_without_publication(
    curated: Any, capsys: Any, operation: str
) -> None:
    pg = CliPostgres("local_authority_yoy", fail_on="configure")
    with patch.object(
        publish_cli.importlib,
        "import_module",
        return_value=SimpleNamespace(connect=Mock(return_value=pg)),
    ):
        result = _invoke(
            capsys,
            [*_args(curated), operation, "--statement-timeout-ms", "30000"],
            environ={"ONS_PAY_POSTGRES_DSN": SECRET},
            expected_exit=4,
        )
    assert result["error"]["code"] == "database_error"
    assert result["acceptance"] is None
    assert pg.closed and pg.queries == [] and pg.reads == pg.commits == 0


def test_owned_postgres_verification_uses_read_only_and_closes(curated: Any, capsys: Any) -> None:
    pg = CliPostgres("local_authority_yoy")
    connect = Mock(return_value=pg)
    with patch.object(
        publish_cli.importlib, "import_module", return_value=SimpleNamespace(connect=connect)
    ):
        _invoke(
            capsys,
            [*_args(curated), "--verify"],
            environ={"ONS_PAY_POSTGRES_DSN": SECRET},
            expected_exit=3,
        )
    connect.assert_called_once_with(SECRET, autocommit=False, connect_timeout=10)
    assert pg.read_only and pg.closed
    assert pg.queries == [] and pg.commits == pg.rollbacks == 0
    assert pg.settings == []


def test_owned_mongodb_selects_fixed_collection_and_closes(curated: Any, capsys: Any) -> None:
    mongo = CliMongo()
    database = Mock()
    client = Mock()
    client.__getitem__ = Mock(return_value=database)
    database.__getitem__ = Mock(return_value=mongo)
    factory = Mock(return_value=client)
    settings = {"ONS_PAY_MONGODB_URI": SECRET, "ONS_PAY_MONGODB_DATABASE": "local_test"}
    with patch.object(
        publish_cli.importlib, "import_module", return_value=SimpleNamespace(MongoClient=factory)
    ):
        result = _invoke(capsys, [*_args(curated, "mongodb"), "--apply"], environ=settings)
    assert result["status"] == "accepted"
    factory.assert_called_once_with(
        SECRET,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=30_000,
        retryReads=False,
        retryWrites=False,
    )
    client.__getitem__.assert_called_once_with("local_test")
    database.__getitem__.assert_called_once_with("ons_pay_local_authority_yoy")
    client.close.assert_called_once_with()


def test_owned_connection_close_failure_withholds_acceptance(curated: Any, capsys: Any) -> None:
    pg = CliPostgres("local_authority_yoy", fail_on="connection_close")
    with patch.object(
        publish_cli.importlib,
        "import_module",
        return_value=SimpleNamespace(connect=Mock(return_value=pg)),
    ):
        result = _invoke(
            capsys,
            [*_args(curated), "--apply"],
            environ={"ONS_PAY_POSTGRES_DSN": SECRET},
            expected_exit=4,
        )
    assert result["acceptance"] is None
    assert result["counts"]["applied"] == 6
    assert pg.closed


def test_uncurated_preview_does_not_create_data_root(tmp_path: Path, capsys: Any) -> None:
    root = tmp_path / "absent"
    _invoke(capsys, _args((root, VintageKey(1, "a" * 40, "b" * 64))), expected_exit=2)
    assert not root.exists()


def test_help_needs_no_optional_analytics_or_database_clients() -> None:
    script = """
import importlib.abc
import sys
class BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split(".")[0] in {"psycopg", "pymongo", "duckdb", "fastapi"}:
            raise AssertionError("optional client imported: " + fullname)
sys.meta_path.insert(0, BlockOptional())
from data_platform_lab.cli.main import main
main(["ons-pay-publish", "--help"])
"""
    completed = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert completed.returncode == 0, completed.stderr
    assert "--apply" in completed.stdout and "--verify" in completed.stdout
