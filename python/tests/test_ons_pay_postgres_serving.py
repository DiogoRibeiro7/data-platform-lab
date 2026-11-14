"""Offline reader configuration and HTTP validation boundaries."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from data_platform_lab.serving.api import create_app
from data_platform_lab.serving.catalog import CatalogError
from data_platform_lab.serving.cli import main
from data_platform_lab.serving.postgres_catalog import PostgresCatalog


def test_factory_and_health_are_inert(tmp_path: Path, monkeypatch: Any) -> None:
    connection = Mock(side_effect=AssertionError("Unexpected connection"))
    monkeypatch.setattr("psycopg.connect", connection)
    root = tmp_path / "uncreated"
    app = create_app(
        root, backend="postgresql", postgres_dsn="private-setting", target_id="reader-test"
    )
    client = TestClient(app)
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/openapi.json").status_code == 200
    assert not root.exists()
    connection.assert_not_called()


@pytest.mark.parametrize(
    "options",
    [
        {"backend": "postgresql"},
        {"backend": "postgresql", "postgres_dsn": "", "target_id": "reader-test"},
        {"backend": "parquet", "postgres_dsn": "private-setting"},
        {"backend": "unknown"},
    ],
)
def test_factory_rejects_ambiguous_configuration(tmp_path: Path, options: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        create_app(tmp_path, **options)


@pytest.mark.parametrize(
    "extra",
    [
        {"registry_commit": "latest"},
        {"source_sha256": "bad"},
        {"model_version": 2},
        {"limit": 101},
        {"limit": 0},
        {"offset": -1},
        {"offset": 100001},
        {"pay_year": 1800},
        {"geography": "x" * 101},
    ],
)
def test_invalid_queries_never_open_database(
    tmp_path: Path,
    monkeypatch: Any,
    extra: dict[str, Any],
) -> None:
    connection = Mock(side_effect=AssertionError("Unexpected connection"))
    monkeypatch.setattr("psycopg.connect", connection)
    client = TestClient(
        create_app(
            tmp_path, backend="postgresql", postgres_dsn="private-setting", target_id="reader-test"
        )
    )
    params = {"registry_commit": "a" * 40, "source_sha256": "b" * 64, **extra}
    assert client.get("/v1/ons-pay/geography-year", params=params).status_code == 422
    connection.assert_not_called()


def test_database_failure_is_private_and_does_not_fallback(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(
        "psycopg.connect", Mock(side_effect=RuntimeError("password-secret row-secret"))
    )
    client = TestClient(
        create_app(
            tmp_path, backend="postgresql", postgres_dsn="private-setting", target_id="reader-test"
        )
    )
    response = client.get("/v1/ons-pay/vintages")
    assert response.status_code == 503
    assert "secret" not in response.text
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("host", ["0.0.0.0", "::", "192.0.2.1", "remote.example"])
def test_cli_blocks_unauthenticated_network_binding(monkeypatch: Any, host: str) -> None:
    monkeypatch.setenv("ONS_PAY_POSTGRES_READER_DSN", "private-setting")
    with pytest.raises(SystemExit) as error:
        main(["--backend", "postgresql", "--target-id", "reader-test", "--host", host])
    assert error.value.code == 2


def test_cli_requires_separate_reader_settings(monkeypatch: Any) -> None:
    monkeypatch.delenv("ONS_PAY_POSTGRES_READER_DSN", raising=False)
    monkeypatch.setenv("DATABASE_URL", "writer-secret")
    monkeypatch.setenv("ONS_PAY_POSTGRES_DSN", "writer-secret")
    with pytest.raises(SystemExit):
        main(["--backend", "postgresql", "--target-id", "reader-test"])


def _reader_cli(path: Path) -> list[str]:
    return ["--backend", "postgresql", "--target-id", "reader-test", "--reader-env-file", str(path)]


def test_cli_loads_private_reader_file_without_expansion_or_environment_changes(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    monkeypatch.delenv("ONS_PAY_POSTGRES_READER_DSN", raising=False)
    monkeypatch.setenv("DATABASE_URL", "owner-secret")
    monkeypatch.setenv("ONS_PAY_POSTGRES_DSN", "writer-secret")
    dsn = "host=localhost password='spaces \\\\ literal$HOME `echo no` = \"quote\"'"
    path = tmp_path / "reader.env"
    path.write_text("# Reader only\n\nONS_PAY_POSTGRES_READER_DSN=" + json.dumps(dsn) + "\n")
    path.chmod(0o600)
    original = path.read_bytes()
    factory, run = Mock(), Mock()
    monkeypatch.setattr("data_platform_lab.serving.api.create_app", factory)
    monkeypatch.setattr("uvicorn.run", run)
    connection = Mock(side_effect=AssertionError("Unexpected connection"))
    monkeypatch.setattr("psycopg.connect", connection)
    for _ in range(2):
        main(_reader_cli(path))
        assert factory.call_args.kwargs == {
            "backend": "postgresql",
            "postgres_dsn": dsn,
            "target_id": "reader-test",
        }
        assert run.call_args.kwargs == {"host": "127.0.0.1", "port": 8000}
    assert path.read_bytes() == original
    assert "ONS_PAY_POSTGRES_READER_DSN" not in os.environ
    assert os.environ["DATABASE_URL"] == "owner-secret"
    connection.assert_not_called()
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize(
    "content",
    [
        b"",
        b'DATABASE_URL="credential-marker"',
        b"ONS_PAY_POSTGRES_READER_DSN=credential-marker",
        b"ONS_PAY_POSTGRES_READER_DSN='credential-marker'",
        b'export ONS_PAY_POSTGRES_READER_DSN="credential-marker"',
        b'ONS_PAY_POSTGRES_READER_DSN="credential-marker" # trailing',
        b'ONS_PAY_POSTGRES_READER_DSN="credential-marker"\nDATABASE_URL="other"',
        b'ONS_PAY_POSTGRES_READER_DSN="one"\nONS_PAY_POSTGRES_READER_DSN="two"',
        b"ONS_PAY_POSTGRES_READER_DSN=null",
        b"ONS_PAY_POSTGRES_READER_DSN=42",
        b"ONS_PAY_POSTGRES_READER_DSN=[]",
        b'ONS_PAY_POSTGRES_READER_DSN="   "',
        b'ONS_PAY_POSTGRES_READER_DSN="\\u0000"',
        b"\xffcredential-marker",
        b"#" + b"x" * 65536,
        b"ONS_PAY_POSTGRES_READER_DSN=" + b"[" * 2000,
    ],
)
def test_cli_rejects_reader_file_contents_without_disclosure(
    tmp_path: Path, monkeypatch: Any, capsys: Any, content: bytes
) -> None:
    monkeypatch.delenv("ONS_PAY_POSTGRES_READER_DSN", raising=False)
    path = tmp_path / "private-path-marker.env"
    path.write_bytes(content)
    path.chmod(0o600)
    imported = Mock(side_effect=AssertionError("Must validate before loading server"))
    monkeypatch.setattr("data_platform_lab.serving.cli.import_module", imported)
    with pytest.raises(SystemExit) as error:
        main(_reader_cli(path))
    assert error.value.code == 2
    output = capsys.readouterr()
    assert "Cannot load reader file" in output.err
    assert all(marker not in output.err for marker in ("credential-marker", "private-path-marker"))
    assert output.out == ""
    imported.assert_not_called()


@pytest.mark.parametrize("ambient", ["reader-secret", ""])
def test_cli_rejects_ambiguous_reader_sources(
    tmp_path: Path, monkeypatch: Any, capsys: Any, ambient: str
) -> None:
    monkeypatch.setenv("ONS_PAY_POSTGRES_READER_DSN", ambient)
    with pytest.raises(SystemExit):
        main(_reader_cli(tmp_path / "never-read.env"))
    output = capsys.readouterr()
    assert "Unset ONS_PAY_POSTGRES_READER_DSN" in output.err
    assert "reader-secret" not in output.err


@pytest.mark.parametrize("kind", ["missing", "directory"])
def test_cli_rejects_unreadable_reader_file(tmp_path: Path, monkeypatch: Any, kind: str) -> None:
    monkeypatch.delenv("ONS_PAY_POSTGRES_READER_DSN", raising=False)
    path = tmp_path / "reader.env"
    if kind == "directory":
        path.mkdir()
    with pytest.raises(SystemExit) as error:
        main(_reader_cli(path))
    assert error.value.code == 2


@pytest.mark.skipif(os.name != "posix", reason="POSIX permissions and file types")
@pytest.mark.parametrize("kind", ["readable", "writable", "symlink", "fifo", "other-owner"])
def test_cli_rejects_nonprivate_reader_file(tmp_path: Path, monkeypatch: Any, kind: str) -> None:
    monkeypatch.delenv("ONS_PAY_POSTGRES_READER_DSN", raising=False)
    path = tmp_path / "reader.env"
    if kind == "fifo":
        os.mkfifo(path, mode=0o600)
    else:
        path.write_text('ONS_PAY_POSTGRES_READER_DSN="service=reader"')
        path.chmod(0o600)
        if kind in {"readable", "writable"}:
            path.chmod(0o640 if kind == "readable" else 0o602)
        elif kind == "symlink":
            link = tmp_path / "link.env"
            link.symlink_to(path)
            path = link
        else:
            uid = os.getuid()
            monkeypatch.setattr(os, "getuid", lambda: uid + 1)
    with pytest.raises(SystemExit) as error:
        main(_reader_cli(path))
    assert error.value.code == 2


def test_cli_reader_file_is_postgres_only(tmp_path: Path, capsys: Any) -> None:
    with pytest.raises(SystemExit):
        main(["--reader-env-file", str(tmp_path / "never-read.env")])
    assert "require --backend postgresql" in capsys.readouterr().err


def test_read_transaction_rolls_back_and_closes_on_failure(
    tmp_path: Path, monkeypatch: Any
) -> None:
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    connection.execute.return_value.fetchone.return_value = (
        False,
        True,
        False,
        False,
        False,
        False,
    )
    monkeypatch.setattr("psycopg.connect", Mock(return_value=connection))
    catalog = PostgresCatalog(tmp_path, dsn="private-setting", target_id="reader-test")
    with pytest.raises(CatalogError, match="reader"):
        catalog.list_vintages()
    statements = [call.args[0] for call in connection.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert all(statement.startswith(("SET ", "SELECT ")) for statement in statements)
    connection.rollback.assert_called_once()
    connection.__exit__.assert_called_once()
