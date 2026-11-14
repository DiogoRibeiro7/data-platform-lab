"""Offline reader configuration and HTTP validation boundaries."""

from __future__ import annotations

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
