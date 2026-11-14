"""Disposable PostgreSQL API parity, integrity, privilege and snapshot tests."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
import uvicorn
from fastapi.testclient import TestClient
from test_ons_pay_postgres_workflow import _snapshot, arguments
from test_ons_pay_postgres_workflow_live import _setup
from test_ons_pay_refresh import _source
from test_postgres_dataset_service_live import _connect
from test_postgres_dataset_service_live import service_target as service_target

from data_platform_lab.orchestration.ons_pay_postgres_cli import main as publish
from data_platform_lab.serving import postgres_catalog
from data_platform_lab.serving.api import create_app
from data_platform_lab.serving.publish_evidence import PublicationArchive

pytestmark = pytest.mark.skipif(
    os.environ.get("DPL_POSTGRES_SERVICE_LIVE") != "1" or os.name != "posix",
    reason="explicit opt-in, fixture-owned Docker Compose and POSIX evidence required",
)


def _published(
    tmp_path: Path, target: Any, monkeypatch: Any, capsys: Any
) -> tuple[str, str, dict[str, Any], list[str]]:
    writer, reader = _setup(target)
    monkeypatch.setenv("ONS_PAY_POSTGRES_DSN", writer)
    argv = arguments(tmp_path, _source(tmp_path))
    publish([*argv, "--apply"])
    result = json.loads(capsys.readouterr().out)
    return writer, reader, result, argv


def _client(root: Path, dsn: str) -> TestClient:
    return TestClient(
        create_app(root, backend="postgresql", postgres_dsn=dsn, target_id="isolated-workflow")
    )


def _independent_consumer(root: Path, reader: str, pins: dict[str, Any]) -> dict[str, Any]:
    """A separate isolated Python process uses HTTP and the standard library only."""
    app = create_app(root, backend="postgresql", postgres_dsn=reader, target_id="isolated-workflow")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not server.started and time.monotonic() < deadline:
                time.sleep(0.05)
            assert server.started
            example = Path(__file__).resolve().parents[2] / "scripts/ons_pay_reader.py"
            output = subprocess.run(
                [
                    sys.executable,
                    "-I",
                    str(example),
                    "--base-url",
                    f"http://127.0.0.1:{port}",
                    "--registry-commit",
                    pins["registry_commit"],
                    "--source-sha256",
                    pins["source_sha256"],
                    "--target-id",
                    "isolated-workflow",
                    "--limit",
                    "2",
                    "--offset",
                    "1",
                ],
                cwd=root.parent,
                capture_output=True,
                text=True,
                timeout=45,
                env={
                    key: value
                    for key, value in os.environ.items()
                    if not key.startswith(("ONS_", "DATABASE_", "AWS_", "PG", "NEON_", "PYTHON"))
                },
            )
            assert output.returncode == 0, output.stderr
            return json.loads(output.stdout)
        finally:
            server.should_exit = True
            thread.join(timeout=10)
            assert not thread.is_alive()


def test_postgres_api_matches_parquet_with_multiple_vintages(
    tmp_path: Path,
    service_target: Any,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    writer, reader, result, argv = _published(tmp_path, service_target, monkeypatch, capsys)
    root = tmp_path / "curated"
    # Another accepted attempt is equivalent only when all source/content fingerprints agree.
    argv[argv.index("--attempt-id") + 1] = str(uuid4())
    publish([*argv, "--apply"])
    capsys.readouterr()
    publish([*arguments(tmp_path, _source(tmp_path, name="second")), "--apply"])
    capsys.readouterr()
    client, parquet = _client(root, reader), TestClient(create_app(root))
    before = _snapshot(root)
    discovery = client.get("/v1/ons-pay/vintages")
    assert discovery.status_code == 200, discovery.text
    vintages = discovery.json()["vintages"]
    assert len(vintages) == 2
    selected = next(
        item for item in vintages if item["registry_commit"] == result["vintage"]["registry_commit"]
    )
    assert selected["dataset_id"] == "ons-gross-median-weekly-pay"
    assert selected["target_id"] == "isolated-workflow"
    assert all(len(table["evidence"]) == 2 for table in selected["tables"])
    assert selected["tables"][0]["first_year"] == 2022
    for path, extra in [
        ("/v1/ons-pay/geography-year", {}),
        ("/v1/ons-pay/geography-year", {"geography": "Local Authority District", "pay_year": 2022}),
        ("/v1/ons-pay/geography-year", {"geography": "x' OR 1=1 --"}),
        ("/v1/ons-pay/geography-year", {"limit": 2, "offset": 1}),
        ("/v1/ons-pay/geography-year", {"offset": 100000}),
        ("/v1/ons-pay/areas/A001/trend", {}),
        ("/v1/ons-pay/areas/A001/trend", {"start_year": 2023, "end_year": 2024}),
    ]:
        params = {**result["vintage"], **extra}
        sql_response, local_response = (
            client.get(path, params=params),
            parquet.get(path, params=params),
        )
        assert sql_response.status_code == local_response.status_code == 200
        sql_page, local_page = sql_response.json(), local_response.json()
        assert sql_page["rows"] == local_page["rows"]
        assert sql_page["total"] == local_page["total"]
        assert sql_page["vintage"]["tables"] == selected["tables"]
    null_page = client.get("/v1/ons-pay/areas/A001/trend", params=result["vintage"]).json()
    assert null_page["rows"][2]["weekly_pay_gbp"] is None
    assert client.post("/v1/ons-pay/vintages").status_code == 405
    assert (
        client.get(
            "/v1/ons-pay/geography-year", params={**result["vintage"], "registry_commit": "f" * 40}
        ).status_code
        == 404
    )
    assert _snapshot(root) == before
    consumed = _independent_consumer(root, reader, result["vintage"])
    assert (
        consumed["rows"]
        == client.get(
            "/v1/ons-pay/geography-year",
            params={**result["vintage"], "limit": 2, "offset": 1},
        ).json()["rows"]
    )
    assert consumed["vintage"]["source_sha256"] == result["vintage"]["source_sha256"]
    assert _snapshot(root) == before
    # HTTP credentials are operator-side: even accidentally supplied writer/admin DSNs fail.
    assert _client(root, writer).get("/v1/ons-pay/vintages").status_code == 503
    assert _client(root, service_target.dsn).get("/v1/ons-pay/vintages").status_code == 503
    with _connect(service_target.dsn) as admin:
        admin.execute(
            "GRANT UPDATE (weekly_pay_gbp) ON public.ons_pay_local_authority_yoy TO reader"
        )
    assert client.get("/v1/ons-pay/vintages").status_code == 503


def test_corrupt_metadata_evidence_and_rows_fail_closed(
    tmp_path: Path,
    service_target: Any,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    _, reader, result, _ = _published(tmp_path, service_target, monkeypatch, capsys)
    root = tmp_path / "curated"
    client = _client(root, reader)
    url, params = "/v1/ons-pay/geography-year", result["vintage"]
    with _connect(service_target.dsn) as admin:
        for column, corrupt in [
            ("evidence_sha256", "f" * 64),
            ("source_plan_sha256", "f" * 64),
            ("sink_snapshot_sha256", "f" * 64),
            ("records_accepted", 999),
            ("evidence_path", "../../private-secret"),
            ("attempt_id", str(uuid4())),
        ]:
            # Identifiers are fixed test literals; values are always parameters.
            old = admin.execute(
                f"SELECT {column} FROM dpl_curated.publications WHERE table_name='geography_year'"
            ).fetchone()[0]
            admin.execute(
                f"UPDATE dpl_curated.publications SET {column}=%s "
                "WHERE table_name='geography_year'",
                (corrupt,),
            )
            admin.commit()
            response = client.get(url, params=params)
            assert response.status_code == 503 and "private-secret" not in response.text
            admin.execute(
                f"UPDATE dpl_curated.publications SET {column}=%s "
                "WHERE table_name='geography_year'",
                (old,),
            )
            admin.commit()
        baseline = PublicationArchive(root).root / (
            result["publications"][0]["evidence"]["record_sha256"] + ".json"
        )
        original = baseline.read_bytes()
        baseline.write_bytes(b"{}")
        assert client.get(url, params=params).status_code == 503
        baseline.write_bytes(original)
        admin.execute(
            "UPDATE public.ons_pay_geography_year "
            "SET areas_total=areas_total+1, areas_with_pay=areas_with_pay+1"
        )
        admin.commit()
        assert client.get(url, params=params).status_code == 503

        admin.execute(
            "UPDATE public.ons_pay_geography_year "
            "SET areas_total=areas_total-1, areas_with_pay=areas_with_pay-1"
        )
        admin.commit()
        deleted = admin.execute(
            "DELETE FROM public.ons_pay_geography_year WHERE record_id="
            "(SELECT record_id FROM public.ons_pay_geography_year LIMIT 1) RETURNING *"
        ).fetchone()
        admin.commit()
        assert client.get(url, params=params).status_code == 503
        admin.execute(
            "INSERT INTO public.ons_pay_geography_year VALUES ("
            + ",".join(["%s"] * len(deleted))
            + ")",
            deleted,
        )
        admin.commit()
        added = ("f" * 64, *deleted[1:])
        admin.execute(
            "INSERT INTO public.ons_pay_geography_year VALUES ("
            + ",".join(["%s"] * len(added))
            + ")",
            added,
        )
        admin.commit()
        assert client.get(url, params=params).status_code == 503
        admin.execute("DELETE FROM public.ons_pay_geography_year WHERE record_id=%s", ("f" * 64,))
        admin.execute("DELETE FROM dpl_curated.publications WHERE table_name='local_authority_yoy'")
        admin.commit()
        assert client.get(url, params=params).status_code == 503
        admin.execute("DELETE FROM dpl_curated.publications")
        admin.commit()
        assert client.get(url, params=params).status_code == 404
        assert client.get("/v1/ons-pay/vintages").json() == {"vintages": []}


def test_pagination_uses_the_reconciled_read_only_snapshot(
    tmp_path: Path,
    service_target: Any,
    monkeypatch: Any,
    capsys: Any,
) -> None:
    _, reader, result, _ = _published(tmp_path, service_target, monkeypatch, capsys)
    root = tmp_path / "curated"
    client = _client(root, reader)
    expected = client.get("/v1/ons-pay/geography-year", params=result["vintage"]).json()
    original = postgres_catalog.reconcile_postgres

    def change_after_reconciliation(plan: Any, connection: Any) -> Any:
        assert connection.execute("SHOW transaction_read_only").fetchone() == ("on",)
        assert connection.execute("SHOW transaction_isolation").fetchone() == ("repeatable read",)
        report = original(plan, connection)
        with _connect(service_target.dsn) as admin:
            admin.execute(
                "UPDATE public.ons_pay_geography_year "
                "SET areas_total=areas_total+1, areas_with_pay=areas_with_pay+1"
            )
        return report

    monkeypatch.setattr(postgres_catalog, "reconcile_postgres", change_after_reconciliation)
    response = client.get("/v1/ons-pay/geography-year", params=result["vintage"])
    assert response.status_code == 200
    assert response.json() == expected
    monkeypatch.setattr(postgres_catalog, "reconcile_postgres", original)
    assert client.get("/v1/ons-pay/geography-year", params=result["vintage"]).status_code == 503
