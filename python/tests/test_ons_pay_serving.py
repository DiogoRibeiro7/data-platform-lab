"""Offline API contract tests over real curated DuckDB/Parquet model outputs."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.curation.ons_pay import curate_ons_pay
from data_platform_lab.serving.api import create_app
from data_platform_lab.serving.catalog import CuratedCatalog
from data_platform_lab.storage import LocalBlobStore

COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
DATASET = "ons-gross-median-weekly-pay"
SOURCE_PATH = f"datasets/{DATASET}/raw/gross-median-weekly-pay.csv"
PAY_CSV = (
    "areacd,areanm,geography,indicator,period,observation,measure,unit,lci_95,uci_95,status,notes\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2022-04-01,100,Pounds,£,90,110,,\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2023-04-01,110,Pounds,£,100,120,,\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,2024-04-01,,Pounds,£,,,x,\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2025-04-01,130,Pounds,£,120,140,,\n"
    "B001,Area B,Local Authority District,Gross median weekly pay,2022-04-01,200,Pounds,£,,,,\n"
    "B001,Area B,Local Authority District,Gross median weekly pay,2024-04-01,220,Pounds,£,,,,\n"
    "R001,Region A,Region,Gross median weekly pay,2023-04-01,500,Pounds,£,,,,\n"
)


def _curate(
    tmp_path: Path, *, commit: str = COMMIT_A, payload: bytes | None = None
) -> tuple[Path, dict[str, object]]:
    """Generate an actual curation receipt/tables, skipping only Git ingestion fixtures."""
    data_root = tmp_path / "curated"
    payload = PAY_CSV.encode("utf-8-sig") if payload is None else payload
    sha = hashlib.sha256(payload).hexdigest()
    storage_key = f"canonical/{DATASET}/{sha}/raw/gross-median-weekly-pay.csv"
    store = LocalBlobStore(tmp_path / "object-store")
    store.put_bytes(storage_key, payload)
    input_receipt = {
        "pipeline_name": "registry_handoff_ingestion",
        "status": "success",
        "manifest_version": 1,
        "dataset_id": DATASET,
        "run_id": f"ingestion-{commit[:8]}",
        "manifest_sha256": "1" * 64,
        "registry": {"repository": "DiogoRibeiro7/data", "commit": commit},
        "objects": [
            {
                "role": "raw",
                "status": "published",
                "source_path": SOURCE_PATH,
                "source_uri": (
                    f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{commit}/{SOURCE_PATH}"
                ),
                "sha256": sha,
                "format": "csv",
                "storage_key": storage_key,
                "size_bytes": len(payload),
            }
        ],
        "source": {
            "publisher": "Office for National Statistics",
            "url": "https://example.org/ons",
            "snapshot": "synthetic-test",
            "retrieved_at": "2026-10-31",
        },
        "citation": "ONS, Synthetic gross median weekly pay",
        "license_name": "Open Government Licence v3.0",
        "license_url": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
    }
    receipt_path = tmp_path / f"ingestion-{commit}.json"
    receipt_path.write_text(json.dumps(input_receipt), encoding="utf-8")
    result = curate_ons_pay(input_receipt=receipt_path, store=store, output_root=data_root)
    return data_root, json.loads(result.receipt_path.read_text(encoding="utf-8"))


def _pin(receipt: dict[str, object]) -> dict[str, str]:
    source = receipt["input"]
    assert isinstance(source, dict)
    return {
        "registry_commit": str(source["registry_commit"]),
        "source_sha256": str(source["source_sha256"]),
    }


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    return _curate(tmp_path)


def test_health_and_empty_catalog(tmp_path: Path) -> None:
    client = TestClient(create_app(tmp_path / "uncreated"))
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/v1/ons-pay/vintages").json() == {"vintages": []}
    missing = client.get(
        "/v1/ons-pay/geography-year",
        params={"registry_commit": COMMIT_A, "source_sha256": "f" * 64},
    )
    assert missing.status_code == 404


def test_vintage_discovery_is_sha_pinned_and_provenanced(
    curated: tuple[Path, dict[str, object]],
) -> None:
    root, receipt = curated
    client = TestClient(create_app(root))
    response = client.get("/v1/ons-pay/vintages")
    assert response.status_code == 200
    vintages = response.json()["vintages"]
    assert len(vintages) == 1
    assert vintages[0]["model_version"] == 1
    assert vintages[0]["registry_commit"] == COMMIT_A
    assert vintages[0]["source_sha256"] == _pin(receipt)["source_sha256"]
    assert (vintages[0]["source_rows"], vintages[0]["rows_without_pay"]) == (7, 1)
    assert vintages[0]["license_name"] == "Open Government Licence v3.0"
    assert vintages[0]["source_publisher"] == "Office for National Statistics"
    assert vintages[0]["manifest_sha256"] == "1" * 64


def test_geography_year_filter_pagination_and_unweighted_semantics(
    curated: tuple[Path, dict[str, object]],
) -> None:
    root, receipt = curated
    client = TestClient(create_app(root))
    path = "/v1/ons-pay/geography-year"
    params = {**_pin(receipt), "geography": "Local Authority District", "limit": 2}
    first = client.get(path, params=params)
    assert first.status_code == 200
    page = first.json()
    assert page["total"] == 4
    assert page["limit"] == 2
    assert [item["pay_year"] for item in page["rows"]] == [2022, 2023]
    assert page["rows"][0]["median_area_weekly_pay_gbp"] == 150.0
    assert page["rows"][0]["areas_total"] == 2
    second = client.get(path, params={**params, "offset": 2})
    assert [item["pay_year"] for item in second.json()["rows"]] == [2024, 2025]
    assert second.json()["rows"][0]["areas_without_pay"] == 1
    year = client.get(path, params={**params, "pay_year": 2024})
    assert year.json()["total"] == 1
    assert year.json()["rows"][0]["median_area_weekly_pay_gbp"] == 220.0


def test_area_trend_preserves_null_and_only_adjacent_nominal_growth(
    curated: tuple[Path, dict[str, object]],
) -> None:
    root, receipt = curated
    client = TestClient(create_app(root))
    params = _pin(receipt)
    response = client.get("/v1/ons-pay/areas/A001/trend", params=params)
    assert response.status_code == 200
    page = response.json()
    assert page["total"] == 4
    assert [row["pay_year"] for row in page["rows"]] == [2022, 2023, 2024, 2025]
    assert page["rows"][0]["reference_date"] == "2022-04-01"
    assert page["rows"][1]["yoy_nominal_pct"] == pytest.approx(10)
    assert page["rows"][2]["weekly_pay_gbp"] is None
    assert page["rows"][2]["status_code"] == "x"
    assert page["rows"][3]["yoy_nominal_pct"] is None
    filtered = client.get(
        "/v1/ons-pay/areas/B001/trend", params={**params, "start_year": 2024, "limit": 1}
    )
    assert filtered.json()["total"] == 1
    assert filtered.json()["rows"][0]["previous_year"] == 2022
    assert filtered.json()["rows"][0]["yoy_nominal_pct"] is None
    assert client.get("/v1/ons-pay/areas/Z999/trend", params=params).json()["rows"] == []


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/v1/ons-pay/geography-year", {"registry_commit": "x"}),
        ("/v1/ons-pay/geography-year", {"limit": 101}),
        ("/v1/ons-pay/geography-year", {"offset": -1}),
        ("/v1/ons-pay/geography-year", {"model_version": 2}),
        ("/v1/ons-pay/areas/A001/trend", {"start_year": 2025, "end_year": 2022}),
        ("/v1/ons-pay/areas/A001/trend", {"start_year": 1800}),
        ("/v1/ons-pay/areas/A001%27/trend", {}),
    ],
)
def test_invalid_requests_rejected(
    curated: tuple[Path, dict[str, object]], path: str, params: dict[str, object]
) -> None:
    root, receipt = curated
    client = TestClient(create_app(root))
    assert client.get(path, params={**_pin(receipt), **params}).status_code == 422


def test_missing_pins_rejected(curated: tuple[Path, dict[str, object]]) -> None:
    root, _ = curated
    client = TestClient(create_app(root))
    assert client.get("/v1/ons-pay/geography-year").status_code == 422
    assert client.get("/v1/ons-pay/areas/A001/trend").status_code == 422


def test_filter_values_are_sql_parameters(curated: tuple[Path, dict[str, object]]) -> None:
    root, receipt = curated
    client = TestClient(create_app(root))
    response = client.get(
        "/v1/ons-pay/geography-year",
        params={**_pin(receipt), "geography": "Region' OR 1=1 --"},
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


def test_requires_exact_source_and_commit(curated: tuple[Path, dict[str, object]]) -> None:
    root, receipt = curated
    client = TestClient(create_app(root))
    pin = _pin(receipt)
    assert (
        client.get(
            "/v1/ons-pay/geography-year", params={**pin, "registry_commit": COMMIT_B}
        ).status_code
        == 404
    )
    assert (
        client.get(
            "/v1/ons-pay/geography-year", params={**pin, "source_sha256": "f" * 64}
        ).status_code
        == 404
    )


def test_two_vintages_never_implicitly_switch(tmp_path: Path) -> None:
    root, first = _curate(tmp_path)
    _, second = _curate(tmp_path, commit=COMMIT_B)
    client = TestClient(create_app(root))
    assert len(client.get("/v1/ons-pay/vintages").json()["vintages"]) == 2
    for receipt, expected in ((first, COMMIT_A), (second, COMMIT_B)):
        response = client.get("/v1/ons-pay/geography-year", params=_pin(receipt))
        assert response.status_code == 200
        assert response.json()["vintage"]["registry_commit"] == expected


def test_idempotent_replay_receipt_accepted(curated: tuple[Path, dict[str, object]]) -> None:
    root, receipt = curated
    replay = {**receipt, "run_id": "second-curation-run"}
    for output in replay["outputs"]:
        output["status"] = "reused"
    (root / "manifests" / "ons_pay_curation_second.json").write_text(json.dumps(replay))
    assert len(CuratedCatalog(root).list_vintages()) == 1


def test_conflicting_duplicate_receipt_fails_closed(
    curated: tuple[Path, dict[str, object]],
) -> None:
    root, receipt = curated
    conflicting = {**receipt, "run_id": "conflicting-curation-run", "input": {**receipt["input"]}}
    conflicting["input"]["citation"] = "Changed and untrusted provenance"
    (root / "manifests" / "ons_pay_curation_conflicting.json").write_text(
        json.dumps(conflicting), encoding="utf-8"
    )
    response = TestClient(create_app(root)).get("/v1/ons-pay/vintages")
    assert response.status_code == 503
    assert "path" not in response.text.lower()


def test_modified_parquet_detected_before_serving(
    curated: tuple[Path, dict[str, object]],
) -> None:
    root, receipt = curated
    path = Path(receipt["outputs"][1]["path"])
    with path.open("ab") as handle:
        handle.write(b"tampered")
    client = TestClient(create_app(root))
    assert client.get("/v1/ons-pay/vintages").status_code == 503
    response = client.get("/v1/ons-pay/geography-year", params=_pin(receipt))
    assert response.status_code == 503
    assert str(root) not in response.text


def test_row_lineage_drift_detected_even_with_updated_file_hash(
    curated: tuple[Path, dict[str, object]],
) -> None:
    import duckdb

    root, receipt = curated
    output = receipt["outputs"][1]
    path = Path(output["path"])
    rewritten = path.with_name("test-rewritten.parquet")
    conn = duckdb.connect(":memory:")
    try:
        conn.execute(
            "COPY (SELECT * REPLACE (? AS source_sha256) FROM read_parquet(?)) "
            "TO ? (FORMAT PARQUET)",
            ["f" * 64, str(path), str(rewritten)],
        )
    finally:
        conn.close()
    rewritten.replace(path)
    output["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = next((root / "manifests").glob("ons_pay_curation_*.json"))
    manifest.write_text(json.dumps(receipt), encoding="utf-8")
    client = TestClient(create_app(root))
    assert client.get("/v1/ons-pay/vintages").status_code == 200
    assert client.get("/v1/ons-pay/geography-year", params=_pin(receipt)).status_code == 503


@pytest.mark.parametrize("fault", ["traversal", "missing", "symlink", "invalid_sha"])
def test_unsafe_or_missing_output_fails_closed(
    curated: tuple[Path, dict[str, object]], tmp_path: Path, fault: str
) -> None:
    root, receipt = curated
    output = receipt["outputs"][1]
    path = Path(output["path"])
    if fault == "traversal":
        output["path"] = str(tmp_path / "elsewhere" / "geography_year.parquet")
    elif fault == "missing":
        path.unlink()
    elif fault == "symlink":
        outside = tmp_path / "outside.parquet"
        outside.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(outside)
    else:
        output["sha256"] = "f" * 64
    if fault in {"traversal", "invalid_sha"}:
        manifest = next((root / "manifests").glob("ons_pay_curation_*.json"))
        manifest.write_text(json.dumps(receipt), encoding="utf-8")
    assert TestClient(create_app(root)).get("/v1/ons-pay/vintages").status_code == 503


def test_invalid_and_symlinked_manifest_fails_closed(
    curated: tuple[Path, dict[str, object]], tmp_path: Path
) -> None:
    root, _ = curated
    manifests = root / "manifests"
    (manifests / "ons_pay_curation_invalid.json").write_text("{invalid", encoding="utf-8")
    client = TestClient(create_app(root))
    assert client.get("/v1/ons-pay/vintages").status_code == 503
    (manifests / "ons_pay_curation_invalid.json").unlink()
    external = tmp_path / "external.json"
    external.write_text("{}", encoding="utf-8")
    (manifests / "ons_pay_curation_link.json").symlink_to(external)
    assert client.get("/v1/ons-pay/vintages").status_code == 503


def test_openapi_and_cli_dispatch(curated: tuple[Path, dict[str, object]]) -> None:
    root, _ = curated
    client = TestClient(create_app(root))
    schema = client.get("/openapi.json").json()
    assert "/v1/ons-pay/vintages" in schema["paths"]
    assert "/v1/ons-pay/geography-year" in schema["paths"]
    assert "/v1/ons-pay/areas/{area_code}/trend" in schema["paths"]
    with patch("uvicorn.run") as run:
        platform_main(["ons-pay-serve", "--data-root", str(root), "--port", "8765"])
    assert run.call_count == 1
    assert run.call_args.kwargs == {"host": "127.0.0.1", "port": 8765}
    assert run.call_args.args[0].title == "Data Platform Lab — ONS pay serving"
