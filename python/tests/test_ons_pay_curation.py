"""Offline end-to-end regression tests for versioned DuckDB pay models."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.curation.ons_pay import (
    CurationError,
    curate_ons_pay,
    parse_ingestion_receipt,
)
from data_platform_lab.ingestion.registry_handoff import ingest_handoff
from data_platform_lab.storage import LocalBlobStore

SOURCE_PATH = "datasets/ons-gross-median-weekly-pay/raw/gross-median-weekly-pay.csv"
FIELDS = (
    "areacd,areanm,geography,indicator,period,observation,measure,unit,lci_95,uci_95,status,notes\n"
)
BASE_CSV = (
    FIELDS + "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2022-04-01,100,Pounds,£,90,110,,\n"
    + "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2023-04-01,110,Pounds,£,100,120,,\n"
    + "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2024-04-01,,Pounds,£,,,x,\n" + "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2025-04-01,130,Pounds,£,120,140,,\n"
    + "B001,Area B,Local Authority District,Gross median weekly pay,"
    "2022-04-01,200,Pounds,£,,,,\n"
    + "B001,Area B,Local Authority District,Gross median weekly pay,"
    "2024-04-01,220,Pounds,£,,,,\n" + "R001,Region A,Region,Gross median weekly pay,"
    "2023-04-01,500,Pounds,£,,,,\n"
)


def _run_git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _ingested_fixture(tmp_path: Path, csv_text: str = BASE_CSV) -> tuple[LocalBlobStore, Path]:
    """Build a real Git-backed v1 handoff and execute the previous milestone."""
    registry = tmp_path / "registry"
    source = registry / SOURCE_PATH
    source.parent.mkdir(parents=True)
    payload = csv_text.encode("utf-8-sig")
    source.write_bytes(payload)
    _run_git(registry, "init", "-q")
    _run_git(registry, "add", ".")
    _run_git(
        registry,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.org",
        "commit",
        "-qm",
        "source",
    )
    commit = _run_git(registry, "rev-parse", "HEAD")
    sha = hashlib.sha256(payload).hexdigest()
    manifest: dict[str, Any] = {
        "manifest_version": 1,
        "dataset_id": "ons-gross-median-weekly-pay",
        "title": "Synthetic ONS pay",
        "lifecycle_status": "active",
        "registry": {"repository": "DiogoRibeiro7/data", "commit": commit},
        "source": {
            "publisher": "Office for National Statistics",
            "url": "https://example.org/ons",
            "snapshot": "synthetic",
            "retrieved_at": "2026-10-31",
        },
        "license": {
            "name": "Open Government Licence v3.0",
            "url": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
            "redistribution": "allowed",
        },
        "citation": "ONS, Synthetic gross median weekly pay",
        "local_bytes_verified": True,
        "objects": [
            {
                "source_path": SOURCE_PATH,
                "source_uri": f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{commit}/{SOURCE_PATH}",
                "sha256": sha,
                "role": "raw",
                "format": "csv",
                "storage_key": (
                    f"canonical/ons-gross-median-weekly-pay/{sha}/raw/gross-median-weekly-pay.csv"
                ),
                "size_bytes": len(payload),
            }
        ],
    }
    handoff = tmp_path / "handoff.json"
    handoff.write_text(json.dumps(manifest), encoding="utf-8")
    store = LocalBlobStore(tmp_path / "objects")
    ingested = ingest_handoff(
        manifest_path=handoff,
        registry_root=registry,
        store=store,
        receipt_dir=tmp_path / "ingestion-receipts",
    )
    return store, ingested.receipt_path


def _parquet_rows(
    path: Path, sql: str, params: list[object] | None = None
) -> list[tuple[Any, ...]]:
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        return conn.execute(sql, [str(path), *(params or [])]).fetchall()
    finally:
        conn.close()


def test_end_to_end_silver_preserves_all_rows_and_suppression(tmp_path: Path) -> None:
    store, ingestion_receipt = _ingested_fixture(tmp_path)
    result = curate_ons_pay(
        input_receipt=ingestion_receipt, store=store, output_root=tmp_path / "curated"
    )
    assert result.source_rows == 7
    assert result.rows_with_pay == 6
    assert result.rows_without_pay == 1
    assert len(result.outputs) == 3
    silver = result.outputs[0]
    assert silver.layer == "silver"
    assert silver.rows == 7
    rows = _parquet_rows(
        silver.path,
        "SELECT weekly_pay_gbp, lci_95_gbp, uci_95_gbp, status_code, "
        "source_sha256, registry_commit FROM read_parquet(?) "
        "WHERE area_code='A001' AND pay_year=2024",
    )
    assert len(rows) == 1
    assert rows[0][:4] == (None, None, None, "x")
    assert len(rows[0][4]) == 64
    assert len(rows[0][5]) == 40
    receipt = json.loads(result.receipt_path.read_text())
    assert receipt["status"] == "success"
    assert receipt["model_version"] == 1
    assert receipt["quality"]["source_rows"] == 7
    assert receipt["input"]["source_sha256"] == rows[0][4]
    assert receipt["input"]["citation"].startswith("ONS")


def test_gold_geography_summary_is_unweighted_with_missing_counts(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    result = curate_ons_pay(input_receipt=receipt, store=store, output_root=tmp_path / "curated")
    summary = result.outputs[1]
    rows = _parquet_rows(
        summary.path,
        "SELECT areas_total, areas_with_pay, areas_without_pay, median_area_weekly_pay_gbp "
        "FROM read_parquet(?) WHERE geography='Local Authority District' AND pay_year=2022",
    )
    assert rows == [(2, 2, 0, 150.0)]
    missing = _parquet_rows(
        summary.path,
        "SELECT areas_total, areas_with_pay, areas_without_pay, median_area_weekly_pay_gbp "
        "FROM read_parquet(?) WHERE geography='Local Authority District' AND pay_year=2024",
    )
    assert missing == [(2, 1, 1, 220.0)]


def test_gold_yoy_uses_adjacent_observed_years_only(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    result = curate_ons_pay(input_receipt=receipt, store=store, output_root=tmp_path / "curated")
    trends = result.outputs[2]
    assert trends.rows == 6
    rows = _parquet_rows(
        trends.path,
        "SELECT area_code, pay_year, yoy_nominal_pct FROM read_parquet(?) "
        "ORDER BY area_code,pay_year",
    )
    assert rows == [
        ("A001", 2022, None),
        ("A001", 2023, pytest.approx(10.0)),
        ("A001", 2024, None),
        ("A001", 2025, None),  # Suppressed predecessor.
        ("B001", 2022, None),
        ("B001", 2024, None),  # A year was skipped; no fake 2023 base.
    ]


def test_replay_reuses_identical_parquet_without_rewriting(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    root = tmp_path / "curated"
    first = curate_ons_pay(input_receipt=receipt, store=store, output_root=root)
    timestamps = [entry.path.stat().st_mtime_ns for entry in first.outputs]
    second = curate_ons_pay(input_receipt=receipt, store=store, output_root=root)
    assert first.receipt_path != second.receipt_path
    assert all(item.reused for item in second.outputs)
    assert timestamps == [entry.path.stat().st_mtime_ns for entry in second.outputs]
    assert [item.sha256 for item in first.outputs] == [item.sha256 for item in second.outputs]
    assert len(list((root / "manifests").glob("ons_pay_curation_*.json"))) == 2


def test_tampered_ingested_object_does_not_produce_curated_output(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    lineage = parse_ingestion_receipt(json.loads(receipt.read_text()))
    store.put_bytes(lineage.storage_key, b"tampered")
    with pytest.raises(CurationError, match="SHA-256/byte count"):
        curate_ons_pay(input_receipt=receipt, store=store, output_root=tmp_path / "curated")
    assert not (tmp_path / "curated").exists()


@pytest.mark.parametrize(
    ("changed_csv", "message"),
    [
        (BASE_CSV.replace("notes\n", "extra\n", 1), "schema drift"),
        (BASE_CSV.replace(",2022-04-01,100,", ",not-a-date,100,", 1), "invalid_period"),
        (BASE_CSV.replace(",2022-04-01,100,", ",2022-04-01,nope,", 1), "invalid_number"),
        (BASE_CSV.replace(",2022-04-01,100,", ",2022-04-01,-10,", 1), "invalid_negative"),
        (BASE_CSV.replace(",£,90,110,,", ",£,120,130,,", 1), "invalid_confidence_interval"),
        (
            BASE_CSV.replace(",2024-04-01,,Pounds,£,,,x,", ",2024-04-01,,Pounds,£,,,,", 1),
            "invalid_suppression",
        ),
        (BASE_CSV.replace(",Pounds,£,90,110", ",Euros,€,90,110", 1), "invalid_measure"),
        (BASE_CSV + BASE_CSV.splitlines()[1] + "\n", "invalid_duplicate_area_year"),
    ],
)
def test_quality_gates_block_invalid_source_before_any_output(
    tmp_path: Path, changed_csv: str, message: str
) -> None:
    store, receipt = _ingested_fixture(tmp_path, changed_csv)
    root = tmp_path / "curated"
    with pytest.raises(CurationError, match=message):
        curate_ons_pay(input_receipt=receipt, store=store, output_root=root)
    assert not list(root.rglob("*.parquet"))
    assert not list(root.rglob("ons_pay_curation_*.json"))


def test_existing_corrupt_parquet_fails_closed(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    root = tmp_path / "curated"
    first = curate_ons_pay(input_receipt=receipt, store=store, output_root=root)
    first.outputs[0].path.write_bytes(b"corrupt")
    with pytest.raises(CurationError, match="existing immutable Parquet output differs"):
        curate_ons_pay(input_receipt=receipt, store=store, output_root=root)
    assert first.outputs[0].path.read_bytes() == b"corrupt"
    assert len(list((root / "manifests").glob("ons_pay_curation_*.json"))) == 1


def test_rejects_forged_or_failed_receipt(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    content = json.loads(receipt.read_text())
    content["status"] = "failed"
    receipt.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(CurationError, match="successful ONS registry-handoff"):
        curate_ons_pay(input_receipt=receipt, store=store, output_root=tmp_path / "curated")


def test_rejects_malformed_nested_receipt_objects(tmp_path: Path) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    content = json.loads(receipt.read_text())
    content["objects"].append("malformed")
    receipt.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(CurationError, match=r"receipt\.objects item"):
        curate_ons_pay(input_receipt=receipt, store=store, output_root=tmp_path / "curated")


def test_missing_optional_duckdb_dependency_has_install_hint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from dataexcept import DependencyError

    store, receipt = _ingested_fixture(tmp_path)

    def blocked_import(_module: str) -> object:
        raise ModuleNotFoundError("duckdb")

    monkeypatch.setattr("data_platform_lab.curation.ons_pay.import_module", blocked_import)
    with pytest.raises(DependencyError, match="analytics extra"):
        curate_ons_pay(input_receipt=receipt, store=store, output_root=tmp_path / "curated")


def test_root_cli_runs_local_model_and_prints_paths(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store, receipt = _ingested_fixture(tmp_path)
    platform_main(
        [
            "ons-pay-curate",
            "--receipt",
            str(receipt),
            "--object-root",
            str(store.root),
            "--output-root",
            str(tmp_path / "curated"),
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["source_rows"] == 7
    assert len(result["outputs"]) == 3
    assert Path(result["receipt_path"]).is_file()


def test_source_header_list_is_explicit_for_portfolio_contract() -> None:
    """Document the twelve ONS fields rather than relying on inferred columns."""
    assert next(csv.reader([FIELDS])) == [
        "areacd",
        "areanm",
        "geography",
        "indicator",
        "period",
        "observation",
        "measure",
        "unit",
        "lci_95",
        "uci_95",
        "status",
        "notes",
    ]
