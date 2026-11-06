"""Exercise real pipeline outputs and manifests together, with no external services."""

from __future__ import annotations

import csv
import json
import re
import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from conftest import SAMPLE_DIR, SQL_DIR, write_jsonl
from dataexcept import FileWriteError

from data_platform_lab.demo import run_demo
from data_platform_lab.ingestion import api_pipeline
from data_platform_lab.ingestion.csv_pipeline import run_pipeline
from data_platform_lab.manifest import read_manifest, validate_manifest
from data_platform_lab.sensor_demo import run_sensor_demo
from data_platform_lab.streaming.processor import process_stream
from data_platform_lab.transform.incremental_etl import run_incremental_etl
from data_platform_lab.warehouse.loader import run_warehouse_pipeline

PIPELINES = (
    "csv_ingestion",
    "api_ingestion",
    "events_etl",
    "sensor_stream",
    "sensor_demo",
    "ecommerce_demo",
    "warehouse",
)
API_URL = "https://example.invalid/posts"
POSTS = [
    {"id": 1, "userId": 1, "title": "First post", "body": "Hello world"},
    {"id": 2, "userId": 2, "title": "Second post", "body": "Another post"},
]


def _run(name: str, root: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Run an exercise in isolation, replacing only the API network boundary."""
    output = root / "output"
    output.mkdir()
    source = root / "input"
    source.mkdir()
    if name == "csv_ingestion":
        (source / "rows.csv").write_text("id,name\n1,Ada\n1,Ada\n2,Bob\n", encoding="utf-8")
        return asdict(run_pipeline(source, output / "merged.csv", ["id", "name"]))
    if name == "api_ingestion":
        monkeypatch.setattr(api_pipeline, "fetch_all_pages", lambda *args, **kwargs: (POSTS, 1))
        return asdict(api_pipeline.run_api_pipeline(API_URL, output / "raw", output / "processed"))
    if name == "events_etl":
        write_jsonl(
            source / "events.jsonl",
            [
                {"event_id": "e1", "type": "page_view", "timestamp": "2024-06-01T10:00:00Z"},
                {"event_id": "e2", "type": "checkout", "timestamp": "2024-06-01T11:00:00Z"},
            ],
        )
        return asdict(run_incremental_etl(source, output / "events", root / "checkpoint.json"))
    if name == "sensor_stream":
        return asdict(process_stream(SAMPLE_DIR / "sensor_events.json", output))
    if name == "sensor_demo":
        return run_sensor_demo(SAMPLE_DIR, output)
    if name == "ecommerce_demo":
        return run_demo(SAMPLE_DIR, output)
    assert name == "warehouse"
    return run_warehouse_pipeline(
        data_dir=SAMPLE_DIR,
        db_path=str(output / "warehouse.db"),
        report_dir=output / "reports",
        sql_dir=SQL_DIR,
    )


def _jsonl_count(path: Path) -> int:
    """Count parsed records rather than trusting a returned summary."""
    return len([json.loads(line) for line in path.read_text().splitlines() if line.strip()])


@pytest.mark.parametrize("name", PIPELINES)
def test_pipeline_manifest_matches_persisted_output(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every shared-writer pipeline records real sources, outputs, counts and run metadata."""
    monkeypatch.chdir(tmp_path)
    result = _run(name, tmp_path, monkeypatch)
    path = Path(result["manifest_path"]).resolve()
    assert path.parent == tmp_path / "data" / "manifests"
    assert list(path.parent.glob("*.json")) == [path]
    manifest = read_manifest(path)
    assert validate_manifest(manifest) == []
    assert manifest["pipeline_name"] == name
    assert manifest["status"] == "success"
    assert re.fullmatch(r"\d{8}_\d{6}", manifest["run_id"])
    assert path.name == f"{name}_{manifest['run_id']}.json"
    assert datetime.fromisoformat(manifest["created_at"]).utcoffset() is not None

    output = tmp_path / "output"
    if name == "csv_ingestion":
        assert manifest["source"] == [str(tmp_path / "input" / "rows.csv")]
        assert manifest["output"] == str(output / "merged.csv")
        with Path(manifest["output"]).open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        assert manifest["row_count"] == len(rows) == 2
        assert manifest["schema_hint"] == ["id", "name"]
        assert manifest["rows_read"] == 3
        assert manifest["duplicates_removed"] == 1
    elif name == "api_ingestion":
        assert manifest["source"] == API_URL
        assert manifest["run_id"] == result["run_id"]
        assert manifest["output"] == result["processed_path"]
        assert manifest["row_count"] == len(json.loads(Path(manifest["output"]).read_text())) == 2
        assert json.loads(Path(manifest["raw_path"]).read_text()) == POSTS
        assert manifest["pages_fetched"] == 1
    elif name == "events_etl":
        assert manifest["source"] == str(tmp_path / "input")
        assert Path(manifest["output"]).parent == output / "events"
        assert manifest["row_count"] == _jsonl_count(Path(manifest["output"])) == 2
        assert manifest["checkpoint_updated"] is True
        assert set(json.loads((tmp_path / "checkpoint.json").read_text())["processed_ids"]) == {
            "e1",
            "e2",
        }
    elif name in {"sensor_stream", "sensor_demo"}:
        assert manifest["source"] == str(SAMPLE_DIR / "sensor_events.json")
        assert manifest["row_count"] == _jsonl_count(output / "accepted.jsonl") == 14
        if name == "sensor_stream":
            assert manifest["output"] == str(output / "accepted.jsonl")
            assert manifest["events_seen"] == 16
            assert manifest["events_rejected"] == manifest["events_duplicate"] == 1
            assert _jsonl_count(Path(manifest["dead_letter_path"])) == 2
        else:
            assert str(output / "accepted.jsonl") in manifest["output"]
            assert all(Path(file).is_file() for file in manifest["output"])
            assert result["pipeline_result"].status == "success"
    elif name == "ecommerce_demo":
        table_names = ("customers", "products", "orders", "order_items")
        assert manifest["source"] == [str(SAMPLE_DIR / f"{table}.csv") for table in table_names]
        assert manifest["output"] == [str(output / f"{table}.csv") for table in table_names]
        assert manifest["run_id"] == result["metadata"].run_id
        assert manifest["status"] == manifest["run"]["status"]
        counts = {}
        for table, file in zip(table_names, manifest["output"], strict=True):
            with Path(file).open(newline="", encoding="utf-8") as stream:
                counts[table] = len(list(csv.DictReader(stream)))
            assert manifest["tables"][table]["rows_out"] == counts[table]
        assert counts == {"customers": 12, "products": 11, "orders": 15, "order_items": 19}
        assert manifest["row_count"] == manifest["run"]["rows_written"] == sum(counts.values())
    else:
        assert manifest["source"] == str(SAMPLE_DIR)
        assert manifest["output"] == str(output / "reports")
        assert manifest["db_path"] == str(output / "warehouse.db")
        connection = sqlite3.connect(manifest["db_path"])
        try:
            counts = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "dim_customer",
                    "dim_product",
                    "dim_date",
                    "fact_order",
                    "fact_order_item",
                    "fact_event",
                )
            }
        finally:
            connection.close()
        assert manifest["warehouse_tables"] == counts
        assert manifest["row_count"] == sum(counts.values()) > 0
        assert (
            json.loads((output / "reports" / "warehouse_summary.json").read_text())["status"]
            == "success"
        )


@pytest.mark.parametrize("name", PIPELINES)
def test_pipeline_manifest_write_failure_is_visible(
    name: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real filesystem failure must not silently return an unrecorded run as successful."""
    monkeypatch.chdir(tmp_path)
    manifest_dir = tmp_path / "data" / "manifests"
    manifest_dir.parent.mkdir()
    manifest_dir.write_text("blocked", encoding="utf-8")
    with pytest.raises(FileWriteError) as error:
        _run(name, tmp_path, monkeypatch)
    assert "manifests" in str(error.value)
    assert isinstance(error.value.__cause__, OSError)
    assert manifest_dir.read_text() == "blocked"
    assert any(path.is_file() for path in (tmp_path / "output").rglob("*"))


def test_failed_sensor_run_records_failure(tmp_path: Path) -> None:
    """An orchestration failure must not be recorded as a successful publication."""
    result = run_sensor_demo(tmp_path / "missing", tmp_path / "output", tmp_path / "manifests")
    manifest = read_manifest(result["manifest_path"])
    assert result["pipeline_result"].status == manifest["status"] == "failed"
    assert manifest["row_count"] == 0
