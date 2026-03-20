"""Tests for the shared manifest utility."""

from __future__ import annotations

import json
import re
from pathlib import Path

from data_platform_lab.manifest import (
    MANIFEST_REQUIRED_KEYS,
    generate_run_id,
    read_manifest,
    validate_manifest,
    write_manifest,
)

# ---------------------------------------------------------------------------
# generate_run_id
# ---------------------------------------------------------------------------


def test_generate_run_id_format() -> None:
    """Returns string matching YYYYMMDD_HHMMSS (15 chars, underscore at pos 8)."""
    run_id = generate_run_id()

    assert len(run_id) == 15
    assert run_id[8] == "_"
    assert re.fullmatch(r"\d{8}_\d{6}", run_id), f"Unexpected format: {run_id}"


# ---------------------------------------------------------------------------
# write_manifest
# ---------------------------------------------------------------------------


def test_write_manifest_creates_file(tmp_path: Path) -> None:
    """write_manifest writes a JSON file that exists on disk."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="test_pipe",
        run_id="20260101_120000",
        source="input.csv",
        output="output.csv",
        row_count=42,
        manifest_dir=manifest_dir,
    )

    assert path.exists()
    assert path.suffix == ".json"


def test_write_manifest_required_fields(tmp_path: Path) -> None:
    """All MANIFEST_REQUIRED_KEYS are present in the written manifest."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="test_pipe",
        run_id="20260101_120000",
        source="input.csv",
        output="output.csv",
        row_count=42,
        manifest_dir=manifest_dir,
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    for key in MANIFEST_REQUIRED_KEYS:
        assert key in data, f"Missing required key: {key}"


def test_write_manifest_with_schema_hint(tmp_path: Path) -> None:
    """schema_hint appears in the manifest when provided."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="test_pipe",
        run_id="20260101_120000",
        source="input.csv",
        output="output.csv",
        row_count=10,
        schema_hint=["id", "name", "email"],
        manifest_dir=manifest_dir,
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["schema_hint"] == ["id", "name", "email"]


def test_write_manifest_with_warnings(tmp_path: Path) -> None:
    """warnings list appears in the manifest when provided."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="test_pipe",
        run_id="20260101_120000",
        source="input.csv",
        output="output.csv",
        row_count=10,
        warnings=["skipped 2 rows", "column mismatch"],
        manifest_dir=manifest_dir,
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["warnings"] == ["skipped 2 rows", "column mismatch"]


def test_write_manifest_with_extras(tmp_path: Path) -> None:
    """Extra keys from the extras dict are merged into the manifest."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="test_pipe",
        run_id="20260101_120000",
        source="input.csv",
        output="output.csv",
        row_count=10,
        extras={"duplicates_removed": 3, "files_processed": ["a.csv", "b.csv"]},
        manifest_dir=manifest_dir,
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["duplicates_removed"] == 3
    assert data["files_processed"] == ["a.csv", "b.csv"]


def test_write_manifest_without_optional_fields(tmp_path: Path) -> None:
    """schema_hint, warnings, and extras keys are absent when not provided."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="test_pipe",
        run_id="20260101_120000",
        source="input.csv",
        output="output.csv",
        row_count=10,
        manifest_dir=manifest_dir,
    )

    data = json.loads(path.read_text(encoding="utf-8"))
    assert "schema_hint" not in data
    assert "warnings" not in data


# ---------------------------------------------------------------------------
# read_manifest
# ---------------------------------------------------------------------------


def test_read_manifest(tmp_path: Path) -> None:
    """Write then read — values round-trip correctly."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="roundtrip",
        run_id="20260101_120000",
        source="src.csv",
        output="dst.csv",
        row_count=99,
        schema_hint=["col_a", "col_b"],
        manifest_dir=manifest_dir,
    )

    data = read_manifest(path)

    assert data["pipeline_name"] == "roundtrip"
    assert data["run_id"] == "20260101_120000"
    assert data["source"] == "src.csv"
    assert data["output"] == "dst.csv"
    assert data["row_count"] == 99
    assert data["schema_hint"] == ["col_a", "col_b"]


# ---------------------------------------------------------------------------
# validate_manifest
# ---------------------------------------------------------------------------


def test_validate_manifest_valid(tmp_path: Path) -> None:
    """A complete manifest has no missing keys."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="valid",
        run_id="20260101_120000",
        source="in.csv",
        output="out.csv",
        row_count=1,
        manifest_dir=manifest_dir,
    )

    data = read_manifest(path)
    missing = validate_manifest(data)
    assert missing == []


def test_validate_manifest_missing_keys() -> None:
    """Manifest missing 'source' and 'output' returns those two keys."""
    data = {
        "pipeline_name": "incomplete",
        "run_id": "20260101_120000",
        "created_at": "2026-01-01T12:00:00+00:00",
        "row_count": 0,
        "status": "success",
    }

    missing = validate_manifest(data)
    assert set(missing) == {"source", "output"}


# ---------------------------------------------------------------------------
# source as list
# ---------------------------------------------------------------------------


def test_write_manifest_source_as_list(tmp_path: Path) -> None:
    """source can be a list of strings and serialises correctly."""
    manifest_dir = tmp_path / "manifests"
    path = write_manifest(
        pipeline_name="multi_src",
        run_id="20260101_120000",
        source=["a.csv", "b.csv", "c.csv"],
        output="merged.csv",
        row_count=30,
        manifest_dir=manifest_dir,
    )

    data = read_manifest(path)
    assert data["source"] == ["a.csv", "b.csv", "c.csv"]


# ---------------------------------------------------------------------------
# Integration: CSV pipeline generates a manifest
# ---------------------------------------------------------------------------


def _write_csv(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_csv_pipeline_generates_manifest(tmp_path: Path, monkeypatch: object) -> None:
    """CSV pipeline produces a manifest file with correct fields."""
    from data_platform_lab.ingestion.csv_pipeline import run_pipeline

    # Change CWD so the manifest is written under tmp_path/data/manifests
    monkeypatch.chdir(tmp_path)  # type: ignore[union-attr]

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    _write_csv(input_dir / "data.csv", "id,name\n1,Alice\n2,Bob\n")

    output_path = tmp_path / "output.csv"
    result = run_pipeline(input_dir, output_path)

    assert hasattr(result, "manifest_path")

    if result.manifest_path:
        manifest_file = Path(result.manifest_path)
        assert manifest_file.exists(), f"Manifest not found at {manifest_file}"

        data = read_manifest(manifest_file)
        missing = validate_manifest(data)
        assert missing == [], f"Manifest missing keys: {missing}"
        assert data["pipeline_name"] == "csv_ingestion"
        assert data["row_count"] == 2
        assert data["status"] == "success"


# ---------------------------------------------------------------------------
# Integration: streaming pipeline generates a manifest
# ---------------------------------------------------------------------------


def test_streaming_generates_manifest(tmp_path: Path, monkeypatch: object) -> None:
    """Streaming processor produces a manifest_path in its summary."""
    from data_platform_lab.streaming.processor import process_stream

    # Change CWD so the manifest is written under tmp_path/data/manifests
    monkeypatch.chdir(tmp_path)  # type: ignore[union-attr]

    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        '{"sensor_id":"s1","type":"temperature","value":22.5,'
        '"unit":"C","location":"room-1","timestamp":"2026-01-01T12:00:00Z"}\n'
        '{"sensor_id":"s2","type":"humidity","value":55.0,'
        '"unit":"%","location":"room-2","timestamp":"2026-01-01T12:01:00Z"}\n',
        encoding="utf-8",
    )

    output_dir = tmp_path / "stream_output"
    summary = process_stream(events_path, output_dir)

    assert hasattr(summary, "manifest_path")

    if summary.manifest_path:
        manifest_file = Path(summary.manifest_path)
        assert manifest_file.exists(), f"Manifest not found at {manifest_file}"

        data = read_manifest(manifest_file)
        missing = validate_manifest(data)
        assert missing == [], f"Manifest missing keys: {missing}"
        assert data["pipeline_name"] == "sensor_stream"
        assert data["row_count"] == 2
