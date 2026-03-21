"""Tests for the sensor pipeline demo."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import SAMPLE_DIR as DATA_DIR

from data_platform_lab.sensor_demo import run_sensor_demo

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(tmp_path: Path) -> dict:
    """Run the sensor demo into *tmp_path* and return the result dict."""
    output_dir = tmp_path / "output"
    manifest_dir = tmp_path / "manifests"
    return run_sensor_demo(
        data_dir=str(DATA_DIR),
        output_dir=str(output_dir),
        manifest_dir=str(manifest_dir),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_run_sensor_demo_success(tmp_path: Path) -> None:
    """Pipeline runs to completion with status 'success' and all 5 steps passed."""
    result = _run(tmp_path)
    pr = result["pipeline_result"]
    assert pr.status == "success"
    assert pr.steps_passed == 5


def test_run_sensor_demo_accepted_output(tmp_path: Path) -> None:
    """accepted.jsonl exists and contains 14 valid deduplicated events."""
    result = _run(tmp_path)
    accepted_path = Path(result["output_dir"]) / "accepted.jsonl"
    assert accepted_path.exists()
    text = accepted_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    assert len(lines) == 14


def test_run_sensor_demo_dead_letter_output(tmp_path: Path) -> None:
    """dead_letter.jsonl exists with 2 entries (1 rejected + 1 duplicate)."""
    result = _run(tmp_path)
    dead_letter_path = Path(result["output_dir"]) / "dead_letter.jsonl"
    assert dead_letter_path.exists()
    text = dead_letter_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    assert len(lines) == 2


def test_run_sensor_demo_hourly_aggregates(tmp_path: Path) -> None:
    """hourly_aggregates.csv exists with a header row plus data rows."""
    result = _run(tmp_path)
    agg_path = Path(result["output_dir"]) / "hourly_aggregates.csv"
    assert agg_path.exists()
    text = agg_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    # At least header + 1 data row
    assert len(lines) >= 2
    assert lines[0].startswith("sensor_id")


def test_run_sensor_demo_location_summary(tmp_path: Path) -> None:
    """location_summary.csv exists with 3 locations (header + 3 data rows)."""
    result = _run(tmp_path)
    loc_path = Path(result["output_dir"]) / "location_summary.csv"
    assert loc_path.exists()
    text = loc_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    # header + 3 locations
    assert len(lines) == 4


def test_run_sensor_demo_summary_json(tmp_path: Path) -> None:
    """summary.json exists and contains expected keys."""
    result = _run(tmp_path)
    summary_path = Path(result["output_dir"]) / "summary.json"
    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    expected_keys = {
        "pipeline_name",
        "events_read",
        "accepted",
        "rejected",
        "duplicates_removed",
        "hourly_buckets",
        "locations",
        "step_results",
    }
    assert expected_keys.issubset(summary.keys())


def test_run_sensor_demo_manifest(tmp_path: Path) -> None:
    """manifest_path is non-empty and the manifest file exists on disk."""
    result = _run(tmp_path)
    manifest_path = result["manifest_path"]
    assert manifest_path
    assert Path(manifest_path).exists()


def test_run_sensor_demo_idempotent(tmp_path: Path) -> None:
    """Running twice to the same output dir overwrites, not appends."""
    output_dir = tmp_path / "output"
    manifest_dir = tmp_path / "manifests"
    kwargs = {
        "data_dir": str(DATA_DIR),
        "output_dir": str(output_dir),
        "manifest_dir": str(manifest_dir),
    }

    run_sensor_demo(**kwargs)
    run_sensor_demo(**kwargs)

    accepted_path = output_dir / "accepted.jsonl"
    text = accepted_path.read_text(encoding="utf-8")
    lines = [line for line in text.splitlines() if line.strip()]
    assert len(lines) == 14
