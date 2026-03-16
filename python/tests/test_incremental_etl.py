"""Tests for the incremental ETL pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from data_platform_lab.transform.incremental_etl import (
    Checkpoint,
    RunSummary,
    load_checkpoint,
    read_events,
    run_incremental_etl,
    save_checkpoint,
    transform_event,
)

# ---------------------------------------------------------------------------
# Shared helpers and test data
# ---------------------------------------------------------------------------

SAMPLE_EVENTS: list[dict[str, object]] = [
    {
        "event_id": "evt-001",
        "type": "page_view",
        "user_id": "U001",
        "page": "/home",
        "timestamp": "2024-06-01T10:00:00Z",
    },
    {
        "event_id": "evt-002",
        "type": "checkout",
        "user_id": "U002",
        "order_id": "ORD-001",
        "timestamp": "2024-06-01T11:30:00Z",
    },
    {
        "event_id": "evt-003",
        "type": "add_to_cart",
        "user_id": None,
        "product_id": "P001",
        "timestamp": "2024-06-01T12:00:00Z",
    },
]


def write_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    """Write a list of event dicts as a JSONL file."""
    with open(path, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


# ===================================================================
# Checkpoint persistence
# ===================================================================


class TestLoadCheckpoint:
    """Tests for :func:`load_checkpoint`."""

    def test_load_checkpoint_no_file(self, tmp_path: Path) -> None:
        """Loading from a nonexistent path returns an empty checkpoint."""
        cp = load_checkpoint(tmp_path / "missing.json", "my_pipeline")
        assert cp.pipeline_name == "my_pipeline"
        assert cp.last_run_at is None
        assert cp.processed_ids == set()
        assert cp.total_runs == 0

    def test_load_checkpoint_corrupted_file(self, tmp_path: Path) -> None:
        """Corrupted checkpoint JSON returns an empty checkpoint instead of crashing."""
        cp_path = tmp_path / "bad.json"
        cp_path.write_text("{this is not valid json", encoding="utf-8")
        cp = load_checkpoint(cp_path, "my_pipeline")
        assert cp.pipeline_name == "my_pipeline"
        assert cp.processed_ids == set()
        assert cp.total_runs == 0

    def test_save_and_load_checkpoint(self, tmp_path: Path) -> None:
        """Round-trip save/load preserves all checkpoint data."""
        cp_path = tmp_path / "checkpoint.json"
        original = Checkpoint(
            pipeline_name="test_pipe",
            last_run_at="2024-06-01T10:00:00Z",
            processed_ids={"evt-001", "evt-002"},
            total_runs=5,
        )
        save_checkpoint(cp_path, original)

        loaded = load_checkpoint(cp_path, "test_pipe")
        assert loaded.pipeline_name == original.pipeline_name
        assert loaded.last_run_at == original.last_run_at
        assert loaded.processed_ids == original.processed_ids
        assert loaded.total_runs == original.total_runs


# ===================================================================
# Reading events
# ===================================================================


class TestReadEvents:
    """Tests for :func:`read_events`."""

    def test_read_events_from_jsonl(self, tmp_path: Path) -> None:
        """Reads events from JSONL files and skips blank lines."""
        content = (
            json.dumps(SAMPLE_EVENTS[0]) + "\n"
            + "\n"  # blank line
            + json.dumps(SAMPLE_EVENTS[1]) + "\n"
        )
        (tmp_path / "events.jsonl").write_text(content)

        events = read_events(tmp_path)
        assert len(events) == 2
        assert events[0]["event_id"] == "evt-001"
        assert events[1]["event_id"] == "evt-002"

    def test_read_events_empty_dir(self, tmp_path: Path) -> None:
        """Empty directory returns an empty list."""
        events = read_events(tmp_path)
        assert events == []

    def test_read_events_skips_malformed_json(self, tmp_path: Path) -> None:
        """Malformed JSON lines are skipped instead of crashing the pipeline."""
        content = (
            json.dumps(SAMPLE_EVENTS[0]) + "\n"
            + "NOT VALID JSON\n"
            + json.dumps(SAMPLE_EVENTS[1]) + "\n"
        )
        (tmp_path / "mixed.jsonl").write_text(content)
        events = read_events(tmp_path)
        assert len(events) == 2
        assert events[0]["event_id"] == "evt-001"
        assert events[1]["event_id"] == "evt-002"


# ===================================================================
# Transform
# ===================================================================


class TestTransformEvent:
    """Tests for :func:`transform_event`."""

    def test_transform_event_valid(self) -> None:
        """Transforms a valid event with all enrichment fields."""
        event = {
            "event_id": "evt-001",
            "type": "checkout",
            "user_id": "U001",
            "timestamp": "2024-06-01T11:30:00Z",
        }
        result = transform_event(event)
        assert result is not None
        assert result["event_date"] == "2024-06-01"
        assert result["hour"] == 11
        assert result["is_purchase"] is True
        assert result["has_user"] is True
        assert "processed_at" in result
        # Original fields preserved
        assert result["event_id"] == "evt-001"

    def test_transform_event_missing_fields(self) -> None:
        """Returns None for events missing required fields."""
        # Missing event_id
        assert transform_event({"type": "click", "timestamp": "2024-06-01T10:00:00Z"}) is None
        # Missing timestamp
        assert transform_event({"event_id": "x", "type": "click"}) is None
        # Missing type
        assert transform_event({"event_id": "x", "timestamp": "2024-06-01T10:00:00Z"}) is None

    def test_transform_event_bad_timestamp(self) -> None:
        """Returns None for events with unparseable timestamps."""
        event = {
            "event_id": "evt-bad",
            "type": "click",
            "timestamp": "not-a-date",
        }
        assert transform_event(event) is None

    def test_transform_event_null_user(self) -> None:
        """has_user is False when user_id is null."""
        event = {
            "event_id": "evt-003",
            "type": "add_to_cart",
            "user_id": None,
            "timestamp": "2024-06-01T12:00:00Z",
        }
        result = transform_event(event)
        assert result is not None
        assert result["has_user"] is False


# ===================================================================
# End-to-end pipeline runs
# ===================================================================


class TestRunIncrementalETL:
    """Integration tests for :func:`run_incremental_etl`."""

    def test_first_run_processes_all(self, tmp_path: Path) -> None:
        """First run processes all events, writes output, updates checkpoint."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        cp_path = tmp_path / "checkpoint.json"

        write_jsonl(input_dir / "batch1.jsonl", SAMPLE_EVENTS)

        summary = run_incremental_etl(input_dir, output_dir, cp_path)

        assert summary.records_seen == 3
        assert summary.records_skipped == 0
        assert summary.records_processed == 3
        assert summary.records_failed == 0
        assert summary.checkpoint_updated is True

        # Checkpoint should contain all 3 event IDs
        cp = load_checkpoint(cp_path, "events_etl")
        assert cp.processed_ids == {"evt-001", "evt-002", "evt-003"}
        assert cp.total_runs == 1

        # Output file should exist with 3 lines
        output_files = list(output_dir.glob("*.jsonl"))
        assert len(output_files) == 1
        lines = output_files[0].read_text().strip().split("\n")
        assert len(lines) == 3

    def test_second_run_no_new_data(self, tmp_path: Path) -> None:
        """Second run with same data processes nothing, no output file created."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        cp_path = tmp_path / "checkpoint.json"

        write_jsonl(input_dir / "batch1.jsonl", SAMPLE_EVENTS)

        # First run
        run_incremental_etl(input_dir, output_dir, cp_path)

        # Clear output to detect new files
        for f in output_dir.glob("*.jsonl"):
            f.unlink()

        # Second run
        summary = run_incremental_etl(input_dir, output_dir, cp_path)

        assert summary.records_seen == 3
        assert summary.records_skipped == 3
        assert summary.records_processed == 0
        assert summary.records_failed == 0
        assert summary.checkpoint_updated is False

        # No new output file
        output_files = list(output_dir.glob("*.jsonl"))
        assert len(output_files) == 0

    def test_new_data_after_first_run(self, tmp_path: Path) -> None:
        """After first run, only new events are processed."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        cp_path = tmp_path / "checkpoint.json"

        write_jsonl(input_dir / "batch1.jsonl", SAMPLE_EVENTS)

        # First run
        run_incremental_etl(input_dir, output_dir, cp_path)

        # Add new events
        new_events: list[dict[str, object]] = [
            {
                "event_id": "evt-004",
                "type": "page_view",
                "user_id": "U003",
                "timestamp": "2024-06-02T09:00:00Z",
            },
            {
                "event_id": "evt-005",
                "type": "checkout",
                "user_id": "U004",
                "timestamp": "2024-06-02T14:00:00Z",
            },
        ]
        write_jsonl(input_dir / "batch2.jsonl", new_events)

        # Clear output directory
        for f in output_dir.glob("*.jsonl"):
            f.unlink()

        # Second run
        summary = run_incremental_etl(input_dir, output_dir, cp_path)

        # Sees all 5 events across both files, but skips the original 3
        assert summary.records_seen == 5
        assert summary.records_skipped == 3
        assert summary.records_processed == 2
        assert summary.records_failed == 0
        assert summary.checkpoint_updated is True

        # Checkpoint now has all 5 IDs
        cp = load_checkpoint(cp_path, "events_etl")
        assert cp.processed_ids == {
            "evt-001", "evt-002", "evt-003", "evt-004", "evt-005"
        }
        assert cp.total_runs == 2

        # Output file contains exactly 2 records
        output_files = list(output_dir.glob("*.jsonl"))
        assert len(output_files) == 1
        lines = output_files[0].read_text().strip().split("\n")
        assert len(lines) == 2

    def test_failure_before_checkpoint_update(self, tmp_path: Path) -> None:
        """If save_checkpoint raises, the checkpoint file is not updated."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        cp_path = tmp_path / "checkpoint.json"

        write_jsonl(input_dir / "batch1.jsonl", SAMPLE_EVENTS)

        with patch(
            "data_platform_lab.transform.incremental_etl.save_checkpoint",
            side_effect=OSError("disk full"),
        ):
            try:
                run_incremental_etl(input_dir, output_dir, cp_path)
            except OSError:
                pass

        # Checkpoint file should not exist (never written)
        assert not cp_path.exists()

    def test_rerun_after_failure(self, tmp_path: Path) -> None:
        """After a simulated failure, rerun successfully processes the events."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        cp_path = tmp_path / "checkpoint.json"

        write_jsonl(input_dir / "batch1.jsonl", SAMPLE_EVENTS)

        # First run fails on save
        with patch(
            "data_platform_lab.transform.incremental_etl.save_checkpoint",
            side_effect=OSError("disk full"),
        ):
            try:
                run_incremental_etl(input_dir, output_dir, cp_path)
            except OSError:
                pass

        # Clear any output from the failed run
        if output_dir.exists():
            for f in output_dir.glob("*.jsonl"):
                f.unlink()

        # Second run succeeds — should process all events since checkpoint
        # was never updated
        summary = run_incremental_etl(input_dir, output_dir, cp_path)

        assert summary.records_processed == 3
        assert summary.records_skipped == 0
        assert summary.checkpoint_updated is True

        cp = load_checkpoint(cp_path, "events_etl")
        assert cp.processed_ids == {"evt-001", "evt-002", "evt-003"}

    def test_duplicate_event_ids_in_input(self, tmp_path: Path) -> None:
        """Duplicate event_ids in input are deduplicated (only processed once)."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        output_dir = tmp_path / "output"
        cp_path = tmp_path / "checkpoint.json"

        events_with_dups: list[dict[str, object]] = [
            SAMPLE_EVENTS[0],
            SAMPLE_EVENTS[1],
            SAMPLE_EVENTS[0],  # duplicate of evt-001
        ]
        write_jsonl(input_dir / "batch1.jsonl", events_with_dups)

        summary = run_incremental_etl(input_dir, output_dir, cp_path)

        # Sees 3 raw records, but only 2 unique event IDs
        assert summary.records_seen == 3
        assert summary.records_processed == 2
        assert summary.records_failed == 0
        assert summary.checkpoint_updated is True

        cp = load_checkpoint(cp_path, "events_etl")
        assert cp.processed_ids == {"evt-001", "evt-002"}

        output_files = list(output_dir.glob("*.jsonl"))
        assert len(output_files) == 1
        lines = output_files[0].read_text().strip().split("\n")
        assert len(lines) == 2
