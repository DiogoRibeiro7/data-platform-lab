"""Tests for the observability tracker utilities."""

from __future__ import annotations

import re
import time

import pytest

from data_platform_lab.observability.tracker import (
    RunMetadata,
    RunTracker,
    Timer,
    format_run_metadata,
    generate_run_id,
    metadata_to_dict,
)

# ---------------------------------------------------------------------------
# Timer
# ---------------------------------------------------------------------------


class TestTimer:
    """Tests for the Timer helper."""

    def test_timer_context_manager(self) -> None:
        with Timer() as t:
            time.sleep(0.01)
        assert t.elapsed > 0

    def test_timer_start_stop(self) -> None:
        t = Timer()
        t.start()
        time.sleep(0.01)
        t.stop()
        assert t.elapsed > 0

    def test_timer_not_started(self) -> None:
        t = Timer()
        assert t.elapsed == 0.0

    def test_timer_running_property(self) -> None:
        t = Timer()
        assert t.running is False

        t.start()
        assert t.running is True

        t.stop()
        assert t.running is False

    def test_timer_elapsed_while_running(self) -> None:
        t = Timer()
        t.start()
        time.sleep(0.01)
        assert t.elapsed > 0
        t.stop()


# ---------------------------------------------------------------------------
# RunTracker
# ---------------------------------------------------------------------------


class TestRunTracker:
    """Tests for the RunTracker class."""

    def test_tracker_context_manager_success(self) -> None:
        tracker = RunTracker("test")
        with tracker:
            pass

        meta = tracker.metadata
        assert meta.status == "success"
        assert meta.started_at != ""
        assert meta.ended_at is not None
        assert meta.duration_seconds >= 0

    def test_tracker_context_manager_failure(self) -> None:
        tracker = RunTracker("test")
        with pytest.raises(ValueError, match="boom"), tracker:
            raise ValueError("boom")

        meta = tracker.metadata
        assert meta.status == "failed"
        assert len(meta.errors) == 1
        assert "ValueError" in meta.errors[0]
        assert "boom" in meta.errors[0]

    def test_tracker_row_counting(self) -> None:
        tracker = RunTracker("test")
        with tracker:
            tracker.inc_rows_read(100)
            tracker.inc_rows_written(95)
            tracker.inc_rows_rejected(5)

        meta = tracker.metadata
        assert meta.rows_read == 100
        assert meta.rows_written == 95
        assert meta.rows_rejected == 5

    def test_tracker_file_counting(self) -> None:
        tracker = RunTracker("test")
        with tracker:
            tracker.inc_files_processed(3)
            tracker.inc_files_rejected(1)

        meta = tracker.metadata
        assert meta.files_processed == 3
        assert meta.files_rejected == 1

    def test_tracker_warnings_and_errors(self) -> None:
        tracker = RunTracker("test")
        with tracker:
            tracker.add_warning("w1")
            tracker.add_warning("w2")
            tracker.add_error("e1")

        meta = tracker.metadata
        assert meta.warnings == ["w1", "w2"]
        assert meta.errors == ["e1"]

    def test_tracker_extra_metadata(self) -> None:
        tracker = RunTracker("test")
        with tracker:
            tracker.set_extra("custom_key", "custom_value")

        meta = tracker.metadata
        assert meta.extra == {"custom_key": "custom_value"}

    def test_tracker_custom_run_id(self) -> None:
        tracker = RunTracker("test", run_id="my-run-123")
        assert tracker.metadata.run_id == "my-run-123"

    def test_tracker_default_run_id(self) -> None:
        tracker = RunTracker("test")
        run_id = tracker.metadata.run_id
        assert re.fullmatch(r"\d{8}_\d{6}", run_id), (
            f"run_id '{run_id}' does not match YYYYMMDD_HHMMSS"
        )

    def test_tracker_start_finish_explicit(self) -> None:
        tracker = RunTracker("test")
        tracker.start()
        time.sleep(0.01)
        tracker.finish(status="success")

        meta = tracker.metadata
        assert meta.status == "success"
        assert meta.started_at != ""
        assert meta.ended_at is not None
        assert meta.duration_seconds > 0

    def test_tracker_multiple_increments(self) -> None:
        tracker = RunTracker("test")
        with tracker:
            tracker.inc_rows_read(10)
            tracker.inc_rows_read(20)
            tracker.inc_rows_read(30)

        assert tracker.metadata.rows_read == 60


# ---------------------------------------------------------------------------
# format_run_metadata
# ---------------------------------------------------------------------------


class TestFormatRunMetadata:
    """Tests for the format_run_metadata helper."""

    def test_format_basic(self) -> None:
        meta = RunMetadata(
            pipeline_name="my_pipe",
            run_id="20260101_120000",
            status="success",
            started_at="2026-01-01T12:00:00",
            ended_at="2026-01-01T12:00:05",
            duration_seconds=5.0,
            rows_read=100,
            rows_written=90,
            rows_rejected=10,
        )
        output = format_run_metadata(meta)
        assert "my_pipe" in output
        assert "success" in output
        assert "Rows read:     100" in output
        assert "Rows written:  90" in output
        assert "Rows rejected: 10" in output

    def test_format_with_warnings(self) -> None:
        meta = RunMetadata(
            pipeline_name="pipe",
            run_id="run1",
            status="success",
            started_at="2026-01-01T12:00:00",
            warnings=["something odd"],
        )
        output = format_run_metadata(meta)
        assert "Warnings (1):" in output
        assert "something odd" in output

    def test_format_with_extras(self) -> None:
        meta = RunMetadata(
            pipeline_name="pipe",
            run_id="run1",
            status="success",
            started_at="2026-01-01T12:00:00",
            extra={"source": "s3://bucket/path"},
        )
        output = format_run_metadata(meta)
        assert "Extra:" in output
        assert "source" in output
        assert "s3://bucket/path" in output


# ---------------------------------------------------------------------------
# metadata_to_dict
# ---------------------------------------------------------------------------


class TestMetadataToDict:
    """Tests for the metadata_to_dict helper."""

    def test_to_dict(self) -> None:
        meta = RunMetadata(
            pipeline_name="pipe",
            run_id="run1",
            status="success",
            started_at="2026-01-01T12:00:00",
            ended_at="2026-01-01T12:00:05",
            duration_seconds=5.0,
            rows_read=50,
            rows_written=45,
            rows_rejected=5,
            files_processed=2,
            files_rejected=0,
            warnings=["w"],
            errors=[],
            extra={"key": "val"},
        )
        d = metadata_to_dict(meta)
        assert isinstance(d, dict)
        assert d["pipeline_name"] == "pipe"
        assert d["run_id"] == "run1"
        assert d["status"] == "success"
        assert d["rows_read"] == 50
        assert d["rows_written"] == 45
        assert d["rows_rejected"] == 5
        assert d["files_processed"] == 2
        assert d["warnings"] == ["w"]
        assert d["extra"] == {"key": "val"}


# ---------------------------------------------------------------------------
# generate_run_id
# ---------------------------------------------------------------------------


class TestGenerateRunId:
    """Tests for the generate_run_id helper."""

    def test_generate_run_id_format(self) -> None:
        run_id = generate_run_id()
        assert re.fullmatch(r"\d{8}_\d{6}", run_id), (
            f"run_id '{run_id}' does not match YYYYMMDD_HHMMSS"
        )
