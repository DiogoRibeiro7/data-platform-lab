"""Observability tracker — reusable timing, counting, and run-metadata utilities."""

from __future__ import annotations

import datetime
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class RunMetadata:
    """Structured metadata for a pipeline run."""

    pipeline_name: str
    run_id: str
    status: str  # "success", "failed", "running"
    started_at: str  # ISO timestamp
    ended_at: str | None = None  # ISO timestamp, None while running
    duration_seconds: float = 0.0
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    files_processed: int = 0
    files_rejected: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)  # custom metadata


# ---------------------------------------------------------------------------
# Timer
# ---------------------------------------------------------------------------


class Timer:
    """Simple execution timer.

    Can be used as a context manager or with start/stop methods.

    Usage::

        with Timer() as t:
            do_work()
        print(t.elapsed)

        # or
        t = Timer()
        t.start()
        do_work()
        t.stop()
        print(t.elapsed)
    """

    def __init__(self) -> None:
        self._start: float | None = None
        self._end: float | None = None

    def start(self) -> Timer:
        """Start the timer. Returns self."""
        self._start = time.perf_counter()
        self._end = None
        return self

    def stop(self) -> Timer:
        """Stop the timer. Returns self."""
        self._end = time.perf_counter()
        return self

    @property
    def elapsed(self) -> float:
        """Elapsed seconds.

        If running, returns time since start.  If stopped, returns
        start-to-stop duration.  If never started, returns ``0.0``.
        """
        if self._start is None:
            return 0.0
        end = self._end if self._end is not None else time.perf_counter()
        return end - self._start

    @property
    def running(self) -> bool:
        """Return *True* while the timer is running."""
        return self._start is not None and self._end is None

    def __enter__(self) -> Timer:
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        self.stop()


# ---------------------------------------------------------------------------
# RunTracker
# ---------------------------------------------------------------------------


class RunTracker:
    """Collects run metadata for a pipeline execution.

    Tracks timing, row counts, file counts, warnings, and errors.
    Can be used as a context manager that auto-starts/stops timing
    and sets status to ``"failed"`` on unhandled exceptions.

    Usage::

        tracker = RunTracker("my_pipeline")
        with tracker:
            tracker.inc_rows_read(100)
            tracker.inc_rows_written(95)
            tracker.inc_rows_rejected(5)
            tracker.add_warning("5 rows had null emails")

        metadata = tracker.metadata
        print(format_run_metadata(metadata))
    """

    def __init__(self, pipeline_name: str, run_id: str | None = None) -> None:
        """Create a tracker.

        If *run_id* is ``None``, one is generated from the current UTC
        timestamp.
        """
        self._timer = Timer()
        self._pipeline_name = pipeline_name
        self._run_id = run_id or generate_run_id()
        self._status = "running"
        self._started_at: str | None = None
        self._ended_at: str | None = None
        self._rows_read = 0
        self._rows_written = 0
        self._rows_rejected = 0
        self._files_processed = 0
        self._files_rejected = 0
        self._warnings: list[str] = []
        self._errors: list[str] = []
        self._extra: dict[str, Any] = {}
        self._logger = logging.getLogger(f"{__name__}.{pipeline_name}")

    # -- lifecycle -----------------------------------------------------------

    def start(self) -> RunTracker:
        """Start timing. Returns self."""
        self._timer.start()
        self._started_at = datetime.datetime.now(datetime.UTC).isoformat()
        self._status = "running"
        self._logger.info(
            "Pipeline '%s' run %s started",
            self._pipeline_name,
            self._run_id,
        )
        return self

    def finish(self, status: str = "success") -> RunTracker:
        """Stop timing and set final status. Returns self."""
        self._timer.stop()
        self._ended_at = datetime.datetime.now(datetime.UTC).isoformat()
        self._status = status
        self._logger.info(
            "Pipeline '%s' run %s finished with status '%s' in %.2fs",
            self._pipeline_name,
            self._run_id,
            status,
            self._timer.elapsed,
        )
        return self

    # -- counters ------------------------------------------------------------

    def inc_rows_read(self, count: int = 1) -> None:
        self._rows_read += count

    def inc_rows_written(self, count: int = 1) -> None:
        self._rows_written += count

    def inc_rows_rejected(self, count: int = 1) -> None:
        self._rows_rejected += count

    def inc_files_processed(self, count: int = 1) -> None:
        self._files_processed += count

    def inc_files_rejected(self, count: int = 1) -> None:
        self._files_rejected += count

    # -- diagnostics ---------------------------------------------------------

    def add_warning(self, message: str) -> None:
        self._warnings.append(message)
        self._logger.warning("[%s] %s", self._pipeline_name, message)

    def add_error(self, message: str) -> None:
        self._errors.append(message)
        self._logger.error("[%s] %s", self._pipeline_name, message)

    def set_extra(self, key: str, value: Any) -> None:
        """Store custom metadata."""
        self._extra[key] = value

    # -- output --------------------------------------------------------------

    @property
    def metadata(self) -> RunMetadata:
        """Build and return the current run metadata snapshot."""
        return RunMetadata(
            pipeline_name=self._pipeline_name,
            run_id=self._run_id,
            status=self._status,
            started_at=self._started_at or "",
            ended_at=self._ended_at,
            duration_seconds=self._timer.elapsed,
            rows_read=self._rows_read,
            rows_written=self._rows_written,
            rows_rejected=self._rows_rejected,
            files_processed=self._files_processed,
            files_rejected=self._files_rejected,
            warnings=list(self._warnings),
            errors=list(self._errors),
            extra=dict(self._extra),
        )

    # -- context manager -----------------------------------------------------

    def __enter__(self) -> RunTracker:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if exc_type is not None:
            self.add_error(f"{exc_type.__name__}: {exc_val}")
            self.finish(status="failed")
        else:
            self.finish(status="success")


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def generate_run_id() -> str:
    """Generate a run ID from the current UTC timestamp: ``YYYYMMDD_HHMMSS``."""
    return datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")


def format_run_metadata(meta: RunMetadata) -> str:
    """Format *RunMetadata* as a human-readable summary string."""
    lines = [
        f"=== Run: {meta.pipeline_name} ({meta.run_id}) ===",
        f"Status: {meta.status}",
        f"Started: {meta.started_at}",
    ]
    if meta.ended_at:
        lines.append(f"Ended:   {meta.ended_at}")
    lines.append(f"Duration: {meta.duration_seconds:.2f}s")
    lines.append("")
    lines.append(f"Rows read:     {meta.rows_read}")
    lines.append(f"Rows written:  {meta.rows_written}")
    lines.append(f"Rows rejected: {meta.rows_rejected}")
    if meta.files_processed or meta.files_rejected:
        lines.append(f"Files processed: {meta.files_processed}")
        lines.append(f"Files rejected:  {meta.files_rejected}")
    if meta.warnings:
        lines.append("")
        lines.append(f"Warnings ({len(meta.warnings)}):")
        for w in meta.warnings:
            lines.append(f"  - {w}")
    if meta.errors:
        lines.append("")
        lines.append(f"Errors ({len(meta.errors)}):")
        for e in meta.errors:
            lines.append(f"  - {e}")
    if meta.extra:
        lines.append("")
        lines.append("Extra:")
        for k, v in meta.extra.items():
            lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def metadata_to_dict(meta: RunMetadata) -> dict[str, Any]:
    """Convert *RunMetadata* to a plain dict suitable for JSON serialization."""
    return asdict(meta)
