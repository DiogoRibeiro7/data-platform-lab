"""Streaming-style sensor event processor.

Reads JSONL sensor events, validates, deduplicates, routes rejected/duplicate
events to a dead-letter file, computes per-sensor aggregates, and writes a
run summary.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataexcept import FileReadError, FileWriteError

from data_platform_lab.manifest import write_manifest

logger = logging.getLogger(__name__)

REQUIRED_FIELDS: list[str] = [
    "sensor_id",
    "type",
    "value",
    "unit",
    "location",
    "timestamp",
]


@dataclass
class EventResult:
    """Result for a single processed event."""

    event: dict[str, Any]
    status: str
    reason: str | None = None


@dataclass
class StreamSummary:
    """Run summary — follows platform conventions (snake_case fields)."""

    pipeline_name: str = "sensor_stream"
    run_at: str = ""
    duration_seconds: float = 0.0
    status: str = "success"
    events_seen: int = 0
    events_accepted: int = 0
    events_rejected: int = 0
    events_duplicate: int = 0
    dead_letter_count: int = 0
    events_late: int = 0
    max_lateness_seconds: float = 0.0
    watermark: str = ""
    lateness_threshold_seconds: float = 0.0
    aggregates: dict[str, Any] = field(default_factory=dict)
    rejection_reasons: dict[str, int] = field(default_factory=dict)
    manifest_path: str = ""


def parse_event_time(timestamp_str: str) -> datetime:
    """Parse an ISO 8601 timestamp string into a timezone-aware datetime."""
    return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))


def classify_lateness(
    event_time: datetime,
    watermark: datetime | None,
    threshold_seconds: float,
) -> tuple[bool, float]:
    """Determine if an event is late relative to the current watermark."""
    if watermark is None:
        return False, 0.0
    lateness = (watermark - event_time).total_seconds()
    is_late = lateness > threshold_seconds
    return is_late, max(lateness, 0.0)


def validate_event(event: dict[str, Any]) -> EventResult:
    """Validate a single event against required-field and type rules."""
    for fld in REQUIRED_FIELDS:
        if fld not in event:
            return EventResult(event=event, status="rejected", reason=f"missing field: {fld}")

    if event["value"] is None:
        return EventResult(event=event, status="rejected", reason="null value")

    for fld in REQUIRED_FIELDS:
        if fld == "value":
            continue
        if not isinstance(event[fld], str) or not event[fld].strip():
            return EventResult(
                event=event, status="rejected", reason=f"empty or invalid field: {fld}"
            )

    if not isinstance(event["value"], (int, float)):
        return EventResult(event=event, status="rejected", reason="value is not a number")

    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return EventResult(event=event, status="rejected", reason="unparseable timestamp")

    return EventResult(event=event, status="accepted")


def deduplicate_key(event: dict[str, Any]) -> str:
    """Return a dedup key: ``"{sensor_id}::{timestamp}"``."""
    return f"{event['sensor_id']}::{event['timestamp']}"


def compute_aggregates(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute grouped aggregates over accepted events."""
    by_sensor: dict[str, dict[str, Any]] = {}
    by_type: dict[str, int] = {}
    by_location: dict[str, int] = {}

    for evt in events:
        sid = evt["sensor_id"]
        val = evt["value"]
        if sid not in by_sensor:
            by_sensor[sid] = {"count": 0, "min_value": val, "max_value": val, "_sum": 0.0}
        entry = by_sensor[sid]
        entry["count"] += 1
        entry["_sum"] += val
        if val < entry["min_value"]:
            entry["min_value"] = val
        if val > entry["max_value"]:
            entry["max_value"] = val

        event_type = evt["type"]
        by_type[event_type] = by_type.get(event_type, 0) + 1
        location = evt["location"]
        by_location[location] = by_location.get(location, 0) + 1

    for entry in by_sensor.values():
        entry["avg_value"] = round(entry["_sum"] / entry["count"], 2)
        del entry["_sum"]

    return {"by_sensor": by_sensor, "by_type": by_type, "by_location": by_location}


def _read_lines(path: Path) -> Iterator[str]:
    """Yield source lines while preserving file-level failure context."""
    try:
        with path.open(encoding="utf-8") as fh:
            yield from fh
    except OSError as exc:
        raise FileReadError(str(path), exc) from exc


def _ensure_output_dir(path: Path) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise FileWriteError(str(path), exc) from exc


def _write_lines(path: Path, lines: Iterable[str]) -> None:
    """Write text chunks incrementally while preserving file failure context."""
    try:
        with path.open("w", encoding="utf-8") as fh:
            for line in lines:
                fh.write(line)
    except OSError as exc:
        raise FileWriteError(str(path), exc) from exc


def _write_text(path: Path, text: str) -> None:
    _write_lines(path, (text,))


def process_stream(
    input_path: str | Path,
    output_dir: str | Path,
    pipeline_name: str = "sensor_stream",
    lateness_threshold_seconds: float = 0.0,
) -> StreamSummary:
    """Process a JSONL file of sensor events end-to-end."""
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    _ensure_output_dir(output_dir)

    logger.info("Starting pipeline '%s' — reading from %s", pipeline_name, input_path)
    start = time.monotonic()

    results: list[EventResult] = []
    seen_keys: set[str] = set()
    accepted_events: list[dict[str, Any]] = []
    rejection_reasons: dict[str, int] = {}
    watermark: datetime | None = None
    late_events: list[dict[str, Any]] = []
    max_lateness = 0.0

    for raw_line in _read_lines(input_path):
        line = raw_line.strip()
        if not line:
            continue

        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            result = EventResult(event={"_raw": line}, status="rejected", reason="malformed JSON")
            rejection_reasons["malformed JSON"] = rejection_reasons.get("malformed JSON", 0) + 1
            logger.warning("Rejected event (malformed JSON): %s", line[:120])
            results.append(result)
            continue

        try:
            result = validate_event(event)
        except Exception:
            result = EventResult(event=event, status="rejected", reason="validation error")
            logger.warning("Rejected event (validation error): %s", event)

        if result.status == "rejected":
            reason = result.reason or "unknown"
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            logger.warning("Rejected event (%s): %s", reason, event)
            results.append(result)
            continue

        key = deduplicate_key(event)
        if key in seen_keys:
            result = EventResult(event=event, status="duplicate", reason="duplicate event")
            rejection_reasons["duplicate event"] = rejection_reasons.get("duplicate event", 0) + 1
            logger.warning("Duplicate event: %s", key)
            results.append(result)
            continue

        seen_keys.add(key)
        accepted_events.append(event)
        event_time = parse_event_time(event["timestamp"])
        is_late, lateness = classify_lateness(event_time, watermark, lateness_threshold_seconds)
        if is_late:
            late_events.append(event)
            if lateness > max_lateness:
                max_lateness = lateness
        if watermark is None or event_time > watermark:
            watermark = event_time
        results.append(result)

    aggregates = compute_aggregates(accepted_events)
    accepted_path = output_dir / "accepted.jsonl"
    dead_letter_path = output_dir / "dead_letter.jsonl"
    late_events_path = output_dir / "late_events.jsonl"

    _write_lines(accepted_path, (json.dumps(evt) + "\n" for evt in accepted_events))
    _write_lines(
        dead_letter_path,
        (
            json.dumps({"event": result.event, "status": result.status, "reason": result.reason})
            + "\n"
            for result in results
            if result.status in ("rejected", "duplicate")
        ),
    )
    _write_lines(late_events_path, (json.dumps(evt) + "\n" for evt in late_events))

    duration = time.monotonic() - start
    events_accepted = sum(1 for result in results if result.status == "accepted")
    events_rejected = sum(1 for result in results if result.status == "rejected")
    events_duplicate = sum(1 for result in results if result.status == "duplicate")

    summary = StreamSummary(
        pipeline_name=pipeline_name,
        run_at=datetime.now(UTC).isoformat(),
        duration_seconds=round(duration, 4),
        status="success",
        events_seen=len(results),
        events_accepted=events_accepted,
        events_rejected=events_rejected,
        events_duplicate=events_duplicate,
        dead_letter_count=events_rejected + events_duplicate,
        events_late=len(late_events),
        max_lateness_seconds=round(max_lateness, 2),
        watermark=watermark.isoformat() if watermark else "",
        lateness_threshold_seconds=lateness_threshold_seconds,
        aggregates=aggregates,
        rejection_reasons=rejection_reasons,
    )

    summary_path = output_dir / "summary.json"
    _write_text(summary_path, json.dumps(asdict(summary), indent=2))

    logger.info(
        "Pipeline '%s' complete — %d accepted (%d late), %d rejected, %d duplicate (%.3fs)",
        pipeline_name,
        events_accepted,
        len(late_events),
        events_rejected,
        events_duplicate,
        duration,
    )

    manifest_path = write_manifest(
        pipeline_name=pipeline_name,
        run_id=datetime.now(UTC).strftime("%Y%m%d_%H%M%S"),
        source=str(input_path),
        output=str(accepted_path),
        row_count=events_accepted,
        status="success",
        extras={
            "events_seen": len(results),
            "events_rejected": events_rejected,
            "events_duplicate": events_duplicate,
            "events_late": len(late_events),
            "dead_letter_path": str(dead_letter_path),
        },
    )
    summary.manifest_path = str(manifest_path)
    return summary
