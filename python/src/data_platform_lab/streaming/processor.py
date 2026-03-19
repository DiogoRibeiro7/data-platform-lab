"""Streaming-style sensor event processor.

Reads JSONL sensor events, validates, deduplicates, routes rejected/duplicate
events to a dead-letter file, computes per-sensor aggregates, and writes a
run summary.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REQUIRED_FIELDS: list[str] = [
    "sensor_id",
    "type",
    "value",
    "unit",
    "location",
    "timestamp",
]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class EventResult:
    """Result for a single processed event."""

    event: dict[str, Any]
    status: str  # "accepted", "rejected", "duplicate"
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
    aggregates: dict[str, Any] = field(default_factory=dict)
    rejection_reasons: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def validate_event(event: dict[str, Any]) -> EventResult:
    """Validate a single event against required-field and type rules.

    Returns an :class:`EventResult` with status ``"accepted"`` when the event
    passes all checks, or ``"rejected"`` with a human-readable *reason*
    otherwise.
    """
    for fld in REQUIRED_FIELDS:
        if fld not in event:
            return EventResult(event=event, status="rejected", reason=f"missing field: {fld}")

    # value must not be None (JSON null)
    if event["value"] is None:
        return EventResult(event=event, status="rejected", reason="null value")

    # Non-empty string checks for string fields.
    for fld in REQUIRED_FIELDS:
        if fld == "value":
            continue
        if not isinstance(event[fld], str) or not event[fld].strip():
            return EventResult(
                event=event, status="rejected", reason=f"empty or invalid field: {fld}"
            )

    # value must be numeric.
    if not isinstance(event["value"], (int, float)):
        return EventResult(event=event, status="rejected", reason="value is not a number")

    # Timestamp must be parseable as ISO 8601.
    try:
        datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return EventResult(event=event, status="rejected", reason="unparseable timestamp")

    return EventResult(event=event, status="accepted")


def deduplicate_key(event: dict[str, Any]) -> str:
    """Return a dedup key: ``"{sensor_id}::{timestamp}"``."""
    return f"{event['sensor_id']}::{event['timestamp']}"


def compute_aggregates(events: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute grouped aggregates over *accepted* events.

    Returns a dict with keys ``by_sensor``, ``by_type``, and ``by_location``.
    """
    by_sensor: dict[str, dict[str, Any]] = {}
    by_type: dict[str, int] = {}
    by_location: dict[str, int] = {}

    for evt in events:
        sid = evt["sensor_id"]
        val = evt["value"]

        # by_sensor
        if sid not in by_sensor:
            by_sensor[sid] = {"count": 0, "min_value": val, "max_value": val, "_sum": 0.0}
        entry = by_sensor[sid]
        entry["count"] += 1
        entry["_sum"] += val
        if val < entry["min_value"]:
            entry["min_value"] = val
        if val > entry["max_value"]:
            entry["max_value"] = val

        # by_type
        t = evt["type"]
        by_type[t] = by_type.get(t, 0) + 1

        # by_location
        loc = evt["location"]
        by_location[loc] = by_location.get(loc, 0) + 1

    # Compute avg and drop internal _sum
    for entry in by_sensor.values():
        entry["avg_value"] = round(entry["_sum"] / entry["count"], 2)
        del entry["_sum"]

    return {
        "by_sensor": by_sensor,
        "by_type": by_type,
        "by_location": by_location,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def process_stream(
    input_path: str | Path,
    output_dir: str | Path,
    pipeline_name: str = "sensor_stream",
) -> StreamSummary:
    """Process a JSONL file of sensor events end-to-end.

    1. Read events one by one from *input_path* (JSONL).
    2. Handle malformed JSON lines (reject with reason ``"malformed JSON"``).
    3. Validate each event.
    4. Deduplicate by key (first occurrence wins).
    5. Write accepted events to ``{output_dir}/accepted.jsonl``.
    6. Write rejected / duplicate events to ``{output_dir}/dead_letter.jsonl``.
    7. Compute aggregates over accepted events only.
    8. Write summary to ``{output_dir}/summary.json``.
    9. Return :class:`StreamSummary`.
    """
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Starting pipeline '%s' — reading from %s", pipeline_name, input_path)
    start = time.monotonic()

    results: list[EventResult] = []
    seen_keys: set[str] = set()
    accepted_events: list[dict[str, Any]] = []
    rejection_reasons: dict[str, int] = {}

    with input_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue

            # --- Parse JSON ---------------------------------------------------
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                result = EventResult(
                    event={"_raw": line}, status="rejected", reason="malformed JSON",
                )
                rejection_reasons["malformed JSON"] = rejection_reasons.get("malformed JSON", 0) + 1
                logger.warning("Rejected event (malformed JSON): %s", line[:120])
                results.append(result)
                continue

            # --- Validate -----------------------------------------------------
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

            # --- Deduplicate --------------------------------------------------
            key = deduplicate_key(event)
            if key in seen_keys:
                result = EventResult(event=event, status="duplicate", reason="duplicate event")
                dup_key = "duplicate event"
                rejection_reasons[dup_key] = rejection_reasons.get(dup_key, 0) + 1
                logger.warning("Duplicate event: %s", key)
                results.append(result)
                continue

            seen_keys.add(key)
            accepted_events.append(event)
            results.append(result)

    # --- Compute aggregates -----------------------------------------------
    aggregates = compute_aggregates(accepted_events)

    # --- Write outputs ----------------------------------------------------
    accepted_path = output_dir / "accepted.jsonl"
    dead_letter_path = output_dir / "dead_letter.jsonl"

    with accepted_path.open("w", encoding="utf-8") as fh:
        for evt in accepted_events:
            fh.write(json.dumps(evt) + "\n")

    with dead_letter_path.open("w", encoding="utf-8") as fh:
        for r in results:
            if r.status in ("rejected", "duplicate"):
                record = {"event": r.event, "status": r.status, "reason": r.reason}
                fh.write(json.dumps(record) + "\n")

    duration = time.monotonic() - start

    events_accepted = sum(1 for r in results if r.status == "accepted")
    events_rejected = sum(1 for r in results if r.status == "rejected")
    events_duplicate = sum(1 for r in results if r.status == "duplicate")

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
        aggregates=aggregates,
        rejection_reasons=rejection_reasons,
    )

    summary_path = output_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(asdict(summary), fh, indent=2)

    logger.info(
        "Pipeline '%s' complete — %d accepted, %d rejected, %d duplicate (%.3fs)",
        pipeline_name,
        events_accepted,
        events_rejected,
        events_duplicate,
        duration,
    )

    return summary
