"""Incremental ETL pipeline with checkpoint-based deduplication.

Reads JSONL event files, processes only unseen events, enriches them with
derived fields, writes output as JSONL, and persists a checkpoint so that
reruns are idempotent.
"""

from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """Tracks which events have already been processed."""

    pipeline_name: str
    last_run_at: str | None = None
    processed_ids: set[str] = field(default_factory=set)
    total_runs: int = 0


@dataclass
class RunSummary:
    """Summary produced by a single incremental ETL run."""

    pipeline_name: str
    run_at: str
    records_seen: int
    records_skipped: int
    records_processed: int
    records_failed: int
    checkpoint_updated: bool


# ---------------------------------------------------------------------------
# Checkpoint persistence
# ---------------------------------------------------------------------------


def load_checkpoint(checkpoint_path: Path, pipeline_name: str) -> Checkpoint:
    """Load checkpoint from file, or return empty checkpoint if not found."""
    if not checkpoint_path.exists():
        logger.info(
            "No checkpoint file at %s — starting fresh for '%s'",
            checkpoint_path,
            pipeline_name,
        )
        return Checkpoint(pipeline_name=pipeline_name)

    try:
        with checkpoint_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(
            "Corrupted checkpoint at %s (%s) — starting fresh for '%s'",
            checkpoint_path,
            exc,
            pipeline_name,
        )
        return Checkpoint(pipeline_name=pipeline_name)

    return Checkpoint(
        pipeline_name=data.get("pipeline_name", pipeline_name),
        last_run_at=data.get("last_run_at"),
        processed_ids=set(data.get("processed_ids", [])),
        total_runs=data.get("total_runs", 0),
    )


def save_checkpoint(checkpoint_path: Path, checkpoint: Checkpoint) -> None:
    """Write checkpoint to JSON file."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "pipeline_name": checkpoint.pipeline_name,
        "last_run_at": checkpoint.last_run_at,
        "processed_ids": sorted(checkpoint.processed_ids),
        "total_runs": checkpoint.total_runs,
    }
    with checkpoint_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("Checkpoint saved to %s", checkpoint_path)


# ---------------------------------------------------------------------------
# Reading input
# ---------------------------------------------------------------------------


def read_events(input_dir: Path) -> list[dict[str, Any]]:
    """Read all JSONL files from *input_dir*, return list of parsed events.

    Each line is a JSON object.  Blank lines are skipped.  Files are read
    in alphabetical order.
    """
    events: list[dict[str, Any]] = []
    jsonl_files = sorted(input_dir.glob("*.jsonl"))

    if not jsonl_files:
        logger.warning("No JSONL files found in %s", input_dir)
        return events

    malformed = 0
    for path in jsonl_files:
        with path.open("r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    events.append(json.loads(stripped))
                except json.JSONDecodeError:
                    malformed += 1
                    logger.warning(
                        "Skipping malformed JSON at %s:%d", path.name, line_no
                    )

    if malformed:
        logger.warning("Skipped %d malformed line(s) across input files", malformed)
    logger.info("Read %d events from %d file(s)", len(events), len(jsonl_files))
    return events


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def transform_event(event: dict[str, Any]) -> dict[str, Any] | None:
    """Transform a single event by enriching it with derived fields.

    Returns the enriched event, or ``None`` if the event is missing any of
    the required fields (``event_id``, ``timestamp``, ``type``).

    Enrichment fields added:
    - ``event_date`` — date extracted from *timestamp* (YYYY-MM-DD)
    - ``hour`` — hour extracted from *timestamp* (integer)
    - ``is_purchase`` — ``True`` if *type* is ``"checkout"``
    - ``has_user`` — ``True`` if *user_id* is present and non-empty
    - ``processed_at`` — ISO timestamp of when the processing happened
    """
    required = ("event_id", "timestamp", "type")
    for key in required:
        if key not in event or event[key] is None:
            logger.warning("Skipping event missing required field '%s'", key)
            return None

    try:
        ts = datetime.datetime.fromisoformat(event["timestamp"])
    except (ValueError, TypeError):
        logger.warning(
            "Skipping event %s: unparseable timestamp '%s'",
            event.get("event_id"),
            event["timestamp"],
        )
        return None

    enriched = dict(event)
    enriched["event_date"] = ts.strftime("%Y-%m-%d")
    enriched["hour"] = ts.hour
    enriched["is_purchase"] = event["type"] == "checkout"
    user_id = event.get("user_id")
    enriched["has_user"] = user_id is not None and user_id != ""
    enriched["processed_at"] = datetime.datetime.now(datetime.UTC).isoformat()

    return enriched


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------


def run_incremental_etl(
    input_dir: Path,
    output_dir: Path,
    checkpoint_path: Path,
    pipeline_name: str = "events_etl",
) -> RunSummary:
    """Run the full incremental ETL pipeline.

    1. Load checkpoint
    2. Read all events from *input_dir*
    3. Filter out already-processed events (by ``event_id`` in checkpoint)
    4. Transform new events
    5. Write output JSONL file to ``output_dir/{run_timestamp}.jsonl``
    6. Update checkpoint with newly processed IDs
    7. Return run summary

    If no new events exist, no output file is written but a summary is still
    returned.  If a transform fails on an event it is counted as failed but
    processing continues.  The checkpoint only records IDs of events that
    were successfully transformed.
    """
    run_at = datetime.datetime.now(datetime.UTC).isoformat()
    checkpoint = load_checkpoint(checkpoint_path, pipeline_name)

    all_events = read_events(input_dir)
    records_seen = len(all_events)

    # Deduplicate within the input by event_id (keep first occurrence)
    seen_in_batch: set[str] = set()
    unique_events: list[dict[str, Any]] = []
    for evt in all_events:
        eid = evt.get("event_id")
        if eid is not None and eid not in seen_in_batch:
            seen_in_batch.add(eid)
            unique_events.append(evt)

    # Filter out already-processed events
    new_events = [
        e for e in unique_events if e.get("event_id") not in checkpoint.processed_ids
    ]
    records_skipped = len(unique_events) - len(new_events)

    # Transform
    transformed: list[dict[str, Any]] = []
    records_failed = 0
    newly_processed_ids: set[str] = set()

    for event in new_events:
        result = transform_event(event)
        if result is None:
            records_failed += 1
        else:
            transformed.append(result)
            newly_processed_ids.add(event["event_id"])

    records_processed = len(transformed)

    # Write output
    if transformed:
        output_dir.mkdir(parents=True, exist_ok=True)
        ts_label = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
        output_path = output_dir / f"{ts_label}.jsonl"
        with output_path.open("w", encoding="utf-8") as fh:
            for record in transformed:
                fh.write(json.dumps(record) + "\n")
        logger.info("Wrote %d records to %s", len(transformed), output_path)

    # Update checkpoint
    checkpoint_updated = False
    if newly_processed_ids:
        checkpoint.processed_ids.update(newly_processed_ids)
        checkpoint.last_run_at = run_at
        checkpoint.total_runs += 1
        save_checkpoint(checkpoint_path, checkpoint)
        checkpoint_updated = True
    else:
        logger.info("No new events processed — checkpoint unchanged.")

    return RunSummary(
        pipeline_name=pipeline_name,
        run_at=run_at,
        records_seen=records_seen,
        records_skipped=records_skipped,
        records_processed=records_processed,
        records_failed=records_failed,
        checkpoint_updated=checkpoint_updated,
    )
