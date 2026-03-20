"""Sensor pipeline demo — ingest, validate, deduplicate, aggregate, and output.

Reads ``data/sample/sensor_events.json`` (JSONL), runs five orchestrated
steps through the :class:`Pipeline` runner with observability tracking,
dead-letter routing, hourly aggregation, and a manifest.

Run from the repository root::

    cd python
    poetry run python -m data_platform_lab.sensor_demo

Or with custom directories::

    poetry run python -m data_platform_lab.sensor_demo --data-dir ../data/sample
"""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from data_platform_lab.manifest import write_manifest
from data_platform_lab.observability.tracker import (
    RunTracker,
    format_run_metadata,
)
from data_platform_lab.orchestration.runner import Pipeline, format_result

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = ("sensor_id", "type", "value", "unit", "location", "timestamp")


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def _ingest(ctx: dict[str, Any]) -> dict[str, Any]:
    """Step 1: read sensor_events.json JSONL and parse each line."""
    data_dir = Path(ctx["data_dir"])
    path = data_dir / "sensor_events.json"

    raw_events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                raw_events.append(json.loads(line))

    ctx["raw_events"] = raw_events
    ctx["tracker"].inc_files_processed()
    ctx["tracker"].inc_rows_read(len(raw_events))

    return {"events_read": len(raw_events)}


def _validate(ctx: dict[str, Any]) -> dict[str, Any]:
    """Step 2: validate each event; split into accepted and rejected."""
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    rejection_reasons: dict[str, int] = {}

    for event in ctx["raw_events"]:
        reasons: list[str] = []

        # Check required fields
        for field in REQUIRED_FIELDS:
            if field not in event:
                reasons.append(f"missing_{field}")

        # Value must be a number (not null / None)
        if "value" in event and not isinstance(event["value"], (int, float)):
            reasons.append("value_not_numeric")

        # Timestamp must be parseable
        if "timestamp" in event:
            try:
                datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                reasons.append("invalid_timestamp")

        if reasons:
            rejected.append({**event, "_rejection_reasons": reasons})
            for r in reasons:
                rejection_reasons[r] = rejection_reasons.get(r, 0) + 1
        else:
            accepted.append(event)

    ctx["accepted"] = accepted
    ctx["rejected"] = rejected
    ctx["tracker"].inc_rows_rejected(len(rejected))

    return {
        "accepted": len(accepted),
        "rejected": len(rejected),
        "rejection_reasons": rejection_reasons,
    }


def _deduplicate(ctx: dict[str, Any]) -> dict[str, Any]:
    """Step 3: deduplicate by sensor_id::timestamp key (first wins)."""
    seen: set[str] = set()
    deduplicated: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []

    for event in ctx["accepted"]:
        key = f"{event['sensor_id']}::{event['timestamp']}"
        if key in seen:
            duplicates.append(event)
        else:
            seen.add(key)
            deduplicated.append(event)

    ctx["deduplicated"] = deduplicated
    ctx["duplicates"] = duplicates

    return {
        "before": len(ctx["accepted"]),
        "after": len(deduplicated),
        "duplicates_removed": len(duplicates),
    }


def _aggregate(ctx: dict[str, Any]) -> dict[str, Any]:
    """Step 4: compute hourly aggregates by sensor_id and per-location totals."""
    hourly: dict[str, list[float]] = {}
    location_events: dict[str, int] = {}
    location_sensors: dict[str, set[str]] = {}

    for event in ctx["deduplicated"]:
        ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
        hour_key = ts.strftime("%Y-%m-%dT%H:00:00Z")
        bucket_key = f"{event['sensor_id']}|{hour_key}"

        hourly.setdefault(bucket_key, []).append(float(event["value"]))

        loc = event["location"]
        location_events[loc] = location_events.get(loc, 0) + 1
        location_sensors.setdefault(loc, set()).add(event["sensor_id"])

    hourly_aggregates: list[dict[str, Any]] = []
    for bucket_key, values in sorted(hourly.items()):
        sensor_id, hour = bucket_key.split("|", 1)
        hourly_aggregates.append({
            "sensor_id": sensor_id,
            "hour": hour,
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": round(sum(values) / len(values), 4),
        })

    location_summary: list[dict[str, Any]] = []
    for loc in sorted(location_events):
        location_summary.append({
            "location": loc,
            "event_count": location_events[loc],
            "sensor_count": len(location_sensors[loc]),
        })

    ctx["hourly_aggregates"] = hourly_aggregates
    ctx["location_summary"] = location_summary

    return {
        "hourly_buckets": len(hourly_aggregates),
        "locations": len(location_summary),
    }


def _output(ctx: dict[str, Any]) -> dict[str, Any]:
    """Step 5: write output files."""
    output_dir = Path(ctx["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    tracker: RunTracker = ctx["tracker"]

    # accepted.jsonl — valid deduplicated events
    accepted_path = output_dir / "accepted.jsonl"
    with accepted_path.open("w", encoding="utf-8") as fh:
        for event in ctx["deduplicated"]:
            fh.write(json.dumps(event) + "\n")
    tracker.inc_rows_written(len(ctx["deduplicated"]))

    # dead_letter.jsonl — rejected + duplicate events with reason
    dead_letter_path = output_dir / "dead_letter.jsonl"
    with dead_letter_path.open("w", encoding="utf-8") as fh:
        for event in ctx["rejected"]:
            fh.write(json.dumps(event) + "\n")
        for event in ctx["duplicates"]:
            fh.write(json.dumps({**event, "_rejection_reasons": ["duplicate"]}) + "\n")

    # hourly_aggregates.csv
    agg_path = output_dir / "hourly_aggregates.csv"
    with agg_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["sensor_id", "hour", "count", "min", "max", "avg"],
        )
        writer.writeheader()
        writer.writerows(ctx["hourly_aggregates"])

    # location_summary.csv
    loc_path = output_dir / "location_summary.csv"
    with loc_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["location", "event_count", "sensor_count"],
        )
        writer.writeheader()
        writer.writerows(ctx["location_summary"])

    # summary.json
    summary_path = output_dir / "summary.json"
    summary = {
        "pipeline_name": ctx["pipeline_name"],
        "events_read": len(ctx["raw_events"]),
        "accepted": len(ctx["deduplicated"]),
        "rejected": len(ctx["rejected"]),
        "duplicates_removed": len(ctx["duplicates"]),
        "hourly_buckets": len(ctx["hourly_aggregates"]),
        "locations": len(ctx["location_summary"]),
        "step_results": ctx["step_results"],
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return {"files_written": 5, "output_dir": str(output_dir)}


# ---------------------------------------------------------------------------
# Main pipeline function
# ---------------------------------------------------------------------------


def run_sensor_demo(
    data_dir: str | Path = "data/sample",
    output_dir: str | Path = "data/silver/sensor_demo",
    manifest_dir: str | Path = "data/manifests",
) -> dict[str, Any]:
    """Run the sensor pipeline demo.

    Returns a dict with:
    - ``pipeline_result``: the :class:`PipelineResult` from the orchestration runner
    - ``metadata``: :class:`RunMetadata` from the tracker
    - ``manifest_path``: path to the manifest file
    - ``output_dir``: output directory path
    """
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    manifest_dir = Path(manifest_dir)

    tracker = RunTracker("sensor_demo")

    with tracker:
        # Build the shared context
        ctx: dict[str, Any] = {
            "data_dir": str(data_dir),
            "output_dir": str(output_dir),
            "tracker": tracker,
        }

        # Wire up the pipeline
        pipeline = Pipeline("sensor_demo")
        pipeline.add_step("ingest", _ingest)
        pipeline.add_step("validate", _validate)
        pipeline.add_step("deduplicate", _deduplicate)
        pipeline.add_step("aggregate", _aggregate)
        pipeline.add_step("output", _output)

        pipeline_result = pipeline.run(ctx)

        # Feed pipeline counters into the tracker
        tracker.set_extra("events_read", len(ctx.get("raw_events", [])))
        tracker.set_extra("accepted", len(ctx.get("deduplicated", [])))
        tracker.set_extra("rejected", len(ctx.get("rejected", [])))
        tracker.set_extra("duplicates_removed", len(ctx.get("duplicates", [])))
        tracker.set_extra("output_dir", str(output_dir))

    meta = tracker.metadata
    run_id = meta.run_id

    # Write manifest
    output_files = [
        str(output_dir / f)
        for f in (
            "accepted.jsonl",
            "dead_letter.jsonl",
            "hourly_aggregates.csv",
            "location_summary.csv",
            "summary.json",
        )
    ]
    manifest_path = write_manifest(
        pipeline_name="sensor_demo",
        run_id=run_id,
        source=str(data_dir / "sensor_events.json"),
        output=output_files,
        row_count=len(ctx.get("deduplicated", [])),
        status=pipeline_result.status,
        schema_hint=list(REQUIRED_FIELDS),
        warnings=meta.warnings or None,
        extras={
            "pipeline_duration_seconds": pipeline_result.duration_seconds,
            "steps_passed": pipeline_result.steps_passed,
            "steps_failed": pipeline_result.steps_failed,
        },
        manifest_dir=manifest_dir,
    )

    return {
        "pipeline_result": pipeline_result,
        "metadata": meta,
        "manifest_path": manifest_path,
        "output_dir": output_dir,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for the sensor demo."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the sensor pipeline demo.",
    )
    parser.add_argument(
        "--data-dir",
        default="../data/sample",
        help="Directory containing sensor_events.json.",
    )
    parser.add_argument(
        "--output-dir",
        default="../data/silver/sensor_demo",
        help="Directory for output files.",
    )
    parser.add_argument(
        "--manifest-dir",
        default="../data/manifests",
        help="Directory for the run manifest JSON.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    result = run_sensor_demo(args.data_dir, args.output_dir, args.manifest_dir)

    print()
    print(format_result(result["pipeline_result"]))
    print()
    print(format_run_metadata(result["metadata"]))
    print()
    print(f"Output: {result['output_dir']}")
    print(f"Manifest: {result['manifest_path']}")


if __name__ == "__main__":
    main()
