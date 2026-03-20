"""CLI entry point for the streaming sensor-event processor."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from data_platform_lab.streaming.processor import process_stream


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Process a JSONL file of sensor events — validate, deduplicate, aggregate.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the JSONL input file (e.g. data/sample/sensor_events.json).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory for output files (accepted.jsonl, dead_letter.jsonl, summary.json).",
    )
    parser.add_argument(
        "--pipeline-name",
        type=str,
        default="sensor_stream",
        help="Name for this pipeline run (default: sensor_stream).",
    )
    parser.add_argument(
        "--lateness-threshold",
        type=float,
        default=0.0,
        help="Allowed lateness in seconds before flagging an event (default: 0).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and run the streaming processor."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = _build_parser()
    args = parser.parse_args(argv)

    summary = process_stream(
        args.input, args.output_dir, args.pipeline_name,
        lateness_threshold_seconds=args.lateness_threshold,
    )

    print("\n=== Stream Processing Summary ===")
    print(f"Pipeline        : {summary.pipeline_name}")
    print(f"Status          : {summary.status}")
    print(f"Events seen     : {summary.events_seen}")
    print(f"Events accepted : {summary.events_accepted}")
    print(f"Events rejected : {summary.events_rejected}")
    print(f"Events duplicate: {summary.events_duplicate}")
    print(f"Events late     : {summary.events_late}")
    print(f"Dead letter     : {summary.dead_letter_count}")
    print(f"Duration        : {summary.duration_seconds}s")
    if summary.events_late > 0:
        print(f"Max lateness    : {summary.max_lateness_seconds}s")
        print(f"Watermark       : {summary.watermark}")

    if summary.rejection_reasons:
        print("\nRejection reasons:")
        for reason, count in summary.rejection_reasons.items():
            print(f"  {reason}: {count}")

    if summary.aggregates.get("by_sensor"):
        print(f"\nSensors tracked : {len(summary.aggregates['by_sensor'])}")

    print(f"\nFull summary written to {args.output_dir / 'summary.json'}")


if __name__ == "__main__":
    main()
    sys.exit(0)
