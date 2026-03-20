"""CLI entry point for the streaming sensor-event processor."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from data_platform_lab.streaming.processor import process_stream


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Process a JSONL file of sensor events — validate, deduplicate, aggregate.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Path to the JSONL input file (e.g. data/sample/sensor_events.json).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for output files (accepted.jsonl, dead_letter.jsonl, summary.json).",
    )
    parser.add_argument(
        "--pipeline-name",
        type=str,
        default=None,
        help="Name for this pipeline run (default: sensor_stream).",
    )
    parser.add_argument(
        "--lateness-threshold",
        type=float,
        default=None,
        help="Allowed lateness in seconds before flagging an event (default: 0).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a JSON config file. CLI flags override config values.",
    )
    return parser


def _resolve_config(args: argparse.Namespace) -> dict[str, Any]:
    """Merge defaults, config file, and CLI flags.

    Precedence: defaults < config file < CLI flags.
    """
    # Defaults
    resolved: dict[str, Any] = {
        "input": None,
        "output_dir": None,
        "pipeline_name": "sensor_stream",
        "lateness_threshold": 0.0,
    }

    # Config file layer
    if args.config:
        from data_platform_lab.config import ConfigError, load_config, validate_config

        try:
            config_data = load_config(args.config)
        except ConfigError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)
        errors = validate_config(
            config_data,
            known=list(resolved.keys()),
        )
        if errors:
            for e in errors:
                print(f"Config error: {e}", file=sys.stderr)
            sys.exit(1)
        for key in resolved:
            if key in config_data:
                resolved[key] = config_data[key]

    # CLI flag layer (only override if explicitly provided, i.e. not None)
    if args.input is not None:
        resolved["input"] = str(args.input)
    if args.output_dir is not None:
        resolved["output_dir"] = str(args.output_dir)
    if args.pipeline_name is not None:
        resolved["pipeline_name"] = args.pipeline_name
    if args.lateness_threshold is not None:
        resolved["lateness_threshold"] = args.lateness_threshold

    return resolved


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and run the streaming processor."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = _resolve_config(args)

    if not cfg["input"]:
        parser.error("--input is required (provide via CLI or config file)")
    if not cfg["output_dir"]:
        parser.error("--output-dir is required (provide via CLI or config file)")

    summary = process_stream(
        cfg["input"],
        cfg["output_dir"],
        cfg["pipeline_name"],
        lateness_threshold_seconds=cfg["lateness_threshold"],
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

    output_dir_path = Path(cfg["output_dir"])
    print(f"\nFull summary written to {output_dir_path / 'summary.json'}")


if __name__ == "__main__":
    main()
    sys.exit(0)
