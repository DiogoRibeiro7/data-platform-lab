"""CLI for the benchmark exercise."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from data_platform_lab.benchmark.runner import (
    format_report,
    run_benchmark,
    save_report,
)


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Benchmark file-processing ingestion strategies.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=None,
        help="Root directory for benchmark files (default: ../data/benchmark).",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=None,
        help="Number of CSV files to generate (default: 50).",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=None,
        help="Rows per generated file (default: 100).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Thread/worker pool size for parallel strategies (default: 4).",
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
    resolved: dict[str, Any] = {
        "work_dir": "../data/benchmark",
        "num_files": 50,
        "rows_per_file": 100,
        "max_workers": 4,
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
    if args.work_dir is not None:
        resolved["work_dir"] = str(args.work_dir)
    if args.num_files is not None:
        resolved["num_files"] = args.num_files
    if args.rows_per_file is not None:
        resolved["rows_per_file"] = args.rows_per_file
    if args.max_workers is not None:
        resolved["max_workers"] = args.max_workers

    return resolved


def main(argv: list[str] | None = None) -> None:
    """Run the benchmark CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = _resolve_config(args)

    report = run_benchmark(
        work_dir=cfg["work_dir"],
        num_files=cfg["num_files"],
        rows_per_file=cfg["rows_per_file"],
        max_workers=cfg["max_workers"],
    )

    print()
    print(format_report(report))

    work_dir = Path(cfg["work_dir"])
    report_path = work_dir / "benchmark_report.json"
    save_report(report, report_path)
    print(f"\nReport saved to {report_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
