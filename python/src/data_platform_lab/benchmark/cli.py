"""CLI for the benchmark exercise."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from data_platform_lab.benchmark.runner import (
    format_report,
    run_benchmark,
    save_report,
)


def main(argv: list[str] | None = None) -> None:
    """Run the benchmark CLI."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

    parser = argparse.ArgumentParser(
        description="Benchmark file-processing ingestion strategies.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("../data/benchmark"),
        help="Root directory for benchmark files (default: ../data/benchmark).",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=50,
        help="Number of CSV files to generate (default: 50).",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=100,
        help="Rows per generated file (default: 100).",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Thread/worker pool size for parallel strategies (default: 4).",
    )
    args = parser.parse_args(argv)

    report = run_benchmark(
        work_dir=args.work_dir,
        num_files=args.num_files,
        rows_per_file=args.rows_per_file,
        max_workers=args.max_workers,
    )

    print()
    print(format_report(report))

    report_path = args.work_dir / "benchmark_report.json"
    save_report(report, report_path)
    print(f"\nReport saved to {report_path}")


if __name__ == "__main__":
    main()
    sys.exit(0)
