"""CLI entry point for the CSV ingestion pipeline."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from data_platform_lab.ingestion.csv_pipeline import run_pipeline


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Ingest, clean, and merge CSV files.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing the source CSV files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path for the cleaned output CSV.",
    )
    parser.add_argument(
        "--required-columns",
        type=str,
        default=None,
        help="Comma-separated list of required column names.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and run the pipeline."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    required: list[str] | None = None
    if args.required_columns:
        required = [c.strip() for c in args.required_columns.split(",")]

    result = run_pipeline(args.input_dir, args.output, required)

    print("=== Pipeline Summary ===")
    print(f"Files processed : {len(result.files_processed)}")
    for name in result.files_processed:
        print(f"  - {name}")
    if result.files_rejected:
        print(f"Files rejected  : {len(result.files_rejected)}")
        for reason in result.files_rejected:
            print(f"  - {reason}")
    print(f"Rows read       : {result.rows_read}")
    print(f"Duplicates removed: {result.duplicates_removed}")
    print(f"Rows written    : {result.rows_written}")


if __name__ == "__main__":
    main()
    sys.exit(0)
