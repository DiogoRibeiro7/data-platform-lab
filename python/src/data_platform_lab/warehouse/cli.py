"""CLI entry point for the warehouse loading pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from data_platform_lab.warehouse.loader import run_warehouse_pipeline


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Load raw data into a SQLite star-schema warehouse and run analytical queries.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("../data/sample"),
        help="Directory with raw CSVs and events.json (default: ../data/sample).",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=":memory:",
        help="SQLite database path (default: :memory:).",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=Path("../data/gold/warehouse"),
        help="Directory for report CSVs and summary JSON (default: ../data/gold/warehouse).",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=Path("../sql"),
        help="Root of the SQL assets tree (default: ../sql).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and run the warehouse pipeline."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = _build_parser()
    args = parser.parse_args(argv)

    result = run_warehouse_pipeline(
        data_dir=args.data_dir,
        db_path=args.db_path,
        report_dir=args.report_dir,
        sql_dir=args.sql_dir,
    )

    # --- Print summary ---
    print()
    print("=== Warehouse Pipeline Summary ===")
    print(f"Status: {result['status']}")
    print(f"DB:     {result['db_path']}")

    print()
    print("--- Staging Tables ---")
    for table, count in result["staging_tables"].items():
        print(f"  {table:20s} {count:>6,} rows")

    print()
    print("--- Warehouse Tables ---")
    for table, count in result["warehouse_tables"].items():
        print(f"  {table:20s} {count:>6,} rows")

    print()
    print("--- Analytical Queries ---")
    for qr in result["queries"]:
        print(f"\n  {qr['name']} — {qr['description']}")
        print(f"    {qr['row_count']} rows returned")
        if qr["rows"]:
            for row in qr["rows"][:5]:
                cols = ", ".join(f"{k}={v}" for k, v in row.items())
                print(f"      {cols}")
            if len(qr["rows"]) > 5:
                print(f"      ... ({len(qr['rows']) - 5} more)")

    if args.report_dir:
        print(f"\nReports written to: {args.report_dir}")


if __name__ == "__main__":
    main()
    sys.exit(0)
