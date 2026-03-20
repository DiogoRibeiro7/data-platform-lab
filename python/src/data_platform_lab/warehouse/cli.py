"""CLI entry point for the warehouse loading pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from data_platform_lab.warehouse.loader import run_warehouse_pipeline


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        description="Load raw data into a SQLite star-schema warehouse and run analytical queries.",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Directory with raw CSVs and events.json (default: ../data/sample).",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="SQLite database path (default: :memory:).",
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        default=None,
        help="Directory for report CSVs and summary JSON (default: ../data/gold/warehouse).",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=None,
        help="Root of the SQL assets tree (default: ../sql).",
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
        "data_dir": "../data/sample",
        "db_path": ":memory:",
        "report_dir": "../data/gold/warehouse",
        "sql_dir": "../sql",
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
    if args.data_dir is not None:
        resolved["data_dir"] = str(args.data_dir)
    if args.db_path is not None:
        resolved["db_path"] = args.db_path
    if args.report_dir is not None:
        resolved["report_dir"] = str(args.report_dir)
    if args.sql_dir is not None:
        resolved["sql_dir"] = str(args.sql_dir)

    return resolved


def main(argv: list[str] | None = None) -> None:
    """Parse arguments and run the warehouse pipeline."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = _resolve_config(args)

    result = run_warehouse_pipeline(
        data_dir=cfg["data_dir"],
        db_path=cfg["db_path"],
        report_dir=cfg["report_dir"],
        sql_dir=cfg["sql_dir"],
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

    report_dir = Path(cfg["report_dir"])
    if report_dir:
        print(f"\nReports written to: {report_dir}")


if __name__ == "__main__":
    main()
    sys.exit(0)
