"""End-to-end e-commerce demo pipeline.

Ingests customers, products, orders, and order_items from ``data/sample/``,
validates each dataset, cleans and standardises records, writes curated
outputs to ``data/silver/demo/``, and produces a structured run summary
with observability metadata.

Run from the repository root::

    cd python
    poetry run python -m data_platform_lab.demo

Or with a custom data directory::

    poetry run python -m data_platform_lab.demo --data-dir ../data/sample
"""

from __future__ import annotations

import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any

from data_platform_lab.ingestion.csv_pipeline import (
    deduplicate,
    read_csv_file,
    standardize_headers,
    trim_fields,
)
from data_platform_lab.observability.tracker import (
    RunTracker,
    format_run_metadata,
    metadata_to_dict,
)
from data_platform_lab.validation.rules import (
    Severity,
    check_allowed_values,
    check_date_format,
    check_no_nulls,
    check_numeric_range,
    check_required_columns,
    check_unique,
)
from data_platform_lab.validation.runner import run_validation

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_and_prepare(path: Path) -> tuple[list[str], list[list[str]]]:
    """Read a CSV, standardise headers, trim fields."""
    raw_headers, raw_rows = read_csv_file(path)
    headers = standardize_headers(raw_headers)
    rows = trim_fields(raw_rows)
    return headers, rows


def _rows_to_dicts(
    headers: list[str], rows: list[list[str]]
) -> list[dict[str, str]]:
    """Convert parallel arrays into a list of dicts."""
    return [dict(zip(headers, row)) for row in rows]


def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    """Write headers + rows as CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Per-table processing
# ---------------------------------------------------------------------------


def process_customers(
    path: Path, tracker: RunTracker
) -> tuple[list[str], list[list[str]], dict[str, Any]]:
    """Ingest, validate, and clean the customers table."""
    headers, rows = _read_and_prepare(path)
    tracker.inc_files_processed()
    tracker.inc_rows_read(len(rows))

    records = _rows_to_dicts(headers, rows)
    report = run_validation(
        records,
        [
            (check_required_columns, {"required": ["customer_id", "email", "created_at"]}),
            (check_no_nulls, {"columns": ["customer_id", "first_name", "last_name"]}),
            (check_unique, {"columns": ["customer_id"]}),
            (check_date_format, {"column": "created_at"}),
        ],
        dataset_name="customers",
    )

    if report.failed > 0:
        tracker.add_warning(
            f"customers: {report.failed} validation check(s) failed"
        )

    # Clean: deduplicate, standardise country casing
    unique_rows, dups = deduplicate(rows)
    if dups:
        tracker.add_warning(f"customers: removed {dups} duplicate row(s)")

    country_idx = headers.index("country")
    for row in unique_rows:
        row[country_idx] = row[country_idx].strip().title()

    tracker.inc_rows_rejected(len(rows) - len(unique_rows))

    return headers, unique_rows, {
        "source": str(path),
        "rows_read": len(rows),
        "rows_out": len(unique_rows),
        "duplicates_removed": dups,
        "validation_status": report.status,
        "validation_checks": report.total_checks,
        "validation_passed": report.passed,
        "validation_failed": report.failed,
    }


def process_products(
    path: Path, tracker: RunTracker
) -> tuple[list[str], list[list[str]], dict[str, Any]]:
    """Ingest, validate, and clean the products table."""
    headers, rows = _read_and_prepare(path)
    tracker.inc_files_processed()
    tracker.inc_rows_read(len(rows))

    records = _rows_to_dicts(headers, rows)
    report = run_validation(
        records,
        [
            (check_required_columns, {"required": ["product_id", "name", "price"]}),
            (check_unique, {"columns": ["product_id"]}),
            (check_numeric_range, {"column": "price", "min_value": 0, "severity": Severity.WARNING}),
            (check_allowed_values, {"column": "currency", "allowed": {"EUR"}, "severity": Severity.WARNING}),
        ],
        dataset_name="products",
    )

    if report.failed > 0:
        tracker.add_warning(
            f"products: {report.failed} validation check(s) failed"
        )

    # Clean: filter out rows with negative prices
    price_idx = headers.index("price")
    clean_rows = []
    rejected = 0
    for row in rows:
        try:
            if float(row[price_idx]) < 0:
                rejected += 1
                continue
        except ValueError:
            rejected += 1
            continue
        clean_rows.append(row)

    if rejected:
        tracker.add_warning(f"products: filtered {rejected} row(s) with invalid price")

    tracker.inc_rows_rejected(rejected)

    return headers, clean_rows, {
        "source": str(path),
        "rows_read": len(rows),
        "rows_out": len(clean_rows),
        "rows_filtered": rejected,
        "validation_status": report.status,
    }


def process_orders(
    path: Path,
    valid_customer_ids: set[str],
    tracker: RunTracker,
) -> tuple[list[str], list[list[str]], dict[str, Any]]:
    """Ingest, validate, and clean the orders table."""
    headers, rows = _read_and_prepare(path)
    tracker.inc_files_processed()
    tracker.inc_rows_read(len(rows))

    records = _rows_to_dicts(headers, rows)
    report = run_validation(
        records,
        [
            (check_required_columns, {"required": ["order_id", "customer_id", "order_date"]}),
            (check_unique, {"columns": ["order_id"]}),
            (check_allowed_values, {"column": "status", "allowed": {"completed", "shipped", "pending", "cancelled"}}),
        ],
        dataset_name="orders",
    )

    if report.failed > 0:
        tracker.add_warning(
            f"orders: {report.failed} validation check(s) failed"
        )

    # Clean: fix date format, flag orphan FKs
    cid_idx = headers.index("customer_id")
    date_idx = headers.index("order_date")
    orphan_count = 0
    for row in rows:
        row[date_idx] = row[date_idx].replace("/", "-")
        if row[cid_idx] not in valid_customer_ids:
            orphan_count += 1

    if orphan_count:
        tracker.add_warning(
            f"orders: {orphan_count} row(s) reference non-existent customer_id"
        )

    return headers, rows, {
        "source": str(path),
        "rows_read": len(rows),
        "rows_out": len(rows),
        "orphan_customer_ids": orphan_count,
        "validation_status": report.status,
    }


def process_order_items(
    path: Path, tracker: RunTracker
) -> tuple[list[str], list[list[str]], dict[str, Any]]:
    """Ingest, validate, and clean the order_items table."""
    headers, rows = _read_and_prepare(path)
    tracker.inc_files_processed()
    tracker.inc_rows_read(len(rows))

    records = _rows_to_dicts(headers, rows)
    report = run_validation(
        records,
        [
            (check_required_columns, {"required": ["order_id", "product_id", "quantity", "unit_price"]}),
        ],
        dataset_name="order_items",
    )

    if report.failed > 0:
        tracker.add_warning(
            f"order_items: {report.failed} validation check(s) failed"
        )

    # Clean: deduplicate
    unique_rows, dups = deduplicate(rows)
    if dups:
        tracker.add_warning(f"order_items: removed {dups} duplicate row(s)")
    tracker.inc_rows_rejected(dups)

    return headers, unique_rows, {
        "source": str(path),
        "rows_read": len(rows),
        "rows_out": len(unique_rows),
        "duplicates_removed": dups,
        "validation_status": report.status,
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_demo(
    data_dir: str | Path = "data/sample",
    output_dir: str | Path = "data/silver/demo",
    manifest_dir: str | Path = "data/manifests",
) -> dict[str, Any]:
    """Run the full e-commerce demo pipeline.

    Returns a dict with ``tracker_metadata`` and per-table ``tables`` summaries.
    """
    data_dir = Path(data_dir)
    output_dir = Path(output_dir)
    manifest_dir = Path(manifest_dir)

    tracker = RunTracker("ecommerce_demo")
    tables: dict[str, Any] = {}

    with tracker:
        # --- Customers ---
        c_headers, c_rows, c_summary = process_customers(
            data_dir / "customers.csv", tracker
        )
        _write_csv(output_dir / "customers.csv", c_headers, c_rows)
        tracker.inc_rows_written(len(c_rows))
        tables["customers"] = c_summary

        # --- Products ---
        p_headers, p_rows, p_summary = process_products(
            data_dir / "products.csv", tracker
        )
        _write_csv(output_dir / "products.csv", p_headers, p_rows)
        tracker.inc_rows_written(len(p_rows))
        tables["products"] = p_summary

        # --- Orders (needs valid customer IDs for FK check) ---
        valid_cids = {row[c_headers.index("customer_id")] for row in c_rows}
        o_headers, o_rows, o_summary = process_orders(
            data_dir / "orders.csv", valid_cids, tracker
        )
        _write_csv(output_dir / "orders.csv", o_headers, o_rows)
        tracker.inc_rows_written(len(o_rows))
        tables["orders"] = o_summary

        # --- Order Items ---
        oi_headers, oi_rows, oi_summary = process_order_items(
            data_dir / "order_items.csv", tracker
        )
        _write_csv(output_dir / "order_items.csv", oi_headers, oi_rows)
        tracker.inc_rows_written(len(oi_rows))
        tables["order_items"] = oi_summary

        # --- Manifest ---
        tracker.set_extra("tables_processed", len(tables))
        tracker.set_extra("output_dir", str(output_dir))

    meta = tracker.metadata

    # Write JSON manifest
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"ecommerce_demo_{meta.run_id}.json"
    manifest = {
        "run": metadata_to_dict(meta),
        "tables": tables,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    return {
        "metadata": meta,
        "tables": tables,
        "manifest_path": str(manifest_path),
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for the demo."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run the e-commerce demo pipeline.",
    )
    parser.add_argument(
        "--data-dir",
        default="../data/sample",
        help="Directory containing the sample CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="../data/silver/demo",
        help="Directory for cleaned output CSVs.",
    )
    parser.add_argument(
        "--manifest-dir",
        default="../data/manifests",
        help="Directory for the run manifest JSON.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    result = run_demo(args.data_dir, args.output_dir, args.manifest_dir)

    print()
    print(format_run_metadata(result["metadata"]))
    print()
    for table_name, summary in result["tables"].items():
        print(f"  {table_name}: {summary['rows_read']} read -> {summary['rows_out']} out")
    print()
    print(f"Manifest: {result['manifest_path']}")


if __name__ == "__main__":
    main()
