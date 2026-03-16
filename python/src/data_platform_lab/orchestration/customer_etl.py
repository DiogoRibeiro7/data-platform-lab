"""Customer ETL workflow — an orchestrated pipeline over real modules.

Demonstrates the orchestration runner wired to actual ingestion, validation,
and cleaning logic using the ``data/sample/customers.csv`` dataset.

Steps:
    1. **extract** — read the CSV, standardise headers, trim fields
    2. **validate** — run data-quality checks (required columns, no nulls,
       unique IDs, date format)
    3. **clean** — deduplicate rows
    4. **load** — write the cleaned CSV to an output path
    5. **report** — build a human-readable summary string

The workflow is driven entirely through the shared *context* dict that the
orchestration runner passes to each step.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Any

from data_platform_lab.ingestion.csv_pipeline import (
    deduplicate,
    read_csv_file,
    standardize_headers,
    trim_fields,
)
from data_platform_lab.orchestration.runner import Pipeline, format_result
from data_platform_lab.validation.rules import (
    check_date_format,
    check_no_nulls,
    check_required_columns,
    check_unique,
)
from data_platform_lab.validation.runner import format_report, run_validation

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Step functions — each receives and mutates the shared context dict
# ---------------------------------------------------------------------------


def extract(ctx: dict[str, Any]) -> dict[str, Any]:
    """Read a CSV file, standardise headers, trim fields.

    Reads ``ctx["input_path"]`` and stores headers + rows in context.
    """
    input_path = Path(ctx["input_path"])
    raw_headers, raw_rows = read_csv_file(input_path)

    headers = standardize_headers(raw_headers)
    rows = trim_fields(raw_rows)

    ctx["headers"] = headers
    ctx["rows"] = rows

    return {
        "rows_read": len(rows),
        "columns": len(headers),
    }


def validate(ctx: dict[str, Any]) -> dict[str, Any]:
    """Run quality checks on the extracted data.

    Converts row arrays to dicts, runs validation, stores the report in context.
    Raises if any critical check fails.
    """
    headers: list[str] = ctx["headers"]
    rows: list[list[str]] = ctx["rows"]

    records = [dict(zip(headers, row)) for row in rows]
    ctx["records"] = records

    checks: list[tuple[Any, dict[str, Any]]] = [
        (check_required_columns, {"required": ["customer_id", "email", "created_at"]}),
        (check_no_nulls, {"columns": ["customer_id", "first_name", "last_name"]}),
        (check_unique, {"columns": ["customer_id"]}),
        (check_date_format, {"column": "created_at"}),
    ]

    report = run_validation(records, checks, dataset_name="customers")
    ctx["validation_report"] = report

    return {
        "total_checks": report.total_checks,
        "passed": report.passed,
        "failed": report.failed,
        "status": report.status,
    }


def clean(ctx: dict[str, Any]) -> dict[str, Any]:
    """Deduplicate rows."""
    rows: list[list[str]] = ctx["rows"]
    unique_rows, removed = deduplicate(rows)
    ctx["rows"] = unique_rows

    return {
        "rows_before": len(rows),
        "rows_after": len(unique_rows),
        "duplicates_removed": removed,
    }


def load(ctx: dict[str, Any]) -> dict[str, Any]:
    """Write the cleaned dataset to a CSV file."""
    output_path = Path(ctx["output_path"])
    output_path.parent.mkdir(parents=True, exist_ok=True)

    headers: list[str] = ctx["headers"]
    rows: list[list[str]] = ctx["rows"]

    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)

    return {
        "output_path": str(output_path),
        "rows_written": len(rows),
    }


def report(ctx: dict[str, Any]) -> str:
    """Build a summary string from validation report and step results."""
    parts: list[str] = []

    validation_report = ctx.get("validation_report")
    if validation_report is not None:
        parts.append(format_report(validation_report))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Workflow builder
# ---------------------------------------------------------------------------


def build_customer_etl(
    input_path: str | Path,
    output_path: str | Path,
) -> Pipeline:
    """Build and return a Pipeline wired to the customer ETL steps.

    Call ``.run()`` on the returned pipeline to execute.
    """
    pipeline = Pipeline("customer_etl")
    pipeline.add_step("extract", extract)
    pipeline.add_step("validate", validate, allow_skip=True)
    pipeline.add_step("clean", clean)
    pipeline.add_step("load", load)
    pipeline.add_step("report", report)

    # Pre-populate context paths — run() will pass these to every step
    pipeline._initial_context = {
        "input_path": str(input_path),
        "output_path": str(output_path),
    }

    return pipeline


def run_customer_etl(
    input_path: str | Path,
    output_path: str | Path,
) -> dict[str, Any]:
    """Build and run the customer ETL pipeline. Returns the pipeline result."""
    pipeline = build_customer_etl(input_path, output_path)
    result = pipeline.run(pipeline._initial_context)
    return result
