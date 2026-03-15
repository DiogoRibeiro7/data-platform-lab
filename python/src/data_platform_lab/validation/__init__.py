"""Validation — enforce schemas and data quality checks at pipeline boundaries.

Covers schema definition, contract enforcement, anomaly detection, row-level
and dataset-level checks, and dead-letter routing for invalid records.
"""

from data_platform_lab.validation.rules import (
    CheckResult,
    Severity,
    check_allowed_values,
    check_date_format,
    check_no_nulls,
    check_numeric_range,
    check_required_columns,
    check_unique,
)
from data_platform_lab.validation.runner import (
    ValidationReport,
    format_report,
    run_validation,
)

__all__ = [
    "CheckResult",
    "Severity",
    "ValidationReport",
    "check_allowed_values",
    "check_date_format",
    "check_no_nulls",
    "check_numeric_range",
    "check_required_columns",
    "check_unique",
    "format_report",
    "run_validation",
]
