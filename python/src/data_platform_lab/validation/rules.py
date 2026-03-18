"""Composable data-quality validation rules.

Each rule function accepts a dataset (list of dicts) plus rule-specific
parameters and returns a :class:`CheckResult` indicating pass/fail,
severity, and the indices of any failing rows.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class Severity(Enum):
    """How critical a validation failure is."""

    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class CheckResult:
    """Result of a single validation check."""

    name: str
    passed: bool
    severity: Severity
    message: str
    failing_rows: list[int] = field(default_factory=list)  # 0-based row indices


# ---------------------------------------------------------------------------
# Rule functions
# ---------------------------------------------------------------------------


def check_required_columns(
    records: list[dict[str, Any]],
    required: list[str],
    severity: Severity = Severity.CRITICAL,
) -> CheckResult:
    """Check that all required column names exist in the records.

    Columns are gathered from the union of keys across *all* rows so that a
    column present in at least one row counts as existing.  If the dataset is
    empty the check passes vacuously.
    """
    if not records:
        return CheckResult(
            name="required_columns",
            passed=True,
            severity=severity,
            message="No records to check.",
        )

    all_keys: set[str] = set()
    for record in records:
        all_keys.update(record.keys())

    missing = [col for col in required if col not in all_keys]

    if missing:
        return CheckResult(
            name="required_columns",
            passed=False,
            severity=severity,
            message=f"Missing required columns: {', '.join(missing)}",
        )

    return CheckResult(
        name="required_columns",
        passed=True,
        severity=severity,
        message="All required columns present.",
    )


def check_no_nulls(
    records: list[dict[str, Any]],
    columns: list[str],
    severity: Severity = Severity.CRITICAL,
) -> CheckResult:
    """Check that specified columns have no ``None`` or empty-string values.

    Returns the 0-based indices of every row that contains at least one null
    (``None`` or ``""``) in the specified columns.
    """
    failing: list[int] = []
    for idx, row in enumerate(records):
        for col in columns:
            value = row.get(col)
            if value is None or value == "":
                failing.append(idx)
                break  # one bad column is enough to flag the row

    if failing:
        return CheckResult(
            name="no_nulls",
            passed=False,
            severity=severity,
            message=(f"Found null/empty values in columns {columns} at rows: {failing}"),
            failing_rows=failing,
        )

    return CheckResult(
        name="no_nulls",
        passed=True,
        severity=severity,
        message="No null values found.",
    )


def check_unique(
    records: list[dict[str, Any]],
    columns: list[str],
    severity: Severity = Severity.CRITICAL,
) -> CheckResult:
    """Check that the combination of specified columns is unique across rows.

    All rows that participate in a duplicate group are reported as failing.
    """
    seen: dict[tuple[Any, ...], list[int]] = {}
    for idx, row in enumerate(records):
        key = tuple(row.get(col) for col in columns)
        seen.setdefault(key, []).append(idx)

    failing: list[int] = []
    for indices in seen.values():
        if len(indices) > 1:
            failing.extend(indices)

    failing.sort()

    if failing:
        return CheckResult(
            name="unique",
            passed=False,
            severity=severity,
            message=(f"Duplicate values found for columns {columns} at rows: {failing}"),
            failing_rows=failing,
        )

    return CheckResult(
        name="unique",
        passed=True,
        severity=severity,
        message="All values are unique.",
    )


def check_numeric_range(
    records: list[dict[str, Any]],
    column: str,
    min_value: float | None = None,
    max_value: float | None = None,
    severity: Severity = Severity.WARNING,
) -> CheckResult:
    """Check that a numeric column falls within [min_value, max_value].

    Rows where the column is missing or not numeric are silently skipped.
    """
    failing: list[int] = []
    for idx, row in enumerate(records):
        value = row.get(column)
        if value is None:
            continue
        if not isinstance(value, (int, float)):
            continue

        below = min_value is not None and value < min_value
        above = max_value is not None and value > max_value
        if below or above:
            failing.append(idx)

    if failing:
        bounds = _range_description(min_value, max_value)
        return CheckResult(
            name="numeric_range",
            passed=False,
            severity=severity,
            message=(f"Column '{column}' out of range {bounds} at rows: {failing}"),
            failing_rows=failing,
        )

    return CheckResult(
        name="numeric_range",
        passed=True,
        severity=severity,
        message=f"Column '{column}' within range.",
    )


def check_allowed_values(
    records: list[dict[str, Any]],
    column: str,
    allowed: set[str],
    severity: Severity = Severity.WARNING,
) -> CheckResult:
    """Check that a column contains only values from the *allowed* set.

    Returns the indices of rows whose value is not in *allowed*.
    """
    failing: list[int] = []
    for idx, row in enumerate(records):
        value = row.get(column)
        if value not in allowed:
            failing.append(idx)

    if failing:
        return CheckResult(
            name="allowed_values",
            passed=False,
            severity=severity,
            message=(f"Column '{column}' contains disallowed values at rows: {failing}"),
            failing_rows=failing,
        )

    return CheckResult(
        name="allowed_values",
        passed=True,
        severity=severity,
        message=f"All values in column '{column}' are allowed.",
    )


def check_date_format(
    records: list[dict[str, Any]],
    column: str,
    date_format: str = "%Y-%m-%d",
    severity: Severity = Severity.WARNING,
) -> CheckResult:
    """Check that a column matches the expected *date_format*.

    Uses :func:`datetime.datetime.strptime` to validate.  Returns failing-row
    indices where the value does not match.
    """
    failing: list[int] = []
    for idx, row in enumerate(records):
        value = row.get(column)
        if value is None:
            failing.append(idx)
            continue
        if not isinstance(value, str):
            failing.append(idx)
            continue
        try:
            datetime.strptime(value, date_format)
        except ValueError:
            failing.append(idx)

    if failing:
        return CheckResult(
            name="date_format",
            passed=False,
            severity=severity,
            message=(
                f"Column '{column}' has invalid date format "
                f"(expected '{date_format}') at rows: {failing}"
            ),
            failing_rows=failing,
        )

    return CheckResult(
        name="date_format",
        passed=True,
        severity=severity,
        message=f"All values in column '{column}' match format '{date_format}'.",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _range_description(
    min_value: float | None,
    max_value: float | None,
) -> str:
    """Return a human-readable description of a numeric range."""
    if min_value is not None and max_value is not None:
        return f"[{min_value}, {max_value}]"
    if min_value is not None:
        return f"[{min_value}, inf)"
    if max_value is not None:
        return f"(-inf, {max_value}]"
    return "(-inf, inf)"
