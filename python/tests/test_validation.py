"""Tests for the data-quality validation framework."""

from __future__ import annotations

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

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

VALID_RECORDS: list[dict[str, object]] = [
    {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30,
        "status": "active",
        "created_at": "2024-01-15",
    },
    {
        "id": 2,
        "name": "Bob",
        "email": "bob@example.com",
        "age": 25,
        "status": "active",
        "created_at": "2024-02-20",
    },
    {
        "id": 3,
        "name": "Carla",
        "email": "carla@example.com",
        "age": 28,
        "status": "inactive",
        "created_at": "2024-03-10",
    },
]

BAD_RECORDS: list[dict[str, object]] = [
    {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30,
        "status": "active",
        "created_at": "2024-01-15",
    },
    {
        "id": 2,
        "name": "",
        "email": None,
        "age": -5,
        "status": "unknown",
        "created_at": "15/01/2024",
    },
    {
        "id": 1,
        "name": "Duplicate",
        "email": "dup@example.com",
        "age": 200,
        "status": "active",
        "created_at": "2024-13-01",
    },
    {
        "id": 4,
        "name": "David",
        "age": 35,
        "status": "active",
        "created_at": "2024-04-05",
    },
]


# ===================================================================
# check_required_columns
# ===================================================================


class TestCheckRequiredColumns:
    """Tests for :func:`check_required_columns`."""

    def test_all_present(self) -> None:
        result = check_required_columns(
            VALID_RECORDS, required=["id", "name", "email"]
        )
        assert result.passed is True
        assert result.failing_rows == []

    def test_missing_columns(self) -> None:
        result = check_required_columns(
            VALID_RECORDS, required=["id", "phone_number"]
        )
        assert result.passed is False
        assert "phone_number" in result.message

    def test_empty_records(self) -> None:
        result = check_required_columns([], required=["id"])
        assert result.passed is True


# ===================================================================
# check_no_nulls
# ===================================================================


class TestCheckNoNulls:
    """Tests for :func:`check_no_nulls`."""

    def test_no_nulls_pass(self) -> None:
        result = check_no_nulls(VALID_RECORDS, columns=["id", "name"])
        assert result.passed is True
        assert result.failing_rows == []

    def test_nulls_found(self) -> None:
        result = check_no_nulls(BAD_RECORDS, columns=["email"])
        assert result.passed is False
        # Row 1 has email=None, row 3 is missing email key entirely
        assert 1 in result.failing_rows
        assert 3 in result.failing_rows

    def test_empty_strings_count_as_null(self) -> None:
        result = check_no_nulls(BAD_RECORDS, columns=["name"])
        assert result.passed is False
        # Row 1 has name=""
        assert 1 in result.failing_rows


# ===================================================================
# check_unique
# ===================================================================


class TestCheckUnique:
    """Tests for :func:`check_unique`."""

    def test_all_unique(self) -> None:
        result = check_unique(VALID_RECORDS, columns=["id"])
        assert result.passed is True
        assert result.failing_rows == []

    def test_duplicates_found(self) -> None:
        result = check_unique(BAD_RECORDS, columns=["id"])
        assert result.passed is False
        # Rows 0 and 2 both have id=1
        assert 0 in result.failing_rows
        assert 2 in result.failing_rows


# ===================================================================
# check_numeric_range
# ===================================================================


class TestCheckNumericRange:
    """Tests for :func:`check_numeric_range`."""

    def test_within_range(self) -> None:
        result = check_numeric_range(
            VALID_RECORDS, column="age", min_value=0, max_value=150
        )
        assert result.passed is True
        assert result.failing_rows == []

    def test_out_of_range(self) -> None:
        result = check_numeric_range(
            BAD_RECORDS, column="age", min_value=0, max_value=150
        )
        assert result.passed is False
        # Row 1 has age=-5, row 2 has age=200
        assert 1 in result.failing_rows
        assert 2 in result.failing_rows

    def test_no_min_or_max(self) -> None:
        # Only upper bound
        result = check_numeric_range(
            BAD_RECORDS, column="age", max_value=150
        )
        assert result.passed is False
        assert 2 in result.failing_rows
        # Row 1 (age=-5) should pass because there is no min_value
        assert 1 not in result.failing_rows

        # Only lower bound
        result2 = check_numeric_range(
            BAD_RECORDS, column="age", min_value=0
        )
        assert result2.passed is False
        assert 1 in result2.failing_rows
        # Row 2 (age=200) should pass because there is no max_value
        assert 2 not in result2.failing_rows


# ===================================================================
# check_allowed_values
# ===================================================================


class TestCheckAllowedValues:
    """Tests for :func:`check_allowed_values`."""

    def test_all_allowed(self) -> None:
        result = check_allowed_values(
            VALID_RECORDS,
            column="status",
            allowed={"active", "inactive"},
        )
        assert result.passed is True
        assert result.failing_rows == []

    def test_disallowed_values(self) -> None:
        result = check_allowed_values(
            BAD_RECORDS,
            column="status",
            allowed={"active", "inactive"},
        )
        assert result.passed is False
        # Row 1 has status="unknown"
        assert 1 in result.failing_rows


# ===================================================================
# check_date_format
# ===================================================================


class TestCheckDateFormat:
    """Tests for :func:`check_date_format`."""

    def test_valid_dates(self) -> None:
        result = check_date_format(VALID_RECORDS, column="created_at")
        assert result.passed is True
        assert result.failing_rows == []

    def test_invalid_dates(self) -> None:
        result = check_date_format(BAD_RECORDS, column="created_at")
        assert result.passed is False
        # Row 1 "15/01/2024" wrong format, row 2 "2024-13-01" invalid month
        assert 1 in result.failing_rows
        assert 2 in result.failing_rows


# ===================================================================
# run_validation
# ===================================================================


class TestRunValidation:
    """Tests for :func:`run_validation`."""

    def test_all_pass(self) -> None:
        checks: list[tuple[object, dict[str, object]]] = [
            (check_required_columns, {"required": ["id", "name"]}),
            (check_no_nulls, {"columns": ["id", "name"]}),
            (check_unique, {"columns": ["id"]}),
        ]
        report = run_validation(VALID_RECORDS, checks, dataset_name="users")  # type: ignore[arg-type]
        assert report.status == "passed"
        assert report.total_checks == 3
        assert report.passed == 3
        assert report.failed == 0

    def test_mixed_results(self) -> None:
        checks: list[tuple[object, dict[str, object]]] = [
            (check_required_columns, {"required": ["id", "name"]}),
            (check_no_nulls, {"columns": ["email"]}),
            (check_unique, {"columns": ["id"]}),
            (
                check_numeric_range,
                {"column": "age", "min_value": 0, "max_value": 150},
            ),
        ]
        report = run_validation(BAD_RECORDS, checks, dataset_name="users")  # type: ignore[arg-type]
        assert report.failed > 0
        assert report.total_checks == 4

    def test_rule_that_raises_does_not_crash_runner(self) -> None:
        """If a rule function throws, it is recorded as a critical failure."""
        def exploding_rule(records: list[dict[str, object]], **kwargs: object) -> CheckResult:
            raise RuntimeError("unexpected error in rule")

        checks: list[tuple[object, dict[str, object]]] = [
            (check_required_columns, {"required": ["id"]}),
            (exploding_rule, {}),
        ]
        report = run_validation(VALID_RECORDS, checks, dataset_name="test")  # type: ignore[arg-type]
        assert report.total_checks == 2
        assert report.passed == 1
        assert report.failed == 1
        assert report.critical_failures == 1
        assert report.status == "failed"

    def test_empty_checks_list(self) -> None:
        """Empty checks list produces a passing report with zero counts."""
        report = run_validation(VALID_RECORDS, [], dataset_name="test")
        assert report.total_checks == 0
        assert report.passed == 0
        assert report.failed == 0
        assert report.status == "passed"

    def test_status_logic(self) -> None:
        # Only warnings (no critical failures) -> "warning"
        warning_checks: list[tuple[object, dict[str, object]]] = [
            (
                check_numeric_range,
                {
                    "column": "age",
                    "min_value": 0,
                    "max_value": 150,
                    "severity": Severity.WARNING,
                },
            ),
        ]
        report_w = run_validation(BAD_RECORDS, warning_checks, dataset_name="t")  # type: ignore[arg-type]
        assert report_w.status == "warning"

        # Critical failure -> "failed"
        critical_checks: list[tuple[object, dict[str, object]]] = [
            (
                check_no_nulls,
                {"columns": ["email"], "severity": Severity.CRITICAL},
            ),
        ]
        report_f = run_validation(BAD_RECORDS, critical_checks, dataset_name="t")  # type: ignore[arg-type]
        assert report_f.status == "failed"

        # All pass -> "passed"
        pass_checks: list[tuple[object, dict[str, object]]] = [
            (check_required_columns, {"required": ["id"]}),
        ]
        report_p = run_validation(VALID_RECORDS, pass_checks, dataset_name="t")  # type: ignore[arg-type]
        assert report_p.status == "passed"


# ===================================================================
# format_report
# ===================================================================


class TestFormatReport:
    """Tests for :func:`format_report`."""

    def test_format_report_output(self) -> None:
        checks: list[tuple[object, dict[str, object]]] = [
            (check_required_columns, {"required": ["id", "name"]}),
            (check_no_nulls, {"columns": ["email"]}),
        ]
        report = run_validation(BAD_RECORDS, checks, dataset_name="orders")  # type: ignore[arg-type]
        text = format_report(report)

        assert "Validation Report: orders" in text
        assert "Status:" in text
        assert "Total checks:" in text
        assert "Passed:" in text
        assert "Failed:" in text
        assert "[PASS]" in text or "[FAIL]" in text
