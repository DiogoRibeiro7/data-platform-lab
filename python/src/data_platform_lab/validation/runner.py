"""Validation runner — execute multiple rules and aggregate results."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from data_platform_lab.validation.rules import CheckResult, Severity

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Aggregated result of running multiple validation checks."""

    dataset_name: str
    total_checks: int
    passed: int
    failed: int
    warnings: int
    critical_failures: int
    status: str  # "passed", "warning", "failed"
    checks: list[CheckResult] = field(default_factory=list)


def run_validation(
    records: list[dict[str, Any]],
    checks: list[tuple[Callable[..., CheckResult], dict[str, Any]]],
    dataset_name: str = "dataset",
) -> ValidationReport:
    """Run a list of validation checks and return an aggregated report.

    Each element of *checks* is a ``(rule_function, kwargs_dict)`` tuple.
    The *records* list is automatically passed as the first positional
    argument to every rule function.

    Example::

        checks = [
            (check_required_columns, {"required": ["id", "name"]}),
            (check_no_nulls, {"columns": ["id"]}),
            (check_unique, {"columns": ["id"]}),
        ]
        report = run_validation(data, checks, dataset_name="users")
    """
    results: list[CheckResult] = []

    for rule_fn, kwargs in checks:
        logger.debug("Running check: %s", rule_fn.__name__)
        try:
            result = rule_fn(records, **kwargs)
        except Exception:
            logger.exception("Check '%s' raised an exception", rule_fn.__name__)
            result = CheckResult(
                name=rule_fn.__name__,
                passed=False,
                severity=Severity.CRITICAL,
                message="Check raised an exception — see logs for details.",
                failing_rows=[],
            )
        results.append(result)

    passed = sum(1 for r in results if r.passed)
    failed_results = [r for r in results if not r.passed]
    failed = len(failed_results)
    warnings = sum(1 for r in failed_results if r.severity == Severity.WARNING)
    critical_failures = sum(1 for r in failed_results if r.severity == Severity.CRITICAL)

    if critical_failures > 0:
        status = "failed"
    elif warnings > 0:
        status = "warning"
    else:
        status = "passed"

    return ValidationReport(
        dataset_name=dataset_name,
        total_checks=len(results),
        passed=passed,
        failed=failed,
        warnings=warnings,
        critical_failures=critical_failures,
        status=status,
        checks=results,
    )


def format_report(report: ValidationReport) -> str:
    """Format a :class:`ValidationReport` as a human-readable string."""
    lines: list[str] = [
        f"Validation Report: {report.dataset_name}",
        "=" * 50,
        f"Status: {report.status.upper()}",
        f"Total checks: {report.total_checks}",
        f"Passed: {report.passed}",
        f"Failed: {report.failed}",
        f"  Warnings: {report.warnings}",
        f"  Critical: {report.critical_failures}",
        "-" * 50,
    ]

    for check in report.checks:
        icon = "PASS" if check.passed else "FAIL"
        lines.append(f"[{icon}] {check.name} ({check.severity.value}): {check.message}")

    lines.append("=" * 50)
    return "\n".join(lines)
