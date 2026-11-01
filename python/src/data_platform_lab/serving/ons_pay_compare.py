"""Read-only, null-safe release comparison of two explicitly pinned ONS pay vintages.

Only business columns in validated Silver Parquet are compared. Registry and
source hashes are deliberately excluded: they identify the two vintages and
must not be mistaken for a revision to every underlying observation.
"""

from __future__ import annotations

import math
import re
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, cast

import duckdb

from data_platform_lab.curation.ons_pay import MODEL_VERSION
from data_platform_lab.serving.catalog import CatalogError, CuratedCatalog, VintageKey

_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_CONTENT_COLUMNS = (
    "area_name",
    "geography",
    "pay_year",
    "weekly_pay_gbp",
    "lci_95_gbp",
    "uci_95_gbp",
    "status_code",
    "notes",
)

ChangeType = Literal["added", "removed", "revised"]
PolicyState = Literal["pass", "breach", "skipped"]
JsonScalar = str | int | float | None


class ComparisonError(ValueError):
    """A pin, published table, or comparison policy is untrusted or invalid."""


@dataclass(frozen=True, slots=True)
class PolicyCheck:
    name: str
    state: PolicyState
    observed: int | float | None
    maximum: int | float | None


@dataclass(frozen=True, slots=True)
class ChangeExample:
    area_code: str
    reference_date: str
    change_type: ChangeType
    changed_fields: tuple[str, ...]
    baseline: dict[str, JsonScalar] | None
    candidate: dict[str, JsonScalar] | None


@dataclass(frozen=True, slots=True)
class ComparisonCounts:
    baseline_rows: int
    candidate_rows: int
    added: int
    removed: int
    revised: int
    unchanged: int
    newly_missing_pay: int
    newly_reported_pay: int
    numeric_pay_revisions: int
    max_absolute_pay_revision_gbp: float | None
    baseline_missing_pay_percent: float
    candidate_missing_pay_percent: float
    missing_pay_increase_pp: float


@dataclass(frozen=True, slots=True)
class ComparisonReport:
    baseline: VintageKey
    candidate: VintageKey
    status: Literal["pass", "breach"]
    counts: ComparisonCounts
    changed_fields: dict[str, int]
    policy_checks: tuple[PolicyCheck, ...]
    examples: dict[str, tuple[ChangeExample, ...]]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible, deterministic audit of a candidate release."""
        return asdict(self)


def _validate_key(key: VintageKey, name: str) -> None:
    if (
        type(key.model_version) is not int
        or key.model_version != MODEL_VERSION
        or not isinstance(key.registry_commit, str)
        or not _COMMIT.fullmatch(key.registry_commit)
        or not isinstance(key.source_sha256, str)
        or not _SHA.fullmatch(key.source_sha256)
    ):
        raise ComparisonError(f"{name} requires exact v1 registry/source pins")


def _row_count(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ComparisonError(f"invalid {name} in release comparison")
    return value


def _validate_max(value: int | float | None, name: str, *, max_value: float | None = None) -> None:
    if value is None:
        return
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        raise ComparisonError(f"{name} must be a non-negative finite number")
    if max_value is not None and value > max_value:
        raise ComparisonError(f"{name} cannot exceed {max_value}")


def _policy(name: str, observed: int | float | None, maximum: int | float | None) -> PolicyCheck:
    if maximum is None:
        return PolicyCheck(name, "skipped", observed, None)
    if observed is None:
        # A requested policy cannot silently pass when the comparable metric
        # is unmeasurable (for example, two snapshots with disjoint keys).
        return PolicyCheck(name, "breach", None, maximum)
    return PolicyCheck(name, "pass" if observed <= maximum else "breach", observed, maximum)


def _check_unique_grain(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    # Table identifiers are fixed within this module, never interpolated CLI input.
    row = conn.execute(
        f"SELECT COUNT(*) FROM ("
        f"SELECT area_code, reference_date, COUNT(*) AS n FROM {table} "
        "GROUP BY area_code, reference_date "
        "HAVING COUNT(*) > 1 OR area_code IS NULL OR reference_date IS NULL)"
    ).fetchone()
    if row is None or _row_count(row[0], "duplicate grain") != 0:
        raise ComparisonError(f"{table} has duplicate or null Silver observation keys")


def _comparison_table(conn: duckdb.DuckDBPyConnection) -> None:
    differences = " OR ".join(f"b.{field} IS DISTINCT FROM c.{field}" for field in _CONTENT_COLUMNS)
    columns = ", ".join(
        f"b.{field} AS baseline_{field}, c.{field} AS candidate_{field}"
        for field in _CONTENT_COLUMNS
    )
    # The FULL OUTER JOIN detects deletions and new keys; null-safe comparisons
    # detect suppression transitions without treating NULL = NULL as a revision.
    conn.execute(
        "CREATE TEMP TABLE comparison AS SELECT "
        "COALESCE(b.area_code, c.area_code) AS area_code, "
        "COALESCE(b.reference_date, c.reference_date) AS reference_date, "
        "CASE WHEN b.area_code IS NULL THEN 'added' "
        "WHEN c.area_code IS NULL THEN 'removed' "
        f"WHEN ({differences}) THEN 'revised' ELSE 'unchanged' END AS change_type, "
        f"{columns} FROM baseline b FULL OUTER JOIN candidate c "
        "USING (area_code, reference_date)"
    )


def _summary(conn: duckdb.DuckDBPyConnection) -> tuple[object, ...]:
    row = conn.execute(
        "SELECT "
        "COUNT(*) FILTER (WHERE change_type = 'added'), "
        "COUNT(*) FILTER (WHERE change_type = 'removed'), "
        "COUNT(*) FILTER (WHERE change_type = 'revised'), "
        "COUNT(*) FILTER (WHERE change_type = 'unchanged'), "
        "COUNT(*) FILTER (WHERE change_type IN ('unchanged', 'revised') "
        "AND baseline_weekly_pay_gbp IS NOT NULL "
        "AND candidate_weekly_pay_gbp IS NULL), "
        "COUNT(*) FILTER (WHERE change_type IN ('unchanged', 'revised') "
        "AND baseline_weekly_pay_gbp IS NULL "
        "AND candidate_weekly_pay_gbp IS NOT NULL), "
        "COUNT(*) FILTER (WHERE change_type IN ('unchanged', 'revised') "
        "AND baseline_weekly_pay_gbp IS NOT NULL "
        "AND candidate_weekly_pay_gbp IS NOT NULL "
        "AND baseline_weekly_pay_gbp IS DISTINCT FROM candidate_weekly_pay_gbp), "
        "MAX(ABS(candidate_weekly_pay_gbp - baseline_weekly_pay_gbp)) "
        "FILTER (WHERE change_type IN ('unchanged', 'revised') "
        "AND baseline_weekly_pay_gbp IS NOT NULL "
        "AND candidate_weekly_pay_gbp IS NOT NULL) "
        "FROM comparison"
    ).fetchone()
    if row is None or len(row) != 8:
        raise ComparisonError("could not summarize the Silver release comparison")
    return row


def _field_counts(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    query = ", ".join(
        "COUNT(*) FILTER (WHERE change_type IN ('unchanged', 'revised') "
        f"AND baseline_{field} IS DISTINCT FROM candidate_{field})"
        for field in _CONTENT_COLUMNS
    )
    row = conn.execute(f"SELECT {query} FROM comparison").fetchone()
    if row is None or len(row) != len(_CONTENT_COLUMNS):
        raise ComparisonError("could not summarize changed Silver fields")
    return {
        field: _row_count(value, f"{field} changes")
        for field, value in zip(_CONTENT_COLUMNS, row, strict=True)
    }


def _scalar(value: object) -> JsonScalar:
    if value is None or type(value) in {str, int}:
        return cast(JsonScalar, value)
    if type(value) is float and math.isfinite(value):
        return value
    raise ComparisonError("curated comparison contains an invalid Silver field value")


def _examples(
    conn: duckdb.DuckDBPyConnection, change: ChangeType, limit: int
) -> tuple[ChangeExample, ...]:
    fields = ", ".join(f"baseline_{field}, candidate_{field}" for field in _CONTENT_COLUMNS)
    cursor = conn.execute(
        "SELECT area_code, reference_date, "
        f"{fields} FROM comparison WHERE change_type = ? "
        "ORDER BY area_code, reference_date LIMIT ?",
        [change, limit],
    )
    names = [str(column[0]) for column in cursor.description]
    results: list[ChangeExample] = []
    for values in cursor.fetchall():
        row = dict(zip(names, values, strict=True))
        area_code = row["area_code"]
        reference_date = row["reference_date"]
        if not isinstance(area_code, str) or not isinstance(reference_date, date):
            raise ComparisonError("invalid Silver release comparison key")
        baseline = (
            None
            if change == "added"
            else {field: _scalar(row[f"baseline_{field}"]) for field in _CONTENT_COLUMNS}
        )
        candidate = (
            None
            if change == "removed"
            else {field: _scalar(row[f"candidate_{field}"]) for field in _CONTENT_COLUMNS}
        )
        changed_fields = (
            tuple(field for field in _CONTENT_COLUMNS if baseline[field] != candidate[field])
            if baseline is not None and candidate is not None
            else ()
        )
        results.append(
            ChangeExample(
                area_code=area_code,
                reference_date=reference_date.isoformat(),
                change_type=change,
                changed_fields=changed_fields,
                baseline=baseline,
                candidate=candidate,
            )
        )
    return tuple(results)


def compare_ons_pay_vintages(
    *,
    data_root: Path,
    baseline: VintageKey,
    candidate: VintageKey,
    max_added_rows: int | None = None,
    max_removed_rows: int | None = None,
    max_revised_rows: int | None = None,
    max_newly_missing_pay_rows: int | None = None,
    max_missing_pay_increase_pp: float | None = None,
    max_absolute_pay_revision_gbp: float | None = None,
    examples_per_change: int = 5,
) -> ComparisonReport:
    """Compare two vetted Silver snapshots and evaluate explicit release gates.

    No latest/default source is discovered, no outputs are written and no
    candidate is promoted. Both Gold/Silver table contracts must first pass.
    """
    _validate_key(baseline, "baseline")
    _validate_key(candidate, "candidate")
    for name, value in (
        ("max_added_rows", max_added_rows),
        ("max_removed_rows", max_removed_rows),
        ("max_revised_rows", max_revised_rows),
        ("max_newly_missing_pay_rows", max_newly_missing_pay_rows),
    ):
        if value is not None and type(value) is not int:
            raise ComparisonError(f"{name} must be an integer")
        _validate_max(value, name)
    _validate_max(max_missing_pay_increase_pp, "max_missing_pay_increase_pp", max_value=100.0)
    _validate_max(max_absolute_pay_revision_gbp, "max_absolute_pay_revision_gbp")
    if type(examples_per_change) is not int or not 0 <= examples_per_change <= 100:
        raise ComparisonError("examples_per_change must be an integer between 0 and 100")

    catalog = CuratedCatalog(data_root)
    try:
        vintages = {vintage.key: vintage for vintage in catalog.list_vintages()}
        if baseline not in vintages or candidate not in vintages:
            raise ComparisonError("both baseline and candidate must be curated exact pins")
        before = vintages[baseline]
        after = vintages[candidate]
        CuratedCatalog.verify_vintage_tables(before)
        if baseline != candidate:
            CuratedCatalog.verify_vintage_tables(after)
    except CatalogError as exc:
        raise ComparisonError("published Silver/Gold integrity or table contract failed") from exc

    try:
        conn = duckdb.connect(":memory:")
    except duckdb.Error as exc:
        raise ComparisonError("cannot initialize an in-memory comparison") from exc
    try:
        for table, vintage in (("baseline", before), ("candidate", after)):
            conn.execute(
                f"CREATE TEMP TABLE {table} AS "
                "SELECT * EXCLUDE (source_sha256, registry_commit) FROM read_parquet(?)",
                [str(vintage.artifacts["observations"].path)],
            )
            _check_unique_grain(conn, table)
        _comparison_table(conn)
        summary = _summary(conn)
        added, removed, revised, unchanged, newly_missing, newly_reported, numeric_revisions = (
            _row_count(value, name)
            for value, name in zip(
                summary[:7],
                (
                    "added rows",
                    "removed rows",
                    "revised rows",
                    "unchanged rows",
                    "newly missing pay rows",
                    "newly reported pay rows",
                    "numeric pay revisions",
                ),
                strict=True,
            )
        )
        if (
            removed + revised + unchanged != before.source_rows
            or added + revised + unchanged != after.source_rows
        ):
            raise ComparisonError("comparison grain and curated row counts disagree")
        maximum = summary[7]
        if maximum is not None and (type(maximum) is not float or not math.isfinite(maximum)):
            raise ComparisonError("invalid maximum numeric pay change")
        baseline_missing = 100.0 * before.rows_without_pay / before.source_rows
        candidate_missing = 100.0 * after.rows_without_pay / after.source_rows
        counts = ComparisonCounts(
            baseline_rows=before.source_rows,
            candidate_rows=after.source_rows,
            added=added,
            removed=removed,
            revised=revised,
            unchanged=unchanged,
            newly_missing_pay=newly_missing,
            newly_reported_pay=newly_reported,
            numeric_pay_revisions=numeric_revisions,
            max_absolute_pay_revision_gbp=maximum,
            baseline_missing_pay_percent=baseline_missing,
            candidate_missing_pay_percent=candidate_missing,
            missing_pay_increase_pp=candidate_missing - baseline_missing,
        )
        field_counts = _field_counts(conn)
        if any(value > revised for value in field_counts.values()) or (
            sum(field_counts.values()) < revised
        ):
            raise ComparisonError("changed-field and revised-row counts disagree")
        examples = {
            change: _examples(conn, change, examples_per_change)
            for change in ("added", "removed", "revised")
        }
    except duckdb.Error as exc:
        raise ComparisonError("cannot query the vetted Silver release comparison") from exc
    finally:
        conn.close()

    policy = (
        _policy("added_rows", counts.added, max_added_rows),
        _policy("removed_rows", counts.removed, max_removed_rows),
        _policy("revised_rows", counts.revised, max_revised_rows),
        _policy("newly_missing_pay_rows", counts.newly_missing_pay, max_newly_missing_pay_rows),
        _policy(
            "missing_pay_increase_pp", counts.missing_pay_increase_pp, max_missing_pay_increase_pp
        ),
        _policy(
            "absolute_pay_revision_gbp",
            counts.max_absolute_pay_revision_gbp,
            max_absolute_pay_revision_gbp,
        ),
    )
    return ComparisonReport(
        baseline=baseline,
        candidate=candidate,
        status="breach" if any(check.state == "breach" for check in policy) else "pass",
        counts=counts,
        changed_fields=field_counts,
        policy_checks=policy,
        examples=examples,
    )
