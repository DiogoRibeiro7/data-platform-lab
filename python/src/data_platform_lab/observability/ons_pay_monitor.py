"""Read-only operational health and alert metrics for one pinned ONS pay vintage.

The monitor inspects existing curation and refresh receipts, never schedules
work or mutates objects. JSON and Prometheus/OpenMetrics-style gauges make
failures actionable by cron, CI, Dagster or an external alerting service.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

from data_platform_lab.curation.ons_pay import MODEL_VERSION
from data_platform_lab.orchestration.ons_pay_refresh import RefreshError, parse_retrieved_date
from data_platform_lab.serving.catalog import (
    CatalogError,
    CuratedCatalog,
    VintageKey,
    VintageNotFound,
)

_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_RUN_RE = re.compile(r"^[0-9a-f]{32}$")

CheckState = Literal["pass", "fail", "skipped"]
BatchStatus = Literal["success", "failed"]


class MonitorError(ValueError):
    """Invalid monitoring configuration or an unreadable refresh-audit log."""


@dataclass(frozen=True, slots=True)
class MonitorCheck:
    name: str
    state: CheckState
    observed: str | float | int | None
    threshold: float | int | None
    detail: str


@dataclass(frozen=True, slots=True)
class RefreshAudit:
    run_id: str
    created_at: datetime
    status: BatchStatus
    completed_vintages: frozenset[VintageKey]


@dataclass(frozen=True, slots=True)
class MonitorReport:
    checked_at_utc: str
    vintage: VintageKey
    status: Literal["healthy", "unhealthy"]
    checks: tuple[MonitorCheck, ...]
    metrics: dict[str, int | float | None]
    latest_batch_run_id: str | None
    latest_vintage_success_run_id: str | None

    def to_dict(self) -> dict[str, Any]:
        """Stable JSON-compatible report for automation and human inspection."""
        return asdict(self)


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise MonitorError(f"invalid {name} in refresh audit")
    return cast(dict[str, object], value)


def _aware_utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise MonitorError("refresh audit timestamp must be ISO text")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorError("invalid refresh audit timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise MonitorError("refresh audit timestamp must carry a timezone")
    return timestamp.astimezone(UTC)


def _read_audits(data_root: Path, checked_at: datetime) -> tuple[RefreshAudit, ...]:
    manifests = data_root / "manifests"
    if manifests.is_symlink():
        raise MonitorError("symlinked refresh-audit directory")
    if not manifests.exists():
        return ()
    if manifests.resolve() != manifests or not manifests.is_dir():
        raise MonitorError("unsafe refresh-audit directory")

    audits: list[RefreshAudit] = []
    run_ids: set[str] = set()
    for path in sorted(manifests.glob("ons_pay_refresh_*.json")):
        try:
            if path.resolve(strict=True) != path or not path.is_file():
                raise MonitorError("unsafe or missing refresh audit")
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
            raise MonitorError("cannot read a valid refresh audit") from exc
        audit = _mapping(raw, "refresh audit")
        if (
            audit.get("pipeline_name") != "ons_pay_refresh"
            or type(audit.get("schema_version")) is not int
            or audit.get("schema_version") != 1
            or type(audit.get("model_version")) is not int
            or audit.get("model_version") != MODEL_VERSION
        ):
            raise MonitorError("unknown refresh audit schema or pipeline")
        run_id = audit.get("run_id")
        if not isinstance(run_id, str) or not _RUN_RE.fullmatch(run_id):
            raise MonitorError("invalid refresh audit run ID")
        if run_id in run_ids:
            raise MonitorError("duplicate refresh audit run ID")
        run_ids.add(run_id)
        created_at = _aware_utc(audit.get("created_at"))
        if created_at > checked_at:
            raise MonitorError("refresh audit creation timestamp is in the future")
        status = audit.get("status")
        if status not in {"success", "failed"}:
            raise MonitorError("invalid refresh audit status")
        failure = audit.get("failure")
        if (status == "success" and failure is not None) or (
            status == "failed" and not isinstance(failure, dict)
        ):
            raise MonitorError("refresh audit failure status contradicts its payload")
        sources = audit.get("sources")
        if not isinstance(sources, list):
            raise MonitorError("refresh audit sources must be a list")
        completed: set[VintageKey] = set()
        for value in sources:
            source = _mapping(value, "completed refresh source")
            commit = source.get("registry_commit")
            sha = source.get("source_sha256")
            if (
                not isinstance(commit, str)
                or not _COMMIT_RE.fullmatch(commit)
                or not isinstance(sha, str)
                or not _SHA_RE.fullmatch(sha)
            ):
                raise MonitorError("invalid completed source pin in refresh audit")
            key = VintageKey(MODEL_VERSION, commit, sha)
            if key in completed:
                raise MonitorError("duplicate completed vintage in refresh audit")
            completed.add(key)
        metrics = _mapping(audit.get("metrics"), "refresh metrics")
        for field in (
            "vintages_completed",
            "objects_published",
            "objects_reused",
            "bytes_verified",
            "parquet_published",
            "parquet_reused",
            "source_rows",
            "rows_with_pay",
            "rows_without_pay",
            "max_source_age_days_observed",
        ):
            value = metrics.get(field)
            if type(value) is not int or value < 0:
                raise MonitorError(f"invalid refresh audit metric: {field}")
        if (
            metrics["vintages_completed"] != len(completed)
            or cast(int, metrics["rows_with_pay"]) + cast(int, metrics["rows_without_pay"])
            != metrics["source_rows"]
            or (status == "success" and not completed)
        ):
            raise MonitorError("refresh audit row metrics or completed count disagree")
        audits.append(
            RefreshAudit(
                run_id=run_id,
                created_at=created_at,
                status=status,
                completed_vintages=frozenset(completed),
            )
        )
    return tuple(sorted(audits, key=lambda item: (item.created_at, item.run_id)))


def _threshold(value: int | float | None, name: str, *, maximum: float | None = None) -> None:
    if value is None:
        return
    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
        raise MonitorError(f"{name} must be a finite non-negative number")
    if maximum is not None and value > maximum:
        raise MonitorError(f"{name} cannot exceed {maximum}")


def monitor_ons_pay(
    *,
    data_root: Path,
    key: VintageKey,
    max_source_age_days: int | None = None,
    max_refresh_age_hours: float | None = None,
    max_missing_pay_percent: float | None = None,
    now: datetime | None = None,
) -> MonitorReport:
    """Evaluate immutable curated data, upstream freshness and audited runs.

    A failed last batch is an operational alert across the ONS pay pipeline;
    the selected vintage's successful refresh recency is measured separately.
    No default/latest vintage is selected on behalf of the caller.
    """
    if (
        key.model_version != MODEL_VERSION
        or not _COMMIT_RE.fullmatch(key.registry_commit)
        or not _SHA_RE.fullmatch(key.source_sha256)
    ):
        raise MonitorError("monitor requires the exact v1 commit and source SHA-256")
    if max_source_age_days is not None and type(max_source_age_days) is not int:
        raise MonitorError("max_source_age_days must be an integer")
    _threshold(max_source_age_days, "max_source_age_days")
    _threshold(max_refresh_age_hours, "max_refresh_age_hours")
    _threshold(max_missing_pay_percent, "max_missing_pay_percent", maximum=100.0)
    checked_at = datetime.now(UTC) if now is None else now
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise MonitorError("monitoring reference timestamp must be timezone-aware")
    checked_at = checked_at.astimezone(UTC)

    checks: list[MonitorCheck] = []
    metrics: dict[str, int | float | None] = {
        "curation_integrity": 0,
        "table_integrity": None,
        "source_age_days": None,
        "source_rows": None,
        "rows_without_pay": None,
        "missing_pay_percent": None,
        "geography_year_rows": None,
        "local_authority_yoy_rows": None,
        "refresh_audit_integrity": 0,
        "last_batch_success": None,
        "last_vintage_refresh_age_hours": None,
    }
    root = data_root.resolve()
    vintage = None
    try:
        vintage = CuratedCatalog(root).get_vintage(key)
    except VintageNotFound:
        checks.append(
            MonitorCheck("curation_integrity", "fail", 0, 1, "Pinned vintage not curated")
        )
    except CatalogError:
        checks.append(
            MonitorCheck("curation_integrity", "fail", 0, 1, "Curated receipt/hash check failed")
        )
    else:
        metrics["curation_integrity"] = 1
        metrics["source_rows"] = vintage.source_rows
        metrics["rows_without_pay"] = vintage.rows_without_pay
        metrics["geography_year_rows"] = vintage.artifacts["geography_year"].rows
        metrics["local_authority_yoy_rows"] = vintage.artifacts["local_authority_yoy"].rows
        checks.append(
            MonitorCheck("curation_integrity", "pass", 1, 1, "Pinned receipts/hashes match")
        )

    if vintage is None:
        checks.append(MonitorCheck("table_contract", "skipped", None, None, "No trusted Parquet"))
        checks.append(
            MonitorCheck(
                "source_freshness",
                "skipped",
                None,
                max_source_age_days,
                "No trusted source metadata",
            )
        )
        checks.append(
            MonitorCheck(
                "missing_pay_rate",
                "skipped",
                None,
                max_missing_pay_percent,
                "No trusted source row counts",
            )
        )
    else:
        try:
            CuratedCatalog.verify_vintage_tables(vintage)
        except CatalogError:
            metrics["table_integrity"] = 0
            checks.append(
                MonitorCheck(
                    "table_contract",
                    "fail",
                    0,
                    1,
                    "Silver/Gold row, schema, or lineage check failed",
                )
            )
        else:
            metrics["table_integrity"] = 1
            checks.append(
                MonitorCheck(
                    "table_contract",
                    "pass",
                    1,
                    1,
                    "Silver/Gold row, schema, and lineage checks passed",
                )
            )

        try:
            retrieved = parse_retrieved_date(vintage.source_retrieved_at)
            source_age = (checked_at.date() - retrieved).days
        except (RefreshError, ValueError):
            checks.append(
                MonitorCheck(
                    "source_freshness",
                    "fail",
                    None,
                    max_source_age_days,
                    "Invalid source retrieval date",
                )
            )
        else:
            metrics["source_age_days"] = source_age
            if source_age < 0:
                checks.append(
                    MonitorCheck(
                        "source_freshness",
                        "fail",
                        source_age,
                        max_source_age_days,
                        "Source retrieval date is in the future",
                    )
                )
            elif max_source_age_days is None:
                checks.append(
                    MonitorCheck(
                        "source_freshness",
                        "skipped",
                        source_age,
                        None,
                        "No source-age SLA configured",
                    )
                )
            else:
                healthy = source_age <= max_source_age_days
                checks.append(
                    MonitorCheck(
                        "source_freshness",
                        "pass" if healthy else "fail",
                        source_age,
                        max_source_age_days,
                        "Source retrieval age within SLA"
                        if healthy
                        else "Source retrieval age breached SLA",
                    )
                )

        missing = 100.0 * vintage.rows_without_pay / vintage.source_rows
        metrics["missing_pay_percent"] = missing
        if max_missing_pay_percent is None:
            checks.append(
                MonitorCheck(
                    "missing_pay_rate",
                    "skipped",
                    missing,
                    None,
                    "No suppression-rate SLA configured",
                )
            )
        else:
            healthy = missing <= max_missing_pay_percent
            checks.append(
                MonitorCheck(
                    "missing_pay_rate",
                    "pass" if healthy else "fail",
                    missing,
                    max_missing_pay_percent,
                    "Missing pay share within SLA" if healthy else "Missing pay share breached SLA",
                )
            )

    latest_batch: RefreshAudit | None = None
    latest_vintage_success: RefreshAudit | None = None
    try:
        audits = _read_audits(root, checked_at)
    except MonitorError:
        checks.append(
            MonitorCheck(
                "refresh_audit_integrity",
                "fail",
                0,
                1,
                "Refresh audit is malformed, unsafe, or future-dated",
            )
        )
    else:
        if not audits:
            checks.append(
                MonitorCheck(
                    "refresh_audit_integrity", "fail", 0, 1, "No audited ONS pay refresh runs found"
                )
            )
        else:
            metrics["refresh_audit_integrity"] = 1
            latest_batch = audits[-1]
            successes = [
                item
                for item in audits
                if item.status == "success" and key in item.completed_vintages
            ]
            latest_vintage_success = successes[-1] if successes else None
            checks.append(
                MonitorCheck(
                    "refresh_audit_integrity",
                    "pass",
                    len(audits),
                    None,
                    "Refresh audit log passed schema/identity checks",
                )
            )

    if latest_batch is None:
        checks.append(
            MonitorCheck("last_batch", "skipped", None, None, "No trusted latest refresh batch")
        )
    else:
        succeeded = latest_batch.status == "success"
        metrics["last_batch_success"] = int(succeeded)
        checks.append(
            MonitorCheck(
                "last_batch",
                "pass" if succeeded else "fail",
                int(succeeded),
                1,
                "Latest refresh batch succeeded"
                if succeeded
                else "Latest ONS pay refresh batch failed",
            )
        )

    if latest_vintage_success is None:
        checks.append(
            MonitorCheck(
                "vintage_refresh_success",
                "fail",
                0,
                1,
                "No successful refresh audit for requested vintage",
            )
        )
        checks.append(
            MonitorCheck(
                "refresh_recency",
                "fail" if max_refresh_age_hours is not None else "skipped",
                None,
                max_refresh_age_hours,
                "No successful pinned refresh for recency evaluation",
            )
        )
    else:
        checks.append(
            MonitorCheck(
                "vintage_refresh_success",
                "pass",
                1,
                1,
                "Selected vintage has a successful refresh audit",
            )
        )
        age_hours = (checked_at - latest_vintage_success.created_at).total_seconds() / 3600.0
        metrics["last_vintage_refresh_age_hours"] = age_hours
        if max_refresh_age_hours is None:
            checks.append(
                MonitorCheck(
                    "refresh_recency", "skipped", age_hours, None, "No refresh-age SLA configured"
                )
            )
        else:
            healthy = age_hours <= max_refresh_age_hours
            checks.append(
                MonitorCheck(
                    "refresh_recency",
                    "pass" if healthy else "fail",
                    age_hours,
                    max_refresh_age_hours,
                    "Selected vintage refreshed within SLA"
                    if healthy
                    else "Selected vintage refresh age breached SLA",
                )
            )

    status: Literal["healthy", "unhealthy"] = (
        "unhealthy" if any(check.state == "fail" for check in checks) else "healthy"
    )
    return MonitorReport(
        checked_at_utc=checked_at.isoformat(),
        vintage=key,
        status=status,
        checks=tuple(checks),
        metrics=metrics,
        latest_batch_run_id=latest_batch.run_id if latest_batch else None,
        latest_vintage_success_run_id=(
            latest_vintage_success.run_id if latest_vintage_success else None
        ),
    )


_METRIC_HELP = {
    "curation_integrity": "Pinned curation receipt and content hashes verified (1=yes).",
    "table_integrity": "All three curated table schemas, rows and lineage verified (1=yes).",
    "source_age_days": "Age of upstream source retrieval in UTC calendar days.",
    "source_rows": "Number of pinned source observations.",
    "rows_without_pay": "Number of source observations lacking numeric pay.",
    "missing_pay_percent": "Percentage of source observations without reported pay.",
    "geography_year_rows": "Number of Gold geography-year summary rows.",
    "local_authority_yoy_rows": "Number of Gold local-authority trend rows.",
    "refresh_audit_integrity": "Refresh audit log is present and valid (1=yes).",
    "last_batch_success": "Latest ONS pay refresh batch succeeded (1=yes).",
    "last_vintage_refresh_age_hours": "Hours since last successful refresh of this pinned vintage.",
}


def format_prometheus(report: MonitorReport) -> str:
    """Render stable gauge metrics for a single selected version/source pin.

    Missing observations are omitted, never replaced with misleading zeroes.
    No network destination or metrics server is created by this formatter.
    """
    label = (
        f'model_version="{report.vintage.model_version}",'
        f'registry_commit="{report.vintage.registry_commit}",'
        f'source_sha256="{report.vintage.source_sha256}"'
    )
    lines = [
        "# HELP dpl_ons_pay_monitor_healthy All configured health checks passed (1=yes).",
        "# TYPE dpl_ons_pay_monitor_healthy gauge",
        f"dpl_ons_pay_monitor_healthy{{{label}}} {int(report.status == 'healthy')}",
    ]
    for name, help_text in _METRIC_HELP.items():
        value = report.metrics[name]
        if value is None:
            continue
        metric_name = f"dpl_ons_pay_{name}"
        lines.extend(
            [
                f"# HELP {metric_name} {help_text}",
                f"# TYPE {metric_name} gauge",
                f"{metric_name}{{{label}}} {value}",
            ]
        )
    return "\n".join(lines) + "\n"
