"""Read-only alert policies over verified, generation-pinned health snapshots.

The archive owns data integrity and provenance. This module evaluates
explicit thresholds without writing alerts, selecting data vintages, or
performing any automatic release/rollback action. Unavailable evidence is
never treated as a successful quality check.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal

from data_platform_lab.observability.ons_pay_alert_policy import AlertPolicy, AlertPolicyError
from data_platform_lab.observability.ons_pay_release_history import (
    HealthSample,
    ReleaseHealthArchive,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseStore

PolicyState = Literal["pass", "alert", "unavailable", "skipped"]
AlertStatus = Literal["ok", "alert", "unavailable"]


class HistoryAlertError(ValueError):
    """Invalid alert configuration, reference time, or changing release pointer."""


@dataclass(frozen=True, slots=True)
class HistoryAlertCheck:
    """One configured check and the observed value, when measurable."""

    name: str
    state: PolicyState
    observed: int | float | None
    threshold: int | float | None
    detail: str


@dataclass(frozen=True, slots=True)
class HistoryAlertReport:
    """Snapshot of advisory signals bound to the current active release."""

    checked_at_utc: str
    status: AlertStatus
    active_generation: int | None
    active_event_sha256: str | None
    active_vintage: VintageKey | None
    window_assessments: int
    window_generations: int
    policy: AlertPolicy
    policy_sha256: str
    checks: tuple[HistoryAlertCheck, ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the explicit policy evidence for external alert consumers."""
        return asdict(self)


def _upper_check(
    name: str,
    observed: int | float | None,
    maximum: int | float | None,
    detail: str,
) -> HistoryAlertCheck:
    if maximum is None:
        return HistoryAlertCheck(name, "skipped", observed, None, "Policy not configured")
    if observed is None:
        return HistoryAlertCheck(name, "unavailable", None, maximum, detail)
    return HistoryAlertCheck(
        name,
        "alert" if observed > maximum else "pass",
        observed,
        maximum,
        "Observed value exceeds the configured maximum"
        if observed > maximum
        else "Observed value is within the configured maximum",
    )


def _missing_drift(
    current: HealthSample | None,
    generations: dict[int, HealthSample],
) -> tuple[float | None, float | None]:
    """Return one-step and cumulative *positive* percentage-point changes.

    Generation gaps or unavailable percentages invalidate both metrics. A
    rollback is an ordinary new generation, including when it reuses a pin.
    """
    if current is None or current.generation < 2:
        return None, None
    previous = generations.get(current.generation - 1)
    if (
        previous is None
        or previous.missing_pay_percent is None
        or current.missing_pay_percent is None
    ):
        return None, None
    step = round(current.missing_pay_percent - previous.missing_pay_percent, 6)

    selected = [generations[generation] for generation in sorted(generations)]
    if len(selected) < 2 or selected[-1].generation != current.generation:
        return step, None
    # Cumulative drift is only meaningful if every intermediate activation
    # has a recorded observation; skipping a generation hides revisions.
    if any(
        item.generation != selected[index - 1].generation + 1
        for index, item in enumerate(selected)
        if index > 0
    ) or any(item.missing_pay_percent is None for item in selected):
        return step, None
    cumulative = 0.0
    for before, after in pairwise(selected):
        before_missing = before.missing_pay_percent
        after_missing = after.missing_pay_percent
        if before_missing is None or after_missing is None:
            return step, None
        cumulative += max(0.0, after_missing - before_missing)
    return step, round(cumulative, 6)


def _evaluate_window(
    *,
    policy: AlertPolicy,
    samples: tuple[HealthSample, ...],
    checked_at: datetime,
    generation: int | None,
    event_sha256: str | None,
    vintage: VintageKey | None,
) -> HistoryAlertReport:
    """Evaluate one verified, bounded observation prefix without I/O.

    Both live alerts and historical replay call this common implementation;
    callers own archive verification and active-pointer consistency checks.
    """
    active_points = (
        [
            item
            for item in samples
            if generation is not None
            and item.generation == generation
            and item.event_sha256 == event_sha256
        ]
        if generation is not None
        else []
    )
    latest = active_points[-1] if active_points else None
    age_hours = (
        (checked_at - datetime.fromisoformat(latest.checked_at_utc)).total_seconds() / 3600.0
        if latest is not None
        else None
    )
    if latest is None:
        active_check = HistoryAlertCheck(
            "active_assessment",
            "alert",
            0,
            1,
            "No verified assessment belongs to the current active release generation",
        )
    elif age_hours is not None and age_hours < 0:
        active_check = HistoryAlertCheck(
            "active_assessment", "alert", 0, 1, "Latest assessment is future-dated"
        )
        latest = None
        age_hours = None
    else:
        active_check = HistoryAlertCheck(
            "active_assessment", "pass", 1, 1, "Latest assessment matches the active event"
        )

    current_samples = active_points if latest is not None else []
    unhealthy_streak: int | None = None
    unhealthy_share: float | None = None
    if current_samples:
        unhealthy_streak = 0
        for item in reversed(current_samples):
            if item.status == "healthy":
                break
            unhealthy_streak += 1
        unhealthy_share = (
            100.0
            * sum(item.status == "unhealthy" for item in current_samples)
            / len(current_samples)
        )

    by_generation: dict[int, HealthSample] = {}
    for sample in samples:
        by_generation[sample.generation] = sample
    missing_step, cumulative = _missing_drift(latest, by_generation)

    if policy.alert_after_unhealthy is None:
        streak_check = HistoryAlertCheck(
            "consecutive_unhealthy", "skipped", unhealthy_streak, None, "Policy not configured"
        )
    elif unhealthy_streak is None:
        streak_check = HistoryAlertCheck(
            "consecutive_unhealthy",
            "unavailable",
            None,
            policy.alert_after_unhealthy,
            "No current-generation assessments for streak evaluation",
        )
    else:
        streak_check = HistoryAlertCheck(
            "consecutive_unhealthy",
            "alert" if unhealthy_streak >= policy.alert_after_unhealthy else "pass",
            unhealthy_streak,
            policy.alert_after_unhealthy,
            "Alert when the trailing unhealthy count reaches the configured minimum",
        )

    checks = (
        active_check,
        _upper_check(
            "assessment_age_hours",
            age_hours,
            policy.max_assessment_age_hours,
            "No current-generation timestamp to evaluate freshness",
        ),
        streak_check,
        _upper_check(
            "unhealthy_share_percent",
            unhealthy_share,
            policy.max_unhealthy_share_percent,
            "No current-generation observations for sample-based unhealthy share",
        ),
        _upper_check(
            "missing_pay_increase_pp",
            missing_step,
            policy.max_missing_pay_increase_pp,
            "Adjacent release generations lack comparable missing-pay observations",
        ),
        _upper_check(
            "cumulative_missing_pay_drift_pp",
            cumulative,
            policy.max_cumulative_missing_pay_drift_pp,
            "The window lacks a complete, measurable sequence of release generations",
        ),
    )
    status: AlertStatus = (
        "alert"
        if any(check.state == "alert" for check in checks)
        else "unavailable"
        if any(check.state == "unavailable" for check in checks)
        else "ok"
    )

    return HistoryAlertReport(
        checked_at_utc=checked_at.isoformat(),
        status=status,
        active_generation=generation,
        active_event_sha256=event_sha256,
        active_vintage=vintage,
        window_assessments=len(samples),
        window_generations=len(by_generation),
        policy=policy,
        policy_sha256=policy.sha256,
        checks=checks,
    )


def evaluate_ons_pay_history_alerts(
    *,
    data_root: Path,
    policy: AlertPolicy | None = None,
    last: int | None = None,
    max_assessment_age_hours: float | None = None,
    alert_after_unhealthy: int | None = None,
    max_unhealthy_share_percent: float | None = None,
    max_missing_pay_increase_pp: float | None = None,
    max_cumulative_missing_pay_drift_pp: float | None = None,
    now: datetime | None = None,
) -> HistoryAlertReport:
    """Evaluate current active history with conservative missing-evidence semantics.

    A mandatory check rejects an unassessed active generation. Thresholds
    remain skipped when omitted; observations are not time-weighted.
    """
    try:
        effective_policy = (policy if policy is not None else AlertPolicy()).with_overrides(
            last=last,
            max_assessment_age_hours=max_assessment_age_hours,
            alert_after_unhealthy=alert_after_unhealthy,
            max_unhealthy_share_percent=max_unhealthy_share_percent,
            max_missing_pay_increase_pp=max_missing_pay_increase_pp,
            max_cumulative_missing_pay_drift_pp=max_cumulative_missing_pay_drift_pp,
        )
    except AlertPolicyError as exc:
        raise HistoryAlertError(str(exc)) from exc

    checked_at = datetime.now(UTC) if now is None else now
    if checked_at.utcoffset() is None:
        raise HistoryAlertError("alert reference time must be timezone-aware")
    checked_at = checked_at.astimezone(UTC)

    store = ReleaseStore(data_root)
    state = store.history()
    history = ReleaseHealthArchive(data_root).trends(last=effective_policy.last)
    report = _evaluate_window(
        policy=effective_policy,
        samples=history.samples,
        checked_at=checked_at,
        generation=state.generation if state is not None else None,
        event_sha256=state.event_sha256 if state is not None else None,
        vintage=state.active if state is not None else None,
    )

    # Archive reads and release promotions are independent. Invalidate a
    # report whose active generation changed during assessment.
    current = store.history()
    if (current is None) != (state is None) or (
        current is not None
        and state is not None
        and (current.generation != state.generation or current.event_sha256 != state.event_sha256)
    ):
        raise HistoryAlertError("active release changed during alert evaluation; retry")
    return report


def format_history_alerts_prometheus(report: HistoryAlertReport) -> str:
    """Render advisory gauges without any network sink or background process."""
    labels = (
        f'{{model_version="{report.active_vintage.model_version}",'
        f'registry_commit="{report.active_vintage.registry_commit}",'
        f'source_sha256="{report.active_vintage.source_sha256}"}}'
        if report.active_vintage is not None
        else ""
    )
    values: tuple[tuple[str, str, int | float | None], ...] = (
        (
            "history_alert",
            "One or more history policies emitted an alert (1=yes).",
            int(report.status == "alert"),
        ),
        (
            "history_unavailable",
            "History evidence is insufficient without a stronger alert (1=yes).",
            int(report.status == "unavailable"),
        ),
        (
            "history_checked_at_seconds",
            "UTC Unix timestamp of alert evaluation.",
            int(datetime.fromisoformat(report.checked_at_utc).timestamp()),
        ),
        (
            "history_active_generation",
            "Current manually activated release generation.",
            report.active_generation,
        ),
        (
            "history_assessments",
            "Validated assessments in the selected observation window.",
            report.window_assessments,
        ),
        (
            "history_checks_alerting",
            "Number of triggered history alert checks.",
            sum(check.state == "alert" for check in report.checks),
        ),
        (
            "history_checks_unavailable",
            "Number of unmeasurable configured history checks.",
            sum(check.state == "unavailable" for check in report.checks),
        ),
    )
    lines: list[str] = []
    for name, help_text, value in values:
        if value is None:
            continue
        metric = f"dpl_ons_pay_{name}"
        lines.extend(
            (f"# HELP {metric} {help_text}", f"# TYPE {metric} gauge", f"{metric}{labels} {value}")
        )
    info_metric = "dpl_ons_pay_history_policy_info"
    lines.extend(
        (
            f"# HELP {info_metric} Fingerprint of the effective alert policy.",
            f"# TYPE {info_metric} gauge",
            f'{info_metric}{{policy_name="{report.policy.name}",'
            f'policy_sha256="{report.policy_sha256}"}} 1',
        )
    )
    code = {"pass": 0, "alert": 1, "unavailable": 2, "skipped": 3}
    metric = "dpl_ons_pay_history_policy_state"
    lines.extend(
        (
            f"# HELP {metric} Policy state: 0 pass, 1 alert, 2 unavailable, 3 skipped.",
            f"# TYPE {metric} gauge",
        )
    )
    for check in report.checks:
        lines.append(f'{metric}{{policy="{check.name}"}} {code[check.state]}')
    return "\n".join(lines) + "\n"
