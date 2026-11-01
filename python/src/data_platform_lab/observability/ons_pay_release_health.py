"""Read-only active-release health with conservative rollback advice.

Only a release-specific missing-pay-rate breach can produce a rollback
recommendation. A previous activated pin must also pass every configured
monitor check and the release-store rollback preflight. Pipeline-level
failures, corrupted data, or unavailable evidence require investigation.
This module never activates, rolls back, repairs, or exports anything itself.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_platform_lab.observability.ons_pay_monitor import (
    MonitorReport,
    format_prometheus,
    monitor_ons_pay,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseError, ReleaseStore

Advice = Literal["keep", "investigate", "review_rollback"]
CandidateStatus = Literal["healthy", "unhealthy", "unverifiable"]


class ReleaseHealthError(ValueError):
    """No active release or a concurrent active-pointer change."""


@dataclass(frozen=True, slots=True)
class ReviewedCandidate:
    """One prior active generation evaluated as a possible manual rollback."""

    generation: int
    vintage: VintageKey
    status: CandidateStatus
    failed_checks: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RollbackAdvice:
    """Decision support, never an instruction that performs a write."""

    action: Advice
    reason: str
    to_generation: int | None
    target: VintageKey | None
    reviewed_candidates: tuple[ReviewedCandidate, ...]


@dataclass(frozen=True, slots=True)
class ReleaseHealthReport:
    """Pinned, generation-bound health and human-reviewable rollback advice."""

    generation: int
    event_sha256: str
    vintage: VintageKey
    active_monitor: MonitorReport
    failed_checks: tuple[str, ...]
    advice: RollbackAdvice

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible evidence without losing the exact pins."""
        return asdict(self)


def _failed(report: MonitorReport) -> tuple[str, ...]:
    return tuple(check.name for check in report.checks if check.state == "fail")


def assess_ons_pay_release(
    *,
    data_root: Path,
    max_source_age_days: int | None = None,
    max_refresh_age_hours: float | None = None,
    max_missing_pay_percent: float | None = None,
    now: datetime | None = None,
) -> ReleaseHealthReport:
    """Assess a manually activated pin and suggest only a verified rollback.

    A change to the active event during evaluation invalidates the report.
    The result remains advisory: a later operator rollback must still satisfy
    its own expected-generation check and curated-data validation.
    """
    store = ReleaseStore(data_root)
    state = store.history()
    if state is None:
        raise ReleaseHealthError("no active release; bootstrap a pinned vintage first")

    checked_at = datetime.now(UTC) if now is None else now
    active = monitor_ons_pay(
        data_root=data_root,
        key=state.active,
        max_source_age_days=max_source_age_days,
        max_refresh_age_hours=max_refresh_age_hours,
        max_missing_pay_percent=max_missing_pay_percent,
        now=checked_at,
    )
    failures = _failed(active)
    examined: list[ReviewedCandidate] = []
    if not failures:
        advice = RollbackAdvice(
            "keep", "Active release passes every configured check.", None, None, ()
        )
    elif set(failures) != {"missing_pay_rate"}:
        advice = RollbackAdvice(
            "investigate",
            "Failures include integrity, freshness, or shared refresh operations; "
            "a rollback is not demonstrated to repair them.",
            None,
            None,
            (),
        )
    else:
        seen: set[VintageKey] = {state.active}
        advice = RollbackAdvice(
            "investigate",
            "The active missing-pay SLA is breached, but no earlier activated "
            "vintage passed every monitoring and rollback preflight check.",
            None,
            None,
            (),
        )
        # Prefer the most recently active distinct pin, preserving exact
        # generation provenance even if the history contains prior rollbacks.
        for generation in range(state.generation - 1, 0, -1):
            target = state.events[generation - 1].to_vintage
            if target in seen:
                continue
            seen.add(target)
            try:
                plan = store.plan_rollback(
                    to_generation=generation, expected_generation=state.generation
                )
            except ReleaseError:
                examined.append(ReviewedCandidate(generation, target, "unverifiable", ()))
                continue
            if not plan.eligible or plan.candidate != target:
                examined.append(ReviewedCandidate(generation, target, "unverifiable", ()))
                continue
            candidate_monitor = monitor_ons_pay(
                data_root=data_root,
                key=target,
                max_source_age_days=max_source_age_days,
                max_refresh_age_hours=max_refresh_age_hours,
                max_missing_pay_percent=max_missing_pay_percent,
                now=checked_at,
            )
            candidate_failures = _failed(candidate_monitor)
            examined.append(
                ReviewedCandidate(
                    generation,
                    target,
                    "unhealthy" if candidate_failures else "healthy",
                    candidate_failures,
                )
            )
            if not candidate_failures:
                advice = RollbackAdvice(
                    "review_rollback",
                    "An earlier activated pin passes the same health checks and "
                    "the read-only rollback preflight. Review before any manual apply.",
                    generation,
                    target,
                    (),
                )
                break
        advice = RollbackAdvice(
            advice.action,
            advice.reason,
            advice.to_generation,
            advice.target,
            tuple(examined),
        )

    # The pointer is atomic but readers do not take the writer lock. Bind the
    # advice to a stable generation and event hash, otherwise ask for a retry.
    current = store.history()
    if (
        current is None
        or current.generation != state.generation
        or current.event_sha256 != state.event_sha256
        or current.active != state.active
    ):
        raise ReleaseHealthError("active release changed during assessment; retry")

    return ReleaseHealthReport(
        generation=state.generation,
        event_sha256=state.event_sha256,
        vintage=state.active,
        active_monitor=active,
        failed_checks=failures,
        advice=advice,
    )


def format_release_prometheus(report: ReleaseHealthReport) -> str:
    """Append low-complexity release gauges to the existing pinned monitor data."""
    label = (
        f'model_version="{report.vintage.model_version}",'
        f'registry_commit="{report.vintage.registry_commit}",'
        f'source_sha256="{report.vintage.source_sha256}"'
    )
    lines = [format_prometheus(report.active_monitor).rstrip("\n")]
    for name, help_text, value in (
        (
            "release_generation",
            "Generation of the locally and explicitly activated ONS pay vintage.",
            report.generation,
        ),
        (
            "release_rollback_recommended",
            "A verified earlier pin merits manual rollback review (1=yes).",
            int(report.advice.action == "review_rollback"),
        ),
        (
            "release_rollback_candidates_reviewed",
            "Distinct earlier activated pins examined for rollback.",
            len(report.advice.reviewed_candidates),
        ),
    ):
        metric = f"dpl_ons_pay_{name}"
        lines.extend(
            [
                f"# HELP {metric} {help_text}",
                f"# TYPE {metric} gauge",
                f"{metric}{{{label}}} {value}",
            ]
        )
    if report.advice.to_generation is not None:
        metric = "dpl_ons_pay_release_rollback_target_generation"
        lines.extend(
            [
                f"# HELP {metric} Prior generation eligible for manual rollback review.",
                f"# TYPE {metric} gauge",
                f"{metric}{{{label}}} {report.advice.to_generation}",
            ]
        )
    return "\n".join(lines) + "\n"
