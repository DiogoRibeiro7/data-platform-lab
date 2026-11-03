"""Paired, read-only comparisons of versioned alerts on verified history.

Both policy configurations consume one validated archive snapshot. Results
describe changes in advisory decisions at recorded checkpoints, not alert
accuracy, unobserved clock ticks, or ground-truth incident outcomes.
"""

from __future__ import annotations

import csv
import io
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from data_platform_lab.observability.ons_pay_alert_policy import AlertPolicy
from data_platform_lab.observability.ons_pay_history_alerts import (
    AlertStatus,
    HistoryAlertCheck,
    HistoryAlertError,
)
from data_platform_lab.observability.ons_pay_history_replay import (
    _CHECK_NAMES,
    ReplayPoint,
    _replay_verified_samples,
    _validate_replay_last,
)
from data_platform_lab.observability.ons_pay_release_history import (
    HealthStatus,
    ReleaseHealthArchive,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseStore

_STATUSES: tuple[AlertStatus, ...] = ("ok", "alert", "unavailable")


@dataclass(frozen=True, slots=True)
class CheckDifference:
    """The exact check evidence that differs between two policy evaluations."""

    name: str
    baseline: HistoryAlertCheck
    candidate: HistoryAlertCheck


@dataclass(frozen=True, slots=True)
class ComparisonPoint:
    """A single immutable archive checkpoint observed by both policies."""

    record_sha256: str
    checked_at_utc: str
    generation: int
    event_sha256: str
    vintage: VintageKey
    recorded_health_status: HealthStatus
    baseline_status: AlertStatus
    candidate_status: AlertStatus
    baseline_window_assessments: int
    candidate_window_assessments: int
    baseline_checks: tuple[HistoryAlertCheck, ...]
    candidate_checks: tuple[HistoryAlertCheck, ...]
    check_differences: tuple[CheckDifference, ...]


@dataclass(frozen=True, slots=True)
class StatusTransition:
    """One cell of the fixed 3-by-3 baseline-to-candidate status matrix."""

    baseline: AlertStatus
    candidate: AlertStatus
    checkpoints: int


@dataclass(frozen=True, slots=True)
class ComparisonSummary:
    """Observed paired counts, not elapsed time or classification metrics."""

    evaluated: int
    status_unchanged: int
    status_changed: int
    check_evidence_changed: int
    alert_added: int
    alert_cleared: int
    alert_to_unavailable: int
    became_unavailable: int
    no_longer_unavailable: int
    first_checked_at_utc: str | None
    last_checked_at_utc: str | None


@dataclass(frozen=True, slots=True)
class PolicyComparison:
    """Fully paired policy decisions tied to both fingerprints and one archive."""

    total_records: int
    replay_last: int
    baseline_policy: AlertPolicy
    baseline_policy_sha256: str
    candidate_policy: AlertPolicy
    candidate_policy_sha256: str
    summary: ComparisonSummary
    transitions: tuple[StatusTransition, ...]
    points: tuple[ComparisonPoint, ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the immutable comparison for deterministic JSON output."""
        return asdict(self)


def _pair_points(baseline: ReplayPoint, candidate: ReplayPoint) -> ComparisonPoint:
    """Fail closed on unequal snapshots or evaluator check schemas."""
    baseline_pin = (
        baseline.record_sha256,
        baseline.checked_at_utc,
        baseline.generation,
        baseline.event_sha256,
        baseline.vintage,
        baseline.recorded_health_status,
    )
    candidate_pin = (
        candidate.record_sha256,
        candidate.checked_at_utc,
        candidate.generation,
        candidate.event_sha256,
        candidate.vintage,
        candidate.recorded_health_status,
    )
    if baseline_pin != candidate_pin:
        raise HistoryAlertError("policy comparison checkpoints are not aligned")
    baseline_checks = {check.name: check for check in baseline.checks}
    candidate_checks = {check.name: check for check in candidate.checks}
    if (
        len(baseline_checks) != len(_CHECK_NAMES)
        or set(baseline_checks) != set(_CHECK_NAMES)
        or len(candidate_checks) != len(_CHECK_NAMES)
        or set(candidate_checks) != set(_CHECK_NAMES)
    ):
        raise HistoryAlertError("policy comparison check schemas are not aligned")
    differences = tuple(
        CheckDifference(name, baseline_checks[name], candidate_checks[name])
        for name in _CHECK_NAMES
        if baseline_checks[name] != candidate_checks[name]
    )
    return ComparisonPoint(
        record_sha256=baseline.record_sha256,
        checked_at_utc=baseline.checked_at_utc,
        generation=baseline.generation,
        event_sha256=baseline.event_sha256,
        vintage=baseline.vintage,
        recorded_health_status=baseline.recorded_health_status,
        baseline_status=baseline.alert_status,
        candidate_status=candidate.alert_status,
        baseline_window_assessments=baseline.window_assessments,
        candidate_window_assessments=candidate.window_assessments,
        baseline_checks=baseline.checks,
        candidate_checks=candidate.checks,
        check_differences=differences,
    )


def compare_ons_pay_alert_policies(
    *, data_root: Path, baseline: AlertPolicy, candidate: AlertPolicy, replay_last: int = 50
) -> PolicyComparison:
    """Compare two policies at the same verified, recorded checkpoints.

    The archive is fully validated once, before output selection, and each
    policy uses only the preceding observations within its own `last` window.
    A later release-pointer change invalidates the entire comparison.
    """
    _validate_replay_last(replay_last)
    store = ReleaseStore(data_root)
    initial = store.history()
    trends = ReleaseHealthArchive(data_root).trends(last=None)
    baseline_replay = _replay_verified_samples(
        samples=trends.samples,
        total_records=trends.total_records,
        policy=baseline,
        replay_last=replay_last,
    )
    candidate_replay = _replay_verified_samples(
        samples=trends.samples,
        total_records=trends.total_records,
        policy=candidate,
        replay_last=replay_last,
    )

    # The same immutable snapshot and checkpoint indices must feed both arms.
    if len(baseline_replay.points) != len(candidate_replay.points):
        raise HistoryAlertError("policy comparison checkpoints are not aligned")
    points = tuple(
        _pair_points(left, right)
        for left, right in zip(baseline_replay.points, candidate_replay.points, strict=True)
    )

    current = store.history()
    if (current is None) != (initial is None) or (
        initial is not None
        and current is not None
        and (
            current.generation != initial.generation or current.event_sha256 != initial.event_sha256
        )
    ):
        raise HistoryAlertError("active release changed during policy comparison; retry")

    counts = Counter((point.baseline_status, point.candidate_status) for point in points)
    transitions = tuple(
        StatusTransition(before, after, counts[before, after])
        for before in _STATUSES
        for after in _STATUSES
    )
    summary = ComparisonSummary(
        evaluated=len(points),
        status_unchanged=sum(point.baseline_status == point.candidate_status for point in points),
        status_changed=sum(point.baseline_status != point.candidate_status for point in points),
        check_evidence_changed=sum(bool(point.check_differences) for point in points),
        alert_added=sum(
            point.baseline_status != "alert" and point.candidate_status == "alert"
            for point in points
        ),
        alert_cleared=counts["alert", "ok"],
        alert_to_unavailable=counts["alert", "unavailable"],
        became_unavailable=sum(
            point.baseline_status != "unavailable" and point.candidate_status == "unavailable"
            for point in points
        ),
        no_longer_unavailable=sum(
            point.baseline_status == "unavailable" and point.candidate_status != "unavailable"
            for point in points
        ),
        first_checked_at_utc=points[0].checked_at_utc if points else None,
        last_checked_at_utc=points[-1].checked_at_utc if points else None,
    )
    return PolicyComparison(
        total_records=trends.total_records,
        replay_last=replay_last,
        baseline_policy=baseline,
        baseline_policy_sha256=baseline.sha256,
        candidate_policy=candidate,
        candidate_policy_sha256=candidate.sha256,
        summary=summary,
        transitions=transitions,
        points=points,
    )


def format_alert_policy_comparison_csv(report: PolicyComparison) -> str:
    """Export one row per checkpoint with stable columns for both policies."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        (
            "baseline_policy_sha256",
            "candidate_policy_sha256",
            "record_sha256",
            "checked_at_utc",
            "generation",
            "event_sha256",
            "model_version",
            "registry_commit",
            "source_sha256",
            "recorded_health_status",
            "baseline_status",
            "candidate_status",
            "baseline_window_assessments",
            "candidate_window_assessments",
            "changed_check_names",
            *[f"baseline_{name}_state" for name in _CHECK_NAMES],
            *[f"candidate_{name}_state" for name in _CHECK_NAMES],
        )
    )
    for point in report.points:
        baseline_states = {check.name: check.state for check in point.baseline_checks}
        candidate_states = {check.name: check.state for check in point.candidate_checks}
        writer.writerow(
            (
                report.baseline_policy_sha256,
                report.candidate_policy_sha256,
                point.record_sha256,
                point.checked_at_utc,
                point.generation,
                point.event_sha256,
                point.vintage.model_version,
                point.vintage.registry_commit,
                point.vintage.source_sha256,
                point.recorded_health_status,
                point.baseline_status,
                point.candidate_status,
                point.baseline_window_assessments,
                point.candidate_window_assessments,
                ";".join(change.name for change in point.check_differences),
                *[baseline_states[name] for name in _CHECK_NAMES],
                *[candidate_states[name] for name in _CHECK_NAMES],
            )
        )
    return buffer.getvalue()
