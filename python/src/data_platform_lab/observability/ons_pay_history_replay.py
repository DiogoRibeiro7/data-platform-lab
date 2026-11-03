"""Deterministic, read-only replay of alert policies at archived observations.

Every checkpoint is evaluated using a verified prefix, never a later record.
Replaying an assessment is *not* reconstructing missing clock ticks, external
notifications, historical database state, or counterfactual health outcomes.
"""

from __future__ import annotations

import csv
import io
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from data_platform_lab.observability.ons_pay_alert_policy import AlertPolicy
from data_platform_lab.observability.ons_pay_history_alerts import (
    AlertStatus,
    HistoryAlertCheck,
    HistoryAlertError,
    _evaluate_window,
)
from data_platform_lab.observability.ons_pay_release_history import (
    HealthSample,
    HealthStatus,
    ReleaseHealthArchive,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseStore

_CHECK_NAMES = (
    "active_assessment",
    "assessment_age_hours",
    "consecutive_unhealthy",
    "unhealthy_share_percent",
    "missing_pay_increase_pp",
    "cumulative_missing_pay_drift_pp",
)


@dataclass(frozen=True, slots=True)
class ReplayPoint:
    """One archived assessment and the alert state it would have produced."""

    record_sha256: str
    checked_at_utc: str
    generation: int
    event_sha256: str
    vintage: VintageKey
    recorded_health_status: HealthStatus
    alert_status: AlertStatus
    window_assessments: int
    checks: tuple[HistoryAlertCheck, ...]


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    """Observed checkpoint counts, not durations or a confusion matrix."""

    evaluated: int
    ok: int
    alert: int
    unavailable: int
    first_checked_at_utc: str | None
    last_checked_at_utc: str | None


@dataclass(frozen=True, slots=True)
class HistoryReplay:
    """Bounded, source-pinned observations plus one shared policy fingerprint."""

    total_records: int
    replay_last: int
    policy: AlertPolicy
    policy_sha256: str
    summary: ReplaySummary
    points: tuple[ReplayPoint, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return deterministic JSON-ready replay evidence."""
        return asdict(self)


def replay_ons_pay_history_alerts(
    *, data_root: Path, policy: AlertPolicy | None = None, replay_last: int = 50
) -> HistoryReplay:
    """Replay a policy at up to 1000 recorded health assessment checkpoints.

    All records are verified before selecting the output. Each checkpoint
    receives a bounded prefix from the *complete* archive, so selecting the
    last few points cannot accidentally erase their earlier policy context.
    """
    _validate_replay_last(replay_last)
    effective = policy if policy is not None else AlertPolicy()

    store = ReleaseStore(data_root)
    state = store.history()
    trends = ReleaseHealthArchive(data_root).trends(last=None)
    report = _replay_verified_samples(
        samples=trends.samples,
        total_records=trends.total_records,
        policy=effective,
        replay_last=replay_last,
    )

    # The historical archive verifies a stable pointer during its own read.
    # Re-check after replay, which may take longer than the archive scan.
    current = store.history()
    if (current is None) != (state is None) or (
        state is not None
        and current is not None
        and (current.generation != state.generation or current.event_sha256 != state.event_sha256)
    ):
        raise HistoryAlertError("active release changed during history replay; retry")
    return report


def _validate_replay_last(replay_last: int) -> None:
    """Reject invalid checkpoint limits before opening the archive."""
    if type(replay_last) is not int or not 1 <= replay_last <= 1000:
        raise HistoryAlertError("replay_last must be an integer from 1 to 1000")


def _replay_verified_samples(
    *, samples: tuple[HealthSample, ...], total_records: int, policy: AlertPolicy, replay_last: int
) -> HistoryReplay:
    """Replay a verified snapshot in memory; shared by replay and paired comparison.

    The caller must first verify the entire archive and later recheck the
    active release. This function has no filesystem reads or writes.
    """
    _validate_replay_last(replay_last)
    all_samples = samples
    start = max(0, len(all_samples) - replay_last)
    points: list[ReplayPoint] = []
    for index in range(start, len(all_samples)):
        observation = all_samples[index]
        # An observation made later must not influence an earlier replay.
        prefix = all_samples[max(0, index - policy.last + 1) : index + 1]
        replayed = _evaluate_window(
            policy=policy,
            samples=prefix,
            checked_at=datetime.fromisoformat(observation.checked_at_utc),
            generation=observation.generation,
            event_sha256=observation.event_sha256,
            vintage=observation.vintage,
        )
        points.append(
            ReplayPoint(
                record_sha256=observation.record_sha256,
                checked_at_utc=observation.checked_at_utc,
                generation=observation.generation,
                event_sha256=observation.event_sha256,
                vintage=observation.vintage,
                recorded_health_status=observation.status,
                alert_status=replayed.status,
                window_assessments=replayed.window_assessments,
                checks=replayed.checks,
            )
        )

    summary = ReplaySummary(
        evaluated=len(points),
        ok=sum(point.alert_status == "ok" for point in points),
        alert=sum(point.alert_status == "alert" for point in points),
        unavailable=sum(point.alert_status == "unavailable" for point in points),
        first_checked_at_utc=points[0].checked_at_utc if points else None,
        last_checked_at_utc=points[-1].checked_at_utc if points else None,
    )
    return HistoryReplay(
        total_records=total_records,
        replay_last=replay_last,
        policy=policy,
        policy_sha256=policy.sha256,
        summary=summary,
        points=tuple(points),
    )


def format_history_replay_csv(report: HistoryReplay) -> str:
    """Export fixed, flat columns for audit review or pandas/DuckDB analysis."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(
        (
            "policy_sha256",
            "record_sha256",
            "checked_at_utc",
            "generation",
            "event_sha256",
            "model_version",
            "registry_commit",
            "source_sha256",
            "recorded_health_status",
            "alert_status",
            "window_assessments",
            "alerting_policies",
            "unavailable_policies",
            *[f"{name}_state" for name in _CHECK_NAMES],
        )
    )
    for point in report.points:
        states = {check.name: check.state for check in point.checks}
        writer.writerow(
            (
                report.policy_sha256,
                point.record_sha256,
                point.checked_at_utc,
                point.generation,
                point.event_sha256,
                point.vintage.model_version,
                point.vintage.registry_commit,
                point.vintage.source_sha256,
                point.recorded_health_status,
                point.alert_status,
                point.window_assessments,
                ";".join(check.name for check in point.checks if check.state == "alert"),
                ";".join(check.name for check in point.checks if check.state == "unavailable"),
                *[states[name] for name in _CHECK_NAMES],
            )
        )
    return buffer.getvalue()
