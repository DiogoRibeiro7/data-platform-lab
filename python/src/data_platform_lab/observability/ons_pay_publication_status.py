"""Read-only outcome and freshness checks over exact-target publication history."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_platform_lab.serving.publish_evidence import (
    PublicationArchive,
    PublicationEvidenceError,
    validate_publication_selection,
)


class PublicationStatusError(ValueError):
    """Invalid selection, freshness threshold or reference time."""


@dataclass(frozen=True, slots=True)
class AttemptStatus:
    """A historical outcome reference, without receipt payloads or row data."""

    attempt_id: str
    record_sha256: str
    outcome: str
    ended_at_utc: str
    age_hours: float
    failure_code: str | None


@dataclass(frozen=True, slots=True)
class PublicationStatusReport:
    """Archive health only; an OK result never claims a fresh sink observation."""

    checked_at_utc: str
    status: Literal["ok", "alert", "unavailable"]
    reason: str
    sink: str
    table: str
    vintage: dict[str, Any]
    target_id: str | None
    max_age_hours: float | None
    counts: dict[str, int]
    latest_attempt_count: int
    latest_attempt: AttemptStatus | None

    def to_dict(self) -> dict[str, Any]:
        """Return a defensive, versioned JSON-compatible monitoring report."""
        return {"schema_version": 1, "operation": "publication_status", **asdict(self)}


def publication_status(
    *,
    data_root: Path,
    sink: str,
    table: str,
    vintage: object,
    target_id: str | None = None,
    max_age_hours: float | None = None,
    checked_at: datetime | None = None,
) -> PublicationStatusReport:
    """Check the latest completed attempt; older acceptance cannot hide newer failure.

    Validate every archive record before filtering. No source artifact, database,
    release pointer, lock or filesystem write is needed. Equal latest timestamps
    are ambiguous, and future-dated outcomes cannot establish current health.
    """
    try:
        selected = validate_publication_selection(
            sink=sink, table=table, vintage=vintage, target_id=target_id
        )
    except PublicationEvidenceError:
        raise PublicationStatusError("Invalid publication selection.") from None
    if max_age_hours is not None:
        try:
            if type(max_age_hours) not in (int, float):
                raise ValueError
            max_age_hours = float(max_age_hours)
            if not math.isfinite(max_age_hours) or max_age_hours <= 0:
                raise ValueError
        except (ValueError, TypeError, OverflowError):
            raise PublicationStatusError("Maximum age must be positive finite hours.") from None
    now = datetime.now(UTC) if checked_at is None else checked_at
    try:
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError
        now = now.astimezone(UTC)
    except (ValueError, OverflowError):
        raise PublicationStatusError("Reference time must be timezone-aware.") from None

    history = PublicationArchive(data_root).matching_history(
        sink=sink, table=table, vintage=selected, target_id=target_id
    )
    counts = {outcome: 0 for outcome in ("accepted", "rejected", "failed")}
    latest_time = None
    latest_count = 0
    attempt = None
    for evidence in history:
        record = evidence.to_dict()
        counts[record["outcome"]] += 1
        ended = datetime.fromisoformat(record["ended_at"])
        if latest_time is None or ended > latest_time:
            latest_time, latest_count = ended, 1
            attempt = AttemptStatus(
                record["run_id"],
                evidence.record_sha256,
                record["outcome"],
                record["ended_at"],
                (now - ended).total_seconds() / 3600,
                record["failure_code"],
            )
        elif ended == latest_time:
            latest_count += 1
            attempt = None
    status: Literal["ok", "alert", "unavailable"] = "unavailable"
    if latest_time is None:
        reason = "no_attempts"
    elif latest_time > now:
        reason = "future_timestamp"
    elif attempt is None:
        reason = "ambiguous_latest_attempt"
    elif attempt.outcome != "accepted":
        status, reason = "alert", f"latest_attempt_{attempt.outcome}"
    elif max_age_hours is not None and attempt.age_hours > max_age_hours:
        status, reason = "alert", "stale"
    else:
        status, reason = "ok", "latest_attempt_accepted"
    return PublicationStatusReport(
        now.isoformat(),
        status,
        reason,
        sink,
        table,
        selected,
        target_id,
        max_age_hours,
        counts,
        latest_count,
        attempt,
    )
