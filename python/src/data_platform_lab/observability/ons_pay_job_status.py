"""Read completed batch outcomes for the exact bytes of a saved ONS job."""

from __future__ import annotations

import json
import math
import os
import re
import stat
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Literal

from data_platform_lab.curation.ons_pay import MODEL_VERSION
from data_platform_lab.orchestration.ons_pay_refresh import RemoteRefreshSource
from data_platform_lab.orchestration.ons_pay_refresh_job import (
    RefreshJob,
    _reject_constant,
    _unique_object,
    load_refresh_job,
)

_MAX_FILES = 10_000
_MAX_AUDIT_BYTES = 4 * 1024 * 1024
_MAX_HISTORY_BYTES = 64 * 1024 * 1024
_RAW_TOTALS = ("objects_published", "objects_reused", "bytes_verified")
_CURATED_TOTALS = (
    "parquet_published",
    "parquet_reused",
    "source_rows",
    "rows_with_pay",
    "rows_without_pay",
)


class JobStatusError(ValueError):
    """The requested status cannot be established from valid local evidence."""


@dataclass(frozen=True, slots=True)
class JobRun:
    """A completed matching run, excluding arbitrary exception text and source rows."""

    run_id: str
    receipt_path: str
    status: Literal["success", "failed"]
    started_at: str
    completed_at: str
    duration_seconds: float
    completed_sources: int
    total_sources: int
    failure_phase: str | None
    failure_source_index: int | None


@dataclass(frozen=True, slots=True)
class JobStatusReport:
    """Audit status only; stored data and current source integrity are not inspected."""

    job_sha256: str
    pipeline_name: str
    checked_at_utc: str
    status: Literal["success", "failed", "stale", "never_run"]
    matching_runs: int
    latest_run: JobRun | None
    latest_success: JobRun | None
    last_success_age_hours: float | None
    max_success_age_hours: float | None
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-compatible report used by the status CLI."""
        return asdict(self)


def _integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise JobStatusError("Invalid non-negative count in job audit.")
    return value


def _hex(value: object, length: int) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", value) is None:
        raise JobStatusError("Invalid identity in job audit.")
    return value


def _aware_utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise JobStatusError("Invalid timestamp in job audit.")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise JobStatusError("Job audit timestamp must carry a timezone.")
    return parsed.astimezone(UTC)


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise JobStatusError("Invalid object in job audit.")
    return value


def _audit_paths(directory: Path, pipeline: str) -> Iterator[Path]:
    # scandir surfaces access errors; glob can suppress them as an empty history.
    with os.scandir(directory) as entries:
        for entry in entries:
            if fnmatchcase(entry.name, f"{pipeline}_*.json"):
                yield Path(entry.path)


def _matching_audits(job: RefreshJob, pipeline: str) -> Iterator[tuple[Path, dict[str, object]]]:
    directory = job.output_root / "manifests"
    try:
        mode = directory.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISDIR(mode) or directory.resolve() != directory:
        raise JobStatusError("Unsafe job audit directory.")
    total_bytes = 0
    for index, path in enumerate(_audit_paths(directory, pipeline)):
        if index >= _MAX_FILES:
            raise JobStatusError("Job audit history exceeds the file limit.")
        if not path.is_file() or path.resolve(strict=True) != path:
            raise JobStatusError("Unsafe job audit file.")
        allowance = min(_MAX_AUDIT_BYTES, _MAX_HISTORY_BYTES - total_bytes)
        with path.open("rb") as stream:
            raw = stream.read(allowance + 1)
        if len(raw) > allowance:
            raise JobStatusError("Job audit history exceeds the byte limit.")
        total_bytes += len(raw)
        audit = _mapping(
            json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        )
        digest = audit.get("job_sha256")
        if "job_sha256" not in audit:
            # Direct invocations and older audits do not identify a saved job.
            continue
        if _hex(digest, 64) == job.sha256:
            yield path, audit


def _validate_run(
    path: Path, audit: dict[str, object], job: RefreshJob, pipeline: str, now: datetime
) -> JobRun:
    if (
        audit.get("pipeline_name") != pipeline
        or type(audit.get("schema_version")) is not int
        or audit["schema_version"] != 1
        or (job.load_only and "model_version" in audit)
        or (
            not job.load_only
            and (
                type(audit.get("model_version")) is not int
                or audit["model_version"] != MODEL_VERSION
            )
        )
    ):
        raise JobStatusError("Unknown job audit schema or pipeline.")
    run_id = _hex(audit.get("run_id"), 32)
    if path.name != f"{pipeline}_{run_id}.json":
        raise JobStatusError("Job audit file name and run identity disagree.")
    started, completed = _aware_utc(audit.get("started_at")), _aware_utc(audit.get("created_at"))
    duration = audit.get("duration_seconds")
    if (
        started > completed
        or completed > now
        or type(duration) not in {int, float}
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
        or abs(duration - (completed - started).total_seconds()) > 0.000001
    ):
        raise JobStatusError("Invalid job audit timing.")
    status = audit.get("status")
    if status not in {"success", "failed"}:
        raise JobStatusError("Invalid job audit outcome.")
    sources = audit.get("sources")
    if not isinstance(sources, list) or len(sources) > len(job.sources):
        raise JobStatusError("Invalid completed sources in job audit.")
    metrics = _mapping(audit.get("metrics"))
    totals = _RAW_TOTALS if job.load_only else _RAW_TOTALS + _CURATED_TOTALS
    sums = dict.fromkeys(totals, 0)
    max_age = 0
    seen: set[tuple[str, str]] = set()
    for index, value in enumerate(sources):
        source = _mapping(value)
        pin = (_hex(source.get("registry_commit"), 40), _hex(source.get("source_sha256"), 64))
        expected = job.sources[index]
        if pin in seen or (
            isinstance(expected, RemoteRefreshSource) and expected.fetch_options.commit != pin[0]
        ):
            raise JobStatusError("Job audit source identities disagree with the batch.")
        seen.add(pin)
        max_age = max(max_age, _integer(source.get("source_age_days")))
        for field in totals:
            sums[field] += _integer(source.get(field))
        if not job.load_only and (
            _integer(source.get("rows_with_pay")) + _integer(source.get("rows_without_pay"))
            != _integer(source.get("source_rows"))
        ):
            raise JobStatusError("Job audit row counts disagree.")
    if (
        _integer(metrics.get("vintages_completed")) != len(sources)
        or _integer(metrics.get("max_source_age_days_observed")) != max_age
        or any(_integer(metrics.get(field)) != count for field, count in sums.items())
        or (job.load_only and any(field in metrics for field in _CURATED_TOTALS))
    ):
        raise JobStatusError("Job audit aggregate counts disagree.")
    policy = _mapping(audit.get("policy"))
    max_source_age = policy.get("max_source_age_days")
    if max_source_age is not None:
        _integer(max_source_age)
    if "max_source_age_days" not in policy or max_source_age != job.max_source_age_days:
        raise JobStatusError("Job audit freshness policy differs from the saved job.")
    as_of = policy.get("as_of_utc_date")
    if not isinstance(as_of, str):
        raise JobStatusError("Invalid job audit reference date.")
    date.fromisoformat(as_of)
    failure_phase: str | None = None
    failure_index: int | None = None
    if status == "success":
        if audit.get("failure") is not None or len(sources) != len(job.sources):
            raise JobStatusError("Successful job audit does not cover the entire batch.")
    else:
        failure = _mapping(audit.get("failure"))
        phase = failure.get("phase")
        allowed = (
            {"preflight", "ingestion"} if job.load_only else {"preflight", "ingestion", "curation"}
        )
        if not isinstance(phase, str) or phase not in allowed:
            raise JobStatusError("Invalid job failure phase.")
        failure_phase = phase
        failed_index = failure.get("source_index")
        if failed_index is not None:
            failure_index = _integer(failed_index)
            if failure_index >= len(job.sources):
                raise JobStatusError("Invalid failed source index in job audit.")
        if (phase == "preflight" and sources) or (
            phase != "preflight" and failure_index != len(sources)
        ):
            raise JobStatusError("Job failure phase and progress disagree.")
    return JobRun(
        run_id=run_id,
        receipt_path=str(path),
        status="success" if status == "success" else "failed",
        started_at=started.isoformat(),
        completed_at=completed.isoformat(),
        duration_seconds=float(duration),
        completed_sources=len(sources),
        total_sources=len(job.sources),
        failure_phase=failure_phase,
        failure_source_index=failure_index,
    )


def inspect_ons_pay_job(
    *, job_path: Path, max_success_age_hours: float | None = None, now: datetime | None = None
) -> JobStatusReport:
    """Inspect completed runs matching the current job digest without executing it.

    A later failed run takes precedence over an earlier success. Freshness is
    measured from the last complete success, never from a failed attempt.
    No handoffs, object stores, curated files, source transports or locks are opened.
    """
    try:
        if max_success_age_hours is not None and (
            type(max_success_age_hours) not in {int, float}
            or not math.isfinite(max_success_age_hours)
            or max_success_age_hours < 0
        ):
            raise JobStatusError("max_success_age_hours must be a finite non-negative number.")
        checked = datetime.now(UTC) if now is None else now
        if (
            not isinstance(checked, datetime)
            or checked.tzinfo is None
            or checked.utcoffset() is None
        ):
            raise JobStatusError("Status reference time must be timezone-aware.")
        checked = checked.astimezone(UTC)
        job = load_refresh_job(job_path)
        pipeline = "ons_pay_load" if job.load_only else "ons_pay_refresh"
        runs = [
            _validate_run(
                path, audit, job, pipeline, checked if now is not None else datetime.now(UTC)
            )
            for path, audit in _matching_audits(job, pipeline)
        ]
        # A concurrent writer may complete while history is being read. Compare
        # each receipt with its read time and report age at inspection completion.
        if now is None:
            checked = datetime.now(UTC)
        runs.sort(key=lambda item: (item.completed_at, item.run_id))
        latest = runs[-1] if runs else None
        success = next((item for item in reversed(runs) if item.status == "success"), None)
        age = (
            (checked - _aware_utc(success.completed_at)).total_seconds() / 3600 if success else None
        )
        state: Literal["success", "failed", "stale", "never_run"] = "never_run"
        if latest is not None:
            state = latest.status
            if (
                state == "success"
                and max_success_age_hours is not None
                and age is not None
                and age > max_success_age_hours
            ):
                state = "stale"
        return JobStatusReport(
            job_sha256=job.sha256,
            pipeline_name=pipeline,
            checked_at_utc=checked.isoformat(),
            status=state,
            matching_runs=len(runs),
            latest_run=latest,
            latest_success=success,
            last_success_age_hours=age,
            max_success_age_hours=max_success_age_hours,
        )
    except JobStatusError:
        raise
    except (OSError, ValueError, TypeError, RuntimeError, OverflowError):
        raise JobStatusError(
            "Cannot read valid saved-job status; check configuration and audits."
        ) from None
