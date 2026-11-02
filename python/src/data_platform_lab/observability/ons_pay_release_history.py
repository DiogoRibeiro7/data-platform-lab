"""Durable, opt-in history and read-only trends for active ONS pay health.

Assessments are canonical, content-addressed JSON documents. A new record is
linked atomically into an append-only directory while the existing release
writer lock prevents activation from racing the write. There is no mutable
index or implicit scheduler. History reads verify every stored record against
the SHA-linked release chain, including records from earlier generations.
"""

from __future__ import annotations

import csv
import hashlib
import io
import itertools
import json
import math
import os
import re
import tempfile
from collections import defaultdict
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from data_platform_lab.curation.ons_pay import MODEL_VERSION
from data_platform_lab.observability.ons_pay_release_health import (
    Advice,
    ReleaseHealthReport,
)
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.release_store import ReleaseState, ReleaseStore

_HASH = re.compile(r"^[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_RUN = re.compile(r"^[0-9a-f]{32}$")
_CHECK_NAMES = frozenset(
    {
        "curation_integrity",
        "table_contract",
        "source_freshness",
        "missing_pay_rate",
        "refresh_audit_integrity",
        "last_batch",
        "vintage_refresh_success",
        "refresh_recency",
    }
)
_METRICS = frozenset(
    {
        "curation_integrity",
        "table_integrity",
        "source_age_days",
        "source_rows",
        "rows_without_pay",
        "missing_pay_percent",
        "geography_year_rows",
        "local_authority_yoy_rows",
        "refresh_audit_integrity",
        "last_batch_success",
        "last_vintage_refresh_age_hours",
    }
)
_MAX_RECORD_BYTES = 2_000_000

HealthStatus = Literal["healthy", "unhealthy"]


class ReleaseHistoryError(ValueError):
    """An archive path, record, release reference, or trend parameter is unsafe."""


@dataclass(frozen=True, slots=True)
class HealthSample:
    """Validated time-series observation tied to an exact historical release."""

    record_sha256: str
    checked_at_utc: str
    generation: int
    event_sha256: str
    vintage: VintageKey
    status: HealthStatus
    failed_checks: tuple[str, ...]
    missing_pay_percent: float | None
    source_age_days: int | None
    refresh_age_hours: float | None
    advice: Advice
    rollback_to_generation: int | None


@dataclass(frozen=True, slots=True)
class GenerationTrend:
    """Within-generation assessment history and a simple missing-pay delta."""

    generation: int
    vintage: VintageKey
    assessments: int
    unhealthy_assessments: int
    first_checked_at_utc: str
    last_checked_at_utc: str
    latest_status: HealthStatus
    missing_pay_change_pp: float | None


@dataclass(frozen=True, slots=True)
class TrendSummary:
    """Aggregate counts over the filtered, bounded timeline."""

    assessments: int
    generations: int
    healthy_assessments: int
    unhealthy_assessments: int
    healthy_to_unhealthy: int
    unhealthy_to_healthy: int
    rollback_reviews: int
    first_checked_at_utc: str | None
    last_checked_at_utc: str | None
    missing_pay_change_pp: float | None


@dataclass(frozen=True, slots=True)
class HealthTrends:
    """Validated JSON/CSV-ready evidence, never an active-release mutation."""

    total_records: int
    summary: TrendSummary
    by_generation: tuple[GenerationTrend, ...]
    samples: tuple[HealthSample, ...]

    def to_dict(self) -> dict[str, Any]:
        """JSON-compatible report with deterministic timeline ordering."""
        return asdict(self)


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def _object(value: object, name: str, fields: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ReleaseHistoryError(f"invalid {name} fields in health history")
    if not all(isinstance(field, str) for field in value):
        raise ReleaseHistoryError(f"invalid {name} keys in health history")
    return cast(dict[str, object], value)


def _pin(value: object) -> VintageKey:
    fields = frozenset({"model_version", "registry_commit", "source_sha256"})
    raw = _object(value, "vintage", fields)
    version, commit, sha = raw["model_version"], raw["registry_commit"], raw["source_sha256"]
    if (
        type(version) is not int
        or version != MODEL_VERSION
        or not isinstance(commit, str)
        or _COMMIT.fullmatch(commit) is None
        or not isinstance(sha, str)
        or _HASH.fullmatch(sha) is None
    ):
        raise ReleaseHistoryError("health record has invalid exact version/commit/source pins")
    return VintageKey(version, commit, sha)


def _sha(value: object, name: str) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise ReleaseHistoryError(f"invalid {name} in health history")
    return value


def _generation(value: object, *, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ReleaseHistoryError("health record references an unknown release generation")
    return value


def _utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ReleaseHistoryError("health record timestamp must be ISO UTC text")
    try:
        moment = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ReleaseHistoryError("invalid health record timestamp") from exc
    offset = moment.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ReleaseHistoryError("health record timestamp must be UTC")
    return moment.astimezone(UTC)


def _number(value: object, name: str, *, integral: bool = False) -> int | float | None:
    if value is None:
        return None
    if type(value) not in ({int} if integral else {int, float}):
        raise ReleaseHistoryError(f"invalid {name} metric in health history")
    try:
        finite = math.isfinite(cast(int | float, value))
    except OverflowError as exc:
        raise ReleaseHistoryError(f"out-of-range {name} metric in health history") from exc
    if not finite:
        raise ReleaseHistoryError(f"non-finite {name} metric in health history")
    return cast(int | float, value)


def _event_hash(state: ReleaseState, generation: int) -> str:
    if generation == state.generation:
        return state.event_sha256
    linked = state.events[generation].previous_event_sha256
    if linked is None:
        raise ReleaseHistoryError("release chain lacks historical event hash")
    return linked


def _sample(envelope: object, sha: str, state: ReleaseState) -> HealthSample:
    wrapper = _object(
        envelope, "health envelope", frozenset({"schema_version", "pipeline_name", "report"})
    )
    if (
        type(wrapper["schema_version"]) is not int
        or wrapper["schema_version"] != 1
        or wrapper["pipeline_name"] != "ons_pay_release_health"
    ):
        raise ReleaseHistoryError("unknown health history record schema")
    report = _object(
        wrapper["report"],
        "health report",
        frozenset(
            {"generation", "event_sha256", "vintage", "active_monitor", "failed_checks", "advice"}
        ),
    )
    generation = _generation(report["generation"], maximum=state.generation)
    event_hash = _sha(report["event_sha256"], "release event hash")
    vintage = _pin(report["vintage"])
    if (
        event_hash != _event_hash(state, generation)
        or state.events[generation - 1].to_vintage != vintage
    ):
        raise ReleaseHistoryError("health record conflicts with SHA-linked release history")

    monitor = _object(
        report["active_monitor"],
        "pinned monitor report",
        frozenset(
            {
                "checked_at_utc",
                "vintage",
                "status",
                "checks",
                "metrics",
                "latest_batch_run_id",
                "latest_vintage_success_run_id",
            }
        ),
    )
    checked_at = _utc(monitor["checked_at_utc"])
    if checked_at < _utc(state.events[generation - 1].created_at_utc):
        raise ReleaseHistoryError("health assessment predates the pinned activation")
    if checked_at > datetime.now(UTC) + timedelta(minutes=5):
        raise ReleaseHistoryError("health assessment timestamp is in the future")
    if _pin(monitor["vintage"]) != vintage:
        raise ReleaseHistoryError("health monitor pin contradicts its release event")
    status = monitor["status"]
    if not isinstance(status, str) or status not in {"healthy", "unhealthy"}:
        raise ReleaseHistoryError("unknown health monitor status")
    for field in ("latest_batch_run_id", "latest_vintage_success_run_id"):
        run_id = monitor[field]
        if run_id is not None and (not isinstance(run_id, str) or _RUN.fullmatch(run_id) is None):
            raise ReleaseHistoryError("invalid recorded refresh run ID")

    checks = monitor["checks"]
    if not isinstance(checks, list) or len(checks) != len(_CHECK_NAMES):
        raise ReleaseHistoryError("health record lacks complete monitor checks")
    seen: set[str] = set()
    failures: list[str] = []
    for item in checks:
        check = _object(
            item,
            "monitor check",
            frozenset({"name", "state", "observed", "threshold", "detail"}),
        )
        name, state_value = check["name"], check["state"]
        if (
            not isinstance(name, str)
            or name not in _CHECK_NAMES
            or name in seen
            or not isinstance(state_value, str)
            or state_value not in {"pass", "fail", "skipped"}
            or not isinstance(check["detail"], str)
        ):
            raise ReleaseHistoryError("invalid or repeated health monitor check")
        seen.add(name)
        if state_value == "fail":
            failures.append(name)
        observed = check["observed"]
        if observed is not None and type(observed) not in {int, float, str}:
            raise ReleaseHistoryError("invalid monitor observed value")
        if type(observed) is float and not math.isfinite(observed):
            raise ReleaseHistoryError("non-finite monitor observed value")
        _number(check["threshold"], "monitor check threshold")
    if seen != _CHECK_NAMES:
        raise ReleaseHistoryError("health monitor check names are incomplete")
    failed = report["failed_checks"]
    if not isinstance(failed, list) or failed != failures:
        raise ReleaseHistoryError("health failure names contradict monitor evidence")
    if (status == "healthy") != (not failures):
        raise ReleaseHistoryError("health status contradicts failed checks")

    metrics = _object(monitor["metrics"], "monitor metrics", _METRICS)
    for name, value in metrics.items():
        _number(
            value,
            name,
            integral=name not in {"missing_pay_percent", "last_vintage_refresh_age_hours"},
        )
    missing = _number(metrics["missing_pay_percent"], "missing pay")
    if missing is not None and not 0 <= missing <= 100:
        raise ReleaseHistoryError("missing-pay percentage is outside 0-100")
    source_age = _number(metrics["source_age_days"], "source age", integral=True)
    refresh_age = _number(metrics["last_vintage_refresh_age_hours"], "refresh age")

    advice = _object(
        report["advice"],
        "rollback advice",
        frozenset({"action", "reason", "to_generation", "target", "reviewed_candidates"}),
    )
    action = advice["action"]
    if (
        not isinstance(action, str)
        or action not in {"keep", "investigate", "review_rollback"}
        or not isinstance(advice["reason"], str)
    ):
        raise ReleaseHistoryError("unknown health rollback advice")
    if (status == "healthy") != (action == "keep"):
        raise ReleaseHistoryError("health advice contradicts observed status")
    to_generation: int | None = None
    if action == "review_rollback":
        if failures != ["missing_pay_rate"] or generation < 2:
            raise ReleaseHistoryError("rollback review lacks specific release-quality failure")
        to_generation = _generation(advice["to_generation"], maximum=generation - 1)
        if _pin(advice["target"]) != state.events[to_generation - 1].to_vintage:
            raise ReleaseHistoryError("rollback target contradicts prior active pin")
    elif advice["to_generation"] is not None or advice["target"] is not None:
        raise ReleaseHistoryError("non-rollback advice must not claim a target")
    reviewed = advice["reviewed_candidates"]
    if not isinstance(reviewed, list):
        raise ReleaseHistoryError("invalid reviewed rollback candidates")
    review_keys: set[VintageKey] = set()
    healthy_generation: set[int] = set()
    prior_generation = generation
    for item in reviewed:
        review = _object(
            item,
            "reviewed candidate",
            frozenset({"generation", "vintage", "status", "failed_checks"}),
        )
        number = _generation(review["generation"], maximum=generation - 1)
        key = _pin(review["vintage"])
        candidate_status = review["status"]
        candidate_failures = review["failed_checks"]
        if (
            key in review_keys
            or key == vintage
            or number >= prior_generation
            or state.events[number - 1].to_vintage != key
            or not isinstance(candidate_status, str)
            or candidate_status not in {"healthy", "unhealthy", "unverifiable"}
            or not isinstance(candidate_failures, list)
            or any(
                not isinstance(name, str) or name not in _CHECK_NAMES for name in candidate_failures
            )
        ):
            raise ReleaseHistoryError("invalid reviewed rollback evidence")
        prior_generation = number
        review_keys.add(key)
        if candidate_status == "healthy":
            if candidate_failures:
                raise ReleaseHistoryError("healthy rollback candidate reports failed checks")
            healthy_generation.add(number)
        elif candidate_status == "unhealthy" and not candidate_failures:
            raise ReleaseHistoryError("unhealthy rollback candidate lacks failed checks")
        elif candidate_status == "unverifiable" and candidate_failures:
            raise ReleaseHistoryError("unverified rollback candidate must not claim check results")
    if action == "review_rollback" and to_generation not in healthy_generation:
        raise ReleaseHistoryError("rollback advice lacks a healthy reviewed target")
    if action != "review_rollback" and healthy_generation:
        raise ReleaseHistoryError("non-rollback advice contradicts a healthy candidate")
    if action == "keep" and reviewed:
        raise ReleaseHistoryError("healthy release should not have rollback candidates")

    return HealthSample(
        record_sha256=sha,
        checked_at_utc=checked_at.isoformat(),
        generation=generation,
        event_sha256=event_hash,
        vintage=vintage,
        status=cast(HealthStatus, status),
        failed_checks=tuple(failures),
        missing_pay_percent=float(missing) if missing is not None else None,
        source_age_days=cast(int | None, source_age),
        refresh_age_hours=float(refresh_age) if refresh_age is not None else None,
        advice=cast(Advice, action),
        rollback_to_generation=to_generation,
    )


def _metric_change(first: float | None, last: float | None) -> float | None:
    if first is None or last is None:
        return None
    return round(last - first, 6)


def _trend(samples: tuple[HealthSample, ...], total: int) -> HealthTrends:
    groups: dict[int, list[HealthSample]] = defaultdict(list)
    for point in samples:
        groups[point.generation].append(point)
    by_generation = tuple(
        GenerationTrend(
            generation=number,
            vintage=group[0].vintage,
            assessments=len(group),
            unhealthy_assessments=sum(item.status == "unhealthy" for item in group),
            first_checked_at_utc=group[0].checked_at_utc,
            last_checked_at_utc=group[-1].checked_at_utc,
            latest_status=group[-1].status,
            missing_pay_change_pp=_metric_change(
                group[0].missing_pay_percent, group[-1].missing_pay_percent
            ),
        )
        for number, group in sorted(groups.items())
    )
    summary = TrendSummary(
        assessments=len(samples),
        generations=len(groups),
        healthy_assessments=sum(item.status == "healthy" for item in samples),
        unhealthy_assessments=sum(item.status == "unhealthy" for item in samples),
        healthy_to_unhealthy=sum(
            before.status == "healthy" and after.status == "unhealthy"
            for before, after in itertools.pairwise(samples)
        ),
        unhealthy_to_healthy=sum(
            before.status == "unhealthy" and after.status == "healthy"
            for before, after in itertools.pairwise(samples)
        ),
        rollback_reviews=sum(item.advice == "review_rollback" for item in samples),
        first_checked_at_utc=samples[0].checked_at_utc if samples else None,
        last_checked_at_utc=samples[-1].checked_at_utc if samples else None,
        missing_pay_change_pp=(
            _metric_change(samples[0].missing_pay_percent, samples[-1].missing_pay_percent)
            if samples
            else None
        ),
    )
    return HealthTrends(total, summary, by_generation, samples)


def format_trends_csv(trends: HealthTrends) -> str:
    """Flatten the validated timeline for SQL/BI ingestion without null coercion."""
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(
        (
            "record_sha256",
            "checked_at_utc",
            "generation",
            "event_sha256",
            "model_version",
            "registry_commit",
            "source_sha256",
            "status",
            "failed_checks",
            "missing_pay_percent",
            "source_age_days",
            "refresh_age_hours",
            "advice",
            "rollback_to_generation",
        )
    )
    for sample in trends.samples:
        writer.writerow(
            (
                sample.record_sha256,
                sample.checked_at_utc,
                sample.generation,
                sample.event_sha256,
                sample.vintage.model_version,
                sample.vintage.registry_commit,
                sample.vintage.source_sha256,
                sample.status,
                ";".join(sample.failed_checks),
                sample.missing_pay_percent,
                sample.source_age_days,
                sample.refresh_age_hours,
                sample.advice,
                sample.rollback_to_generation,
            )
        )
    return output.getvalue()


class ReleaseHealthArchive:
    """Content-addressed assessments with validated, read-only historical views."""

    def __init__(self, data_root: Path) -> None:
        self.release = ReleaseStore(data_root)
        self.root = self.release.root / "health"
        self.records = self.root / "assessments"

    def _check_paths(self) -> None:
        for path in (self.root, self.records):
            if path.is_symlink() or (path.exists() and not path.is_dir()):
                raise ReleaseHistoryError("unsafe release-health archive directory")
            if path.exists() and path.resolve() != path:
                raise ReleaseHistoryError("symlinked release-health archive parent")

    def _ensure_directories(self) -> None:
        self._check_paths()
        for path in (self.root, self.records):
            try:
                path.mkdir(exist_ok=True)
            except OSError as exc:
                raise ReleaseHistoryError("cannot create release-health archive") from exc
        self._check_paths()

    def _read(self, path: Path) -> tuple[dict[str, object], bytes]:
        try:
            if path.resolve(strict=True) != path or not path.is_file():
                raise ReleaseHistoryError("unsafe health assessment path")
            if path.stat().st_size > _MAX_RECORD_BYTES:
                raise ReleaseHistoryError("health assessment is larger than the schema limit")
            payload = path.read_bytes()
            raw = json.loads(payload)
            if not isinstance(raw, dict) or _canonical(raw) != payload:
                raise ReleaseHistoryError("noncanonical health assessment JSON")
        except (OSError, ValueError, RuntimeError, UnicodeError) as exc:
            raise ReleaseHistoryError("cannot read valid health assessment") from exc
        return cast(dict[str, object], raw), payload

    def _publish(self, payload: bytes, sha: str) -> None:
        target = self.records / f"{sha}.json"
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.records, prefix=".health-", suffix=".tmp", delete=False
            ) as output:
                temporary = Path(output.name)
                output.write(payload)
                output.flush()
                os.fsync(output.fileno())
            try:
                # Hard-link publication prevents a concurrent writer from
                # overwriting an existing content-addressed record.
                os.link(temporary, target)
            except FileExistsError:
                _, existing = self._read(target)
                if existing != payload or hashlib.sha256(existing).hexdigest() != sha:
                    raise ReleaseHistoryError(
                        "conflicting health assessment at assessment hash"
                    ) from None
            directory = os.open(self.records, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError as exc:
            raise ReleaseHistoryError("cannot durably publish health assessment") from exc
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def record(self, report: ReleaseHealthReport) -> str:
        """Explicitly archive one stable active assessment; return its SHA-256.

        Only this method creates directories/files. It shares the release
        writer lock so an activation cannot supersede the assessed generation
        between pointer validation and durable record publication.
        """
        envelope = {
            "schema_version": 1,
            "pipeline_name": "ons_pay_release_health",
            "report": report.to_dict(),
        }
        try:
            payload = _canonical(envelope)
        except (TypeError, ValueError) as exc:
            raise ReleaseHistoryError("health assessment cannot be serialized") from exc
        if len(payload) > _MAX_RECORD_BYTES:
            raise ReleaseHistoryError("health assessment is larger than the schema limit")
        sha = hashlib.sha256(payload).hexdigest()
        with self.release.locked_history() as state:
            if (
                state is None
                or state.generation != report.generation
                or state.event_sha256 != report.event_sha256
                or state.active != report.vintage
            ):
                raise ReleaseHistoryError("active generation changed before recording; retry")
            # Validate the JSON representation that will actually be stored:
            # dataclasses.asdict preserves tuples before JSON turns them into lists.
            _sample(json.loads(payload), sha, state)
            self._ensure_directories()
            self._publish(payload, sha)
        return sha

    def trends(self, *, last: int = 50, generation: int | None = None) -> HealthTrends:
        """Validate **all** stored records before filtering and summarizing them."""
        if type(last) is not int or not 1 <= last <= 1000:
            raise ReleaseHistoryError("last must be an integer from 1 to 1000")
        if generation is not None and (type(generation) is not int or generation < 1):
            raise ReleaseHistoryError("generation filter must be a positive integer")
        state = self.release.history()
        self._check_paths()
        if not self.records.exists():
            return _trend((), 0)
        if state is None:
            raise ReleaseHistoryError("recorded assessments lack a verified release chain")
        samples: list[HealthSample] = []
        for path in sorted(self.records.glob("*.json")):
            if _HASH.fullmatch(path.stem) is None:
                raise ReleaseHistoryError("unrecognized health assessment filename")
            envelope, payload = self._read(path)
            if hashlib.sha256(payload).hexdigest() != path.stem:
                raise ReleaseHistoryError("content-addressed assessment hash mismatch")
            samples.append(_sample(envelope, path.stem, state))
        total = len(samples)
        samples.sort(key=lambda item: (item.checked_at_utc, item.record_sha256))
        if generation is not None:
            samples = [item for item in samples if item.generation == generation]
        selected = tuple(samples[-last:])
        latest_state = self.release.history()
        if (
            latest_state is None
            or latest_state.generation != state.generation
            or latest_state.event_sha256 != state.event_sha256
        ):
            raise ReleaseHistoryError("release generation changed during history read; retry")
        return _trend(selected, total)
