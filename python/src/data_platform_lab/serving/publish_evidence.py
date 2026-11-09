"""Immutable publication manifests, independent of manual release activation.

The local archive is trusted. Hashes detect content changes, not a malicious
writer who can replace both the manifest and its content-addressed filename.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from data_platform_lab.manifest import validate_manifest

if TYPE_CHECKING:
    from data_platform_lab.serving.publish_plan import PublishPlan
    from data_platform_lab.serving.publish_reconcile import ReconciliationReport
    from data_platform_lab.serving.publish_sinks import PublishReceipt, SinkName

_SHA = re.compile(r"[0-9a-f]{64}")
_COMMIT = re.compile(r"[0-9a-f]{40}")
_TARGET_ID = re.compile(r"[a-z][a-z0-9_-]{0,63}")
_PIPELINE = "ons_pay_publication"
_MAX_BYTES = 16_384
_MAX_RECORDS = 10_000
_FAILURES = {
    "source_validation_failed": 2,
    "configuration_error": 2,
    "database_error": 4,
    "reconciliation_failed": 3,
    "acceptance_rejected": 3,
}


class PublicationEvidenceError(ValueError):
    """Publication evidence is invalid, conflicting, or could not be persisted."""


def validate_target_id(value: str) -> str:
    """Validate the syntax of a public operator label; never echo invalid input."""
    if type(value) is not str or _TARGET_ID.fullmatch(value) is None:
        raise PublicationEvidenceError(
            "target ID must be 1-64 lowercase letters, digits, underscores or hyphens, "
            "starting with a letter"
        )
    return value


def validate_attempt_id(value: str) -> str:
    """Require a canonical UUID, preventing ambiguous identities or path input."""
    try:
        if str(UUID(value)) != value:
            raise ValueError
    except (ValueError, AttributeError, TypeError):
        raise PublicationEvidenceError("attempt ID must be a canonical UUID") from None
    return value


def _canonical(value: object) -> bytes:
    """Use the archive convention of canonical JSON followed by one newline."""
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode("utf-8")


def _object(value: object, fields: set[str]) -> dict[str, Any]:
    """Reject both omitted and unrecognized fields in every stored object."""
    if type(value) is not dict or set(value) != fields:
        raise PublicationEvidenceError("invalid publication evidence fields")
    return value


def _count(value: object, *, maximum: int = 100_000) -> int:
    """Require bounded integer counts, excluding boolean lookalikes."""
    if type(value) is not int or not 0 <= value <= maximum:
        raise PublicationEvidenceError("invalid publication evidence count")
    return value


def _sha(value: object) -> str:
    """Validate the same lowercase SHA-256 form used by the publication plan."""
    if type(value) is not str or _SHA.fullmatch(value) is None:
        raise PublicationEvidenceError("invalid publication evidence fingerprint")
    return value


def _utc(value: object) -> datetime:
    """Parse canonical, timezone-aware UTC timestamps for deterministic manifests."""
    if type(value) is not str:
        raise PublicationEvidenceError("invalid publication timestamp")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() != UTC.utcoffset(parsed) or parsed.isoformat() != value:
            raise ValueError
    except ValueError:
        raise PublicationEvidenceError("publication timestamps must be canonical UTC") from None
    return parsed


def _validate(raw: object) -> dict[str, Any]:
    """Check exact schema, provenance, counts and all cross-evidence relationships."""
    if (
        type(raw) is not dict
        or type(raw.get("schema_version")) is not int
        or raw["schema_version"] not in (1, 2)
    ):
        raise PublicationEvidenceError("unknown publication manifest schema")
    version = raw["schema_version"]
    record = _object(
        raw,
        {
            "schema_version",
            "pipeline_name",
            "run_id",
            "created_at",
            "source",
            "output",
            "row_count",
            "status",
            "started_at",
            "ended_at",
            "duration_seconds",
            "outcome",
            "sink",
            "table",
            "vintage",
            "counts",
            "fingerprints",
            "receipt",
            "reconciliation",
            "acceptance",
            "failure_code",
        }
        | ({"target_id"} if version == 2 else set()),
    )
    if validate_manifest(record) or record["pipeline_name"] != _PIPELINE:
        raise PublicationEvidenceError("unknown publication manifest schema")
    if version == 2:
        validate_target_id(record["target_id"])
    validate_attempt_id(record["run_id"])
    started, ended = _utc(record["started_at"]), _utc(record["ended_at"])
    if (
        ended < started
        or record["created_at"] != record["ended_at"]
        or type(record["duration_seconds"]) is not float
        or record["duration_seconds"] != (ended - started).total_seconds()
    ):
        raise PublicationEvidenceError("inconsistent publication timestamps")
    if record["sink"] not in ("postgresql", "mongodb") or record["table"] not in (
        "geography_year",
        "local_authority_yoy",
    ):
        raise PublicationEvidenceError("unknown publication target")
    vintage = _object(record["vintage"], {"model_version", "registry_commit", "source_sha256"})
    if (
        type(vintage["model_version"]) is not int
        or vintage["model_version"] != 1
        or type(vintage["registry_commit"]) is not str
        or _COMMIT.fullmatch(vintage["registry_commit"]) is None
    ):
        raise PublicationEvidenceError("invalid publication vintage")
    _sha(vintage["source_sha256"])
    if (
        record["source"] != f"sha256:{vintage['source_sha256']}"
        or record["output"] != f"{record['sink']}:ons_pay_{record['table']}"
    ):
        raise PublicationEvidenceError("manifest source or output contradicts publication")
    counts = _object(record["counts"], {"source", "applied", "observed"})
    source_count = _count(counts["source"])
    for name in ("applied", "observed"):
        if counts[name] is not None:
            _count(counts[name])
    fingerprints = _object(
        record["fingerprints"],
        {
            "source_artifact_sha256",
            "source_plan_sha256",
            "expected_sink_sha256",
            "observed_sink_sha256",
        },
    )
    for name, value in fingerprints.items():
        if name != "observed_sink_sha256" or value is not None:
            _sha(value)
    receipt = record["receipt"]
    if receipt is not None:
        receipt = _object(
            receipt, {"sink", "table", "vintage", "records_applied", "records_sha256"}
        )
        if (
            any(
                _canonical(receipt[key]) != _canonical(record[key])
                for key in ("sink", "table", "vintage")
            )
            or _count(receipt["records_applied"]) != source_count
            or receipt["records_sha256"] != fingerprints["source_plan_sha256"]
        ):
            raise PublicationEvidenceError("publication receipt contradicts source")
    if counts["applied"] != (receipt["records_applied"] if receipt else None):
        raise PublicationEvidenceError("applied count contradicts write receipt")
    if _count(record["row_count"]) != (counts["applied"] or 0):
        raise PublicationEvidenceError("manifest count contradicts write receipt")
    summary = record["reconciliation"]
    if summary is None:
        if counts["observed"] is not None or fingerprints["observed_sink_sha256"] is not None:
            raise PublicationEvidenceError("observed values lack a reconciliation report")
    else:
        summary = _object(
            summary,
            {
                "passed",
                "missing_count",
                "unexpected_count",
                "changed_record_count",
                "changed_field_count",
            },
        )
        missing, extra, changed = (
            _count(summary[key])
            for key in ("missing_count", "unexpected_count", "changed_record_count")
        )
        fields = _count(summary["changed_field_count"], maximum=2**63 - 1)
        observed = _count(counts["observed"])
        _sha(fingerprints["observed_sink_sha256"])
        if (
            missing > source_count
            or extra > observed
            or source_count - missing != observed - extra
            or changed > source_count - missing
            or fields < changed
            or (changed == 0 and fields != 0)
        ):
            raise PublicationEvidenceError("reconciliation counts are inconsistent")
        passed = (
            not (missing or extra or changed or fields)
            and source_count == observed
            and fingerprints["expected_sink_sha256"] == fingerprints["observed_sink_sha256"]
        )
        if type(summary["passed"]) is not bool or summary["passed"] != passed:
            raise PublicationEvidenceError("reconciliation verdict contradicts evidence")
    outcome = record["outcome"]
    if outcome not in ("accepted", "rejected", "failed"):
        raise PublicationEvidenceError("unknown publication outcome")
    if record["status"] != ("success" if outcome == "accepted" else "failed"):
        raise PublicationEvidenceError("manifest status contradicts publication outcome")
    if outcome == "accepted":
        expected_acceptance = {
            "sink": record["sink"],
            "table": record["table"],
            "vintage": vintage,
            "records_accepted": source_count,
            "source_plan_sha256": fingerprints["source_plan_sha256"],
            "sink_snapshot_sha256": fingerprints["expected_sink_sha256"],
            "status": "accepted",
        }
        acceptance = _object(record["acceptance"], set(expected_acceptance))
        if (
            receipt is None
            or summary is None
            or summary["passed"] is not True
            or record["failure_code"] is not None
            or _canonical(acceptance) != _canonical(expected_acceptance)
            or type(acceptance["records_accepted"]) is not int
        ):
            raise PublicationEvidenceError("accepted publication lacks matching gate evidence")
    elif record["acceptance"] is not None or record["failure_code"] not in tuple(_FAILURES):
        raise PublicationEvidenceError("unsuccessful publication has invalid failure evidence")
    elif outcome == "rejected":
        if (
            receipt is None
            or summary is None
            or summary["passed"]
            or record["failure_code"] != "acceptance_rejected"
        ):
            raise PublicationEvidenceError("rejected publication lacks a completed gate rejection")
    elif record["failure_code"] == "acceptance_rejected":
        raise PublicationEvidenceError("failed publication cannot claim a completed gate rejection")
    return record


@dataclass(frozen=True, slots=True)
class PublicationEvidence:
    """Immutable canonical bytes; callers receive independent JSON dictionaries."""

    payload: bytes

    @property
    def record_sha256(self) -> str:
        """Content address of this exact manifest, including its attempt timestamps."""
        return hashlib.sha256(self.payload).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """Read a defensive copy of the verified publication manifest."""
        return _validate(json.loads(self.payload))

    def cli_result(self, *, replayed: bool) -> dict[str, Any]:
        """Reproduce the operator summary without asserting a fresh sink observation."""
        record = self.to_dict()
        failure = record["failure_code"]
        return {
            "schema_version": 1,
            "operation": "apply",
            "status": record["outcome"],
            **({"target_id": record["target_id"]} if "target_id" in record else {}),
            **{
                key: record[key]
                for key in (
                    "sink",
                    "table",
                    "vintage",
                    "counts",
                    "fingerprints",
                    "reconciliation",
                    "acceptance",
                )
            },
            "error": None
            if failure is None
            else {"code": failure, "message": "Recorded publication attempt did not succeed."},
            "evidence": {
                "attempt_id": record["run_id"],
                "record_sha256": self.record_sha256,
                "replayed": replayed,
            },
        }

    @property
    def exit_code(self) -> int:
        """Return the original operation's stable exit code for an idempotent retry."""
        failure = self.to_dict()["failure_code"]
        return 0 if failure is None else _FAILURES[failure]


def _build(
    plan: PublishPlan,
    *,
    sink: SinkName,
    attempt_id: str,
    started_at: datetime,
    ended_at: datetime,
    receipt: PublishReceipt | None,
    report: ReconciliationReport | None,
    failure_code: str | None,
    target_id: str | None,
) -> PublicationEvidence:
    """Derive the outcome from existing gate inputs, never from a caller's status claim."""
    from data_platform_lab.serving.publish_gate import (
        PublicationAcceptanceError,
        accept_publication,
    )
    from data_platform_lab.serving.publish_reconcile import expected_sink_fingerprint

    if target_id is not None:
        validate_target_id(target_id)
    expected = expected_sink_fingerprint(plan, sink=sink)
    acceptance = None
    outcome = "failed"
    if failure_code is None:
        if receipt is None or report is None:
            raise PublicationEvidenceError(
                "a completed outcome requires write and read-back evidence"
            )
        try:
            acceptance = accept_publication(plan, receipt, report).to_dict()
            outcome = "accepted"
        except PublicationAcceptanceError:
            outcome, failure_code = "rejected", "acceptance_rejected"
    elif failure_code not in _FAILURES or failure_code == "acceptance_rejected":
        raise PublicationEvidenceError("unknown execution failure code")
    if report is not None and (
        report.sink != sink
        or report.table != plan.table
        or report.vintage != plan.vintage
        or report.source_plan_sha256 != plan.records_sha256
        or report.expected_sink_sha256 != expected
        or report.expected_count != plan.source_rows
    ):
        raise PublicationEvidenceError("reconciliation report contradicts the source plan")
    raw = {
        "schema_version": 1 if target_id is None else 2,
        **({"target_id": target_id} if target_id is not None else {}),
        "pipeline_name": _PIPELINE,
        "run_id": attempt_id,
        "created_at": ended_at.isoformat(),
        "started_at": started_at.isoformat(),
        "ended_at": ended_at.isoformat(),
        "duration_seconds": (ended_at - started_at).total_seconds(),
        "source": f"sha256:{plan.vintage.source_sha256}",
        "output": f"{sink}:ons_pay_{plan.table}",
        "row_count": receipt.records_applied if receipt else 0,
        "status": "success" if outcome == "accepted" else "failed",
        "outcome": outcome,
        "sink": sink,
        "table": plan.table,
        "vintage": asdict(plan.vintage),
        "counts": {
            "source": plan.source_rows,
            "applied": receipt.records_applied if receipt else None,
            "observed": report.observed_count if report else None,
        },
        "fingerprints": {
            "source_artifact_sha256": plan.source_artifact_sha256,
            "source_plan_sha256": plan.records_sha256,
            "expected_sink_sha256": expected,
            "observed_sink_sha256": report.observed_sink_sha256 if report else None,
        },
        "receipt": asdict(receipt) if receipt else None,
        "reconciliation": None
        if report is None
        else {
            "passed": report.passed,
            "missing_count": len(report.missing_ids),
            "unexpected_count": len(report.unexpected_ids),
            "changed_record_count": len(report.changed_fields),
            "changed_field_count": sum(len(fields) for fields in report.changed_fields.values()),
        },
        "acceptance": acceptance,
        "failure_code": failure_code,
    }
    try:
        payload = _canonical(_validate(raw))
    except (TypeError, ValueError, OverflowError) as exc:
        raise PublicationEvidenceError("cannot build consistent publication evidence") from exc
    if len(payload) > _MAX_BYTES:
        raise PublicationEvidenceError("publication evidence exceeds the size limit")
    return PublicationEvidence(payload)


def _sync_directory(path: Path) -> None:
    """Use the existing release/archive directory-fsync durability pattern."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _matches_publication(
    record: dict[str, Any], plan: PublishPlan, *, sink: SinkName, target_id: str | None
) -> bool:
    """Use the same exact source and target identity for retries and baseline checks."""
    return (
        record["sink"] == sink
        and record.get("target_id") == target_id
        and record["table"] == plan.table
        and record["vintage"] == asdict(plan.vintage)
        and record["fingerprints"]["source_artifact_sha256"] == plan.source_artifact_sha256
        and record["fingerprints"]["source_plan_sha256"] == plan.records_sha256
    )


class PublicationArchive:
    """Append-only manifests with one terminal record per attempt, never a release pointer."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = Path(os.path.abspath(data_root))
        self.root = self.data_root / "manifests" / _PIPELINE

    def _check_paths(self) -> None:
        """Reject indirect and non-directory paths before reading or creating metadata."""
        try:
            for path in (self.data_root, self.root.parent, self.root):
                if (
                    path.is_symlink()
                    or path.resolve() != path
                    or (path.exists() and not path.is_dir())
                ):
                    raise PublicationEvidenceError("unsafe publication archive directory")
        except (OSError, RuntimeError) as exc:
            raise PublicationEvidenceError("unsafe publication archive directory") from exc

    def _read(self, path: Path) -> PublicationEvidence:
        """Read one bounded regular file and validate its exact bytes and schema."""
        try:
            if _SHA.fullmatch(path.stem) is None or path.is_symlink() or path.resolve() != path:
                raise PublicationEvidenceError("unsafe publication record path")
            fd = os.open(
                path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
            )
            with os.fdopen(fd, "rb") as handle:
                if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                    raise PublicationEvidenceError("publication record is not a regular file")
                payload = handle.read(_MAX_BYTES + 1)
            if len(payload) > _MAX_BYTES or hashlib.sha256(payload).hexdigest() != path.stem:
                raise PublicationEvidenceError("publication evidence size or hash mismatch")
            record = _validate(json.loads(payload))
            if _canonical(record) != payload:
                raise PublicationEvidenceError("noncanonical publication evidence")
        except (OSError, ValueError, RuntimeError, UnicodeError, TypeError) as exc:
            raise PublicationEvidenceError("cannot read valid publication evidence") from exc
        return PublicationEvidence(payload)

    def history(
        self, *, last: int = 50, target_id: str | None = None
    ) -> tuple[PublicationEvidence, ...]:
        """Validate all records, then filter by label before limiting; never create files."""
        if type(last) is not int or not 1 <= last <= _MAX_RECORDS:
            raise PublicationEvidenceError("history limit must be an integer from 1 to 10000")
        if target_id is not None:
            validate_target_id(target_id)
        try:
            self._check_paths()
            if not self.root.exists():
                return ()
            records: list[PublicationEvidence] = []
            seen: set[str] = set()
            for path in self.root.iterdir():
                if path.name == ".publication.lock" or (
                    path.name.startswith(".publication-") and path.suffix == ".tmp"
                ):
                    continue
                if path.suffix != ".json" or len(records) >= _MAX_RECORDS:
                    raise PublicationEvidenceError(
                        "unknown archive entry or archive limit exceeded"
                    )
                evidence = self._read(path)
                attempt_id = evidence.to_dict()["run_id"]
                if attempt_id in seen:
                    raise PublicationEvidenceError(
                        "conflicting outcomes for one publication attempt"
                    )
                seen.add(attempt_id)
                records.append(evidence)
            records.sort(key=lambda item: (item.to_dict()["ended_at"], item.record_sha256))
            if target_id is not None:
                records = [item for item in records if item.to_dict().get("target_id") == target_id]
            return tuple(records[-last:])
        except OSError as exc:
            raise PublicationEvidenceError("cannot inspect publication archive") from exc

    def get_attempt(self, attempt_id: str) -> PublicationEvidence | None:
        """Read a final attempt, checking the entire archive even when it is absent."""
        validate_attempt_id(attempt_id)
        return next(
            (
                record
                for record in self.history(last=_MAX_RECORDS)
                if record.to_dict()["run_id"] == attempt_id
            ),
            None,
        )

    def accepted_baseline(
        self,
        plan: PublishPlan,
        *,
        sink: SinkName,
        attempt_id: str,
        target_id: str | None = None,
    ) -> PublicationEvidence:
        """Read an accepted record matching the current plan, without locks or writes.

        This validates historical evidence only. The caller must separately read
        the current sink to detect drift; the returned acceptance is not refreshed.
        """
        from data_platform_lab.serving.publish_reconcile import expected_sink_fingerprint

        if target_id is not None:
            validate_target_id(target_id)
        evidence = self.get_attempt(attempt_id)
        if evidence is None:
            raise PublicationEvidenceError("accepted baseline was not found")
        record = evidence.to_dict()
        if (
            record["outcome"] != "accepted"
            or not _matches_publication(record, plan, sink=sink, target_id=target_id)
            or record["counts"]["source"] != plan.source_rows
            or record["fingerprints"]["expected_sink_sha256"]
            != expected_sink_fingerprint(plan, sink=sink)
        ):
            raise PublicationEvidenceError(
                "baseline does not establish acceptance of this publication"
            )
        return evidence

    @contextmanager
    def _locked(self) -> Iterator[None]:
        """Serialize local evidence writers using an independent POSIX lock."""
        try:
            import fcntl
        except ModuleNotFoundError:
            raise PublicationEvidenceError(
                "recording evidence requires a local POSIX filesystem"
            ) from None
        try:
            self._check_paths()
            for path in (self.data_root, self.root.parent, self.root):
                if not path.exists():
                    path.mkdir(exist_ok=True)
                    _sync_directory(path.parent)
            self._check_paths()
            fd = os.open(
                self.root / ".publication.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
            )
            with os.fdopen(fd, "rb") as handle:
                if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                    raise PublicationEvidenceError("publication lock is not a regular file")
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError as exc:
            raise PublicationEvidenceError("cannot lock or persist publication evidence") from exc

    def prepare(
        self,
        plan: PublishPlan,
        *,
        sink: SinkName,
        attempt_id: str,
        target_id: str | None = None,
    ) -> PublicationEvidence | None:
        """Check storage and attempt reuse before sink I/O; only used for explicit writes."""
        validate_attempt_id(attempt_id)
        if target_id is not None:
            validate_target_id(target_id)
        with self._locked():
            records = self.history(last=_MAX_RECORDS)
            existing = next(
                (item for item in records if item.to_dict()["run_id"] == attempt_id), None
            )
            if existing is None and len(records) >= _MAX_RECORDS:
                raise PublicationEvidenceError("publication archive limit exceeded")
            if existing is not None:
                record = existing.to_dict()
                if not _matches_publication(record, plan, sink=sink, target_id=target_id):
                    raise PublicationEvidenceError(
                        "attempt ID already belongs to a different publication"
                    )
                # Complete durability after a previous link succeeded but directory fsync failed.
                _sync_directory(self.root)
            return existing

    def _publish(self, evidence: PublicationEvidence) -> None:
        """Publish complete bytes without replacing an existing content-addressed record."""
        target = self.root / f"{evidence.record_sha256}.json"
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=self.root, prefix=".publication-", suffix=".tmp", delete=False
            ) as output:
                temporary = Path(output.name)
                output.write(evidence.payload)
                output.flush()
                os.fsync(output.fileno())
            os.link(temporary, target)
            _sync_directory(self.root)
        finally:
            if temporary is not None:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def record(
        self,
        plan: PublishPlan,
        *,
        sink: SinkName,
        attempt_id: str,
        started_at: datetime,
        ended_at: datetime,
        receipt: PublishReceipt | None = None,
        report: ReconciliationReport | None = None,
        failure_code: str | None = None,
        target_id: str | None = None,
    ) -> PublicationEvidence:
        """Explicitly record an outcome, evaluating the acceptance gate for completed attempts.

        Retry persistence with identical inputs and timestamps. A new sink attempt
        needs a new UUID. Conflicts never replace an earlier accepted or failed record.
        Optional target labels describe operator configuration, not verified server identity.
        """
        try:
            evidence = _build(
                plan,
                sink=sink,
                attempt_id=attempt_id,
                started_at=started_at,
                ended_at=ended_at,
                receipt=receipt,
                report=report,
                failure_code=failure_code,
                target_id=target_id,
            )
        except (TypeError, ValueError, RuntimeError) as exc:
            raise PublicationEvidenceError("cannot build consistent publication evidence") from exc
        with self._locked():
            records = self.history(last=_MAX_RECORDS)
            existing = next(
                (item for item in records if item.to_dict()["run_id"] == attempt_id), None
            )
            if existing is not None:
                if existing.payload != evidence.payload:
                    raise PublicationEvidenceError(
                        "attempt already has a different immutable outcome"
                    )
                _sync_directory(self.root)
                return existing
            if len(records) >= _MAX_RECORDS:
                raise PublicationEvidenceError("publication archive limit exceeded")
            self._publish(evidence)
        return evidence
