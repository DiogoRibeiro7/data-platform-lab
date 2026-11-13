"""Transform an explicitly selected completed raw-load batch without source I/O."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from data_platform_lab.curation.ons_pay import (
    MODEL_VERSION,
    CurationResult,
    IngestedPay,
    curate_ons_pay,
    parse_ingestion_receipt,
)
from data_platform_lab.orchestration.local_run_guard import (
    LocalRunBusyError,
    LocalRunGuardError,
    guard_local_roots,
)
from data_platform_lab.orchestration.ons_pay_refresh import _write_audit, parse_retrieved_date
from data_platform_lab.orchestration.ons_pay_refresh_job import _reject_constant, _unique_object
from data_platform_lab.storage import BlobStore, LocalBlobStore

_MAX_AUDIT_BYTES = 4 * 1024 * 1024
_MAX_RECEIPT_BYTES = 1024 * 1024
_MAX_SOURCES = 256
_LOAD_COUNTS = ("objects_published", "objects_reused", "bytes_verified")


class TransformError(ValueError):
    """A transformation failed; an audit may describe completed source progress."""

    def __init__(self, message: str, *, receipt_path: Path | None = None) -> None:
        super().__init__(message)
        self.receipt_path = receipt_path


class TransformBusyError(TransformError):
    """A cooperating writer holds a local destination; no batch audit was created."""


@dataclass(frozen=True, slots=True)
class TransformSource:
    """One validated ingestion receipt bound to the planning snapshot."""

    receipt_path: Path
    receipt_sha256: str
    source: IngestedPay


@dataclass(frozen=True, slots=True)
class TransformPlan:
    """An ordered metadata plan; stored bytes and model quality remain unchecked."""

    load_audit_path: Path
    load_audit_sha256: str
    load_run_id: str
    load_job_sha256: str | None
    sources: tuple[TransformSource, ...]


@dataclass(frozen=True, slots=True)
class TransformedSource:
    """A successfully curated source and its original ingestion identity."""

    planned: TransformSource
    result: CurationResult


@dataclass(frozen=True, slots=True)
class TransformResult:
    """Successful offline batch with a separate transformation audit."""

    run_id: str
    receipt_path: Path
    plan: TransformPlan
    sources: tuple[TransformedSource, ...]


def _mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TransformError("Invalid object in load evidence.")
    return value


def _count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise TransformError("Invalid non-negative count in load evidence.")
    return value


def _hex(value: object, length: int) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", value) is None:
        raise TransformError("Invalid identity in load evidence.")
    return value


def _read(path: Path, maximum: int) -> tuple[dict[str, Any], str]:
    path = Path(os.path.abspath(path))
    if not path.is_file() or path.resolve(strict=True) != path.absolute():
        raise TransformError("Load evidence must be a regular file without symlinks.")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise TransformError("Load evidence exceeds its byte limit.")
    return (
        _mapping(
            json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
        ),
        hashlib.sha256(raw).hexdigest(),
    )


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise TransformError("Invalid load timestamp.")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise TransformError("Load timestamps must carry a timezone.")
    return parsed.astimezone(UTC)


def plan_ons_pay_transform(
    *, load_audit: Path, expected_load_sha256: str | None = None
) -> TransformPlan:
    """Validate all receipt links before any transformation, network or store I/O.

    Only successful whole raw-load batches are accepted. Paths inside the audit
    are absolute, as emitted by refresh; paths are not rebased after moving files.
    """
    try:
        audit, digest = _read(load_audit, _MAX_AUDIT_BYTES)
        if expected_load_sha256 is not None and _hex(expected_load_sha256, 64) != digest:
            raise TransformError("Load audit differs from the selected SHA-256.")
        if (
            audit.get("pipeline_name") != "ons_pay_load"
            or type(audit.get("schema_version")) is not int
            or audit["schema_version"] != 1
            or audit.get("status") != "success"
            or audit.get("failure") is not None
            or "model_version" in audit
        ):
            raise TransformError("Expected a successful whole ONS raw-load v1 batch.")
        run_id = _hex(audit.get("run_id"), 32)
        started, completed = (
            _timestamp(audit.get("started_at")),
            _timestamp(audit.get("created_at")),
        )
        duration = audit.get("duration_seconds")
        if (
            started > completed
            or completed > datetime.now(UTC)
            or type(duration) not in {int, float}
            or not isinstance(duration, (int, float))
            or not math.isfinite(duration)
            or duration < 0
            or abs(duration - (completed - started).total_seconds()) > 0.000001
        ):
            raise TransformError("Inconsistent load timing.")
        job_sha = _hex(audit["job_sha256"], 64) if "job_sha256" in audit else None
        entries = audit.get("sources")
        if not isinstance(entries, list) or not 1 <= len(entries) <= _MAX_SOURCES:
            raise TransformError("Load batch must contain 1 to 256 completed sources.")
        totals = dict.fromkeys(_LOAD_COUNTS, 0)
        max_age = 0
        seen: set[tuple[str, str]] = set()
        planned: list[TransformSource] = []
        for entry in entries:
            item = _mapping(entry)
            value = item.get("ingestion_receipt_path")
            if not isinstance(value, str) or not Path(value).is_absolute():
                raise TransformError("Load audit must link absolute ingestion receipt paths.")
            receipt_path = Path(value)
            receipt, receipt_sha = _read(receipt_path, _MAX_RECEIPT_BYTES)
            source = parse_ingestion_receipt(receipt)
            pin = (source.registry_commit, source.source_sha256)
            if (
                pin in seen
                or pin != (item.get("registry_commit"), item.get("source_sha256"))
                or item.get("retrieved_at") != source.source["retrieved_at"]
            ):
                raise TransformError("Load source identity differs from its ingestion receipt.")
            seen.add(pin)
            parse_retrieved_date(source.source["retrieved_at"])
            max_age = max(max_age, _count(item.get("source_age_days")))
            objects = receipt["objects"]
            observed = {
                "objects_published": sum(obj["status"] == "published" for obj in objects),
                "objects_reused": sum(obj["status"] == "reused" for obj in objects),
                "bytes_verified": sum(_count(obj.get("size_bytes")) for obj in objects),
            }
            for field in _LOAD_COUNTS:
                count = _count(item.get(field))
                receipt_field = {
                    "objects_published": "files_published",
                    "objects_reused": "files_reused",
                }.get(field, field)
                if count != observed[field] or count != _count(receipt.get(receipt_field)):
                    raise TransformError("Load source counts differ from its ingestion receipt.")
                totals[field] += count
            planned.append(TransformSource(receipt_path, receipt_sha, source))
        metrics = _mapping(audit.get("metrics"))
        if (
            _count(metrics.get("vintages_completed")) != len(planned)
            or _count(metrics.get("max_source_age_days_observed")) != max_age
            or any(_count(metrics.get(field)) != count for field, count in totals.items())
        ):
            raise TransformError("Load batch counts disagree with its completed sources.")
        return TransformPlan(load_audit.resolve(), digest, run_id, job_sha, tuple(planned))
    except TransformError:
        raise
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, OverflowError):
        raise TransformError("Cannot validate the load audit and its ingestion receipts.") from None


def _record(item: TransformedSource) -> dict[str, object]:
    source, result = item.planned, item.result
    return {
        "registry_commit": source.source.registry_commit,
        "source_sha256": source.source.source_sha256,
        "ingestion_receipt_path": str(source.receipt_path),
        "ingestion_receipt_sha256": source.receipt_sha256,
        "curation_receipt_path": str(result.receipt_path),
        "source_rows": result.source_rows,
        "rows_with_pay": result.rows_with_pay,
        "rows_without_pay": result.rows_without_pay,
        "parquet_published": sum(not output.reused for output in result.outputs),
        "parquet_reused": sum(output.reused for output in result.outputs),
    }


def _transform_locked(
    *, load_audit: Path, store: BlobStore, output_root: Path, expected_load_sha256: str | None
) -> TransformResult:
    started, run_id = datetime.now(UTC), uuid4().hex
    receipt_path = output_root / "manifests" / f"ons_pay_transform_{run_id}.json"
    plan: TransformPlan | None = None
    completed: list[TransformedSource] = []
    failure: dict[str, object] | None = None
    phase = "preflight"
    index: int | None = None
    try:
        plan = plan_ons_pay_transform(
            load_audit=load_audit, expected_load_sha256=expected_load_sha256
        )
        phase = "curation"
        for source in plan.sources:
            index = len(completed)
            result = curate_ons_pay(
                input_receipt=source.receipt_path,
                store=store,
                output_root=output_root,
                expected_receipt_sha256=source.receipt_sha256,
            )
            completed.append(TransformedSource(source, result))
    except Exception as exc:
        # Driver messages can contain source rows or private connection details.
        failure = {
            "phase": phase,
            "source_index": index,
            "error_type": type(exc).__name__,
            "message": f"Batch {phase} failed.",
        }
    finished = datetime.now(UTC)
    records = [_record(item) for item in completed]
    document = {
        "pipeline_name": "ons_pay_transform",
        "schema_version": 1,
        "model_version": MODEL_VERSION,
        "run_id": run_id,
        "status": "failed" if failure else "success",
        "started_at": started.isoformat(),
        "created_at": finished.isoformat(),
        "duration_seconds": (finished - started).total_seconds(),
        "input": {
            "load_audit_path": str(load_audit.resolve()),
            "load_audit_sha256": plan.load_audit_sha256 if plan else None,
            "load_run_id": plan.load_run_id if plan else None,
            "load_job_sha256": plan.load_job_sha256 if plan else None,
            "expected_load_sha256": expected_load_sha256,
        },
        "sources": records,
        "sources_planned": len(plan.sources) if plan else None,
        "metrics": {
            "vintages_completed": len(completed),
            **{
                field: sum(getattr(item.result, field) for item in completed)
                for field in ("source_rows", "rows_with_pay", "rows_without_pay")
            },
            "parquet_published": sum(
                not output.reused for item in completed for output in item.result.outputs
            ),
            "parquet_reused": sum(
                output.reused for item in completed for output in item.result.outputs
            ),
        },
        "failure": failure,
    }
    try:
        _write_audit(receipt_path, document)
    except (OSError, ValueError):
        raise TransformError(
            "Unable to publish the transformation audit; outputs may already exist."
        ) from None
    if failure is not None or plan is None:
        raise TransformError(
            f"ONS transformation failed in {phase}; audit: {receipt_path}",
            receipt_path=receipt_path,
        )
    return TransformResult(run_id, receipt_path, plan, tuple(completed))


def transform_ons_pay(
    *,
    load_audit: Path,
    store: BlobStore,
    output_root: Path,
    expected_load_sha256: str | None = None,
) -> TransformResult:
    """Curate a completed raw batch with local guards and separate progress evidence.

    A retry revalidates all receipts and stored bytes, then reuses matching Parquet.
    Earlier successful sources survive later failures; no source is downloaded,
    no full-refresh success is fabricated, and no release is activated.
    """
    if expected_load_sha256 is not None:
        _hex(expected_load_sha256, 64)
    try:
        output_root = output_root.resolve()
        roots = [output_root]
        if isinstance(store, LocalBlobStore):
            roots.append(store.root)
        with guard_local_roots(roots):
            return _transform_locked(
                load_audit=load_audit,
                store=store,
                output_root=output_root,
                expected_load_sha256=expected_load_sha256,
            )
    except LocalRunBusyError:
        raise TransformBusyError(
            "Another ONS writer is using a destination; retry after it finishes."
        ) from None
    except LocalRunGuardError:
        raise TransformError("Unable to acquire ONS transformation destination guards.") from None
    except (OSError, RuntimeError):
        raise TransformError("Unable to access ONS transformation destinations.") from None
