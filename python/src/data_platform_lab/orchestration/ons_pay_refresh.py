"""Resumable, provider-neutral ONS pay ingestion and curation refresh batches.

Each source is a *pinned* canonical handoff paired with its exact Git checkout.
There is no implicit latest snapshot, external fetch, or mutable output key.
Earlier successful vintages remain published if a later backfill item fails;
an atomic batch audit makes the partial outcome visible before a safe retry.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from data_platform_lab.curation.ons_pay import DATASET_ID, MODEL_VERSION, curate_ons_pay
from data_platform_lab.ingestion.registry_handoff import ingest_handoff, parse_handoff
from data_platform_lab.storage import BlobStore

_REGISTRY = "DiogoRibeiro7/data"
_RAW_SOURCE = f"datasets/{DATASET_ID}/raw/gross-median-weekly-pay.csv"


class RefreshError(ValueError):
    """A refresh failed; an audit receipt may identify its partial progress."""

    def __init__(
        self,
        message: str,
        *,
        receipt_path: Path | None = None,
        source_index: int | None = None,
    ) -> None:
        super().__init__(message)
        self.receipt_path = receipt_path
        self.source_index = source_index


@dataclass(frozen=True, slots=True)
class RefreshSource:
    """One handoff and the Git checkout at the handoff's exact commit."""

    manifest_path: Path
    registry_root: Path


@dataclass(frozen=True, slots=True)
class PlannedSource:
    """Validated source identity and age before any ingestion writes."""

    source: RefreshSource
    registry_commit: str
    source_sha256: str
    retrieved_at: str
    age_days: int


@dataclass(frozen=True, slots=True)
class RefreshedSource:
    """One successfully ingested/curated vintage with auditable counts."""

    manifest_path: Path
    registry_commit: str
    source_sha256: str
    retrieved_at: str
    source_age_days: int
    ingestion_receipt_path: Path
    curation_receipt_path: Path
    objects_published: int
    objects_reused: int
    bytes_verified: int
    parquet_published: int
    parquet_reused: int
    source_rows: int
    rows_with_pay: int
    rows_without_pay: int
    silver_rows: int
    geography_year_rows: int
    local_authority_yoy_rows: int


@dataclass(frozen=True, slots=True)
class RefreshResult:
    run_id: str
    receipt_path: Path
    sources: tuple[RefreshedSource, ...]


def parse_retrieved_date(value: str) -> date:
    """Interpret upstream retrieval metadata as UTC calendar-day freshness."""
    try:
        return date.fromisoformat(value)
    except ValueError:
        pass
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RefreshError("source.retrieved_at must be an ISO date or aware timestamp") from exc
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise RefreshError("source.retrieved_at timestamp must include a timezone")
    return timestamp.astimezone(UTC).date()


def plan_ons_pay_refresh(
    sources: Sequence[RefreshSource],
    *,
    max_source_age_days: int | None = None,
    as_of: date | None = None,
) -> tuple[PlannedSource, ...]:
    """Validate a batch's manifests, identity, duplicate pins and freshness SLA.

    Planning is read-only. In particular, it does not verify Git source bytes;
    the ingestion step always re-verifies committed blobs before publishing.
    """
    if not sources:
        raise RefreshError("at least one pinned ONS pay handoff is required")
    if max_source_age_days is not None and (
        type(max_source_age_days) is not int or max_source_age_days < 0
    ):
        raise RefreshError("max_source_age_days must be a non-negative integer")
    today = as_of if as_of is not None else datetime.now(UTC).date()
    if not isinstance(today, date) or isinstance(today, datetime):
        raise RefreshError("as_of must be a UTC calendar date")

    inspected: list[PlannedSource] = []
    seen: set[tuple[str, str]] = set()
    for index, source in enumerate(sources):
        try:
            plan = parse_handoff(json.loads(source.manifest_path.read_text(encoding="utf-8")))
            if plan.dataset_id != DATASET_ID or plan.repository != _REGISTRY:
                raise RefreshError("handoff is not the approved ONS pay registry source")
            raw = [item for item in plan.objects if item.role == "raw"]
            if len(raw) != 1 or raw[0].source_path != _RAW_SOURCE or raw[0].format != "csv":
                raise RefreshError("handoff must identify the one canonical ONS pay CSV")
            pin = (plan.commit, raw[0].sha256)
            if pin in seen:
                raise RefreshError("the batch repeats an identical registry/source pin")
            retrieved = parse_retrieved_date(plan.retrieved_at)
            age_days = (today - retrieved).days
            if age_days < 0:
                raise RefreshError("source retrieval date is in the future")
            if max_source_age_days is not None and age_days > max_source_age_days:
                raise RefreshError(
                    f"source freshness SLA breached: {age_days} > {max_source_age_days} days"
                )
            seen.add(pin)
            inspected.append(
                PlannedSource(source, plan.commit, raw[0].sha256, plan.retrieved_at, age_days)
            )
        except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
            raise RefreshError(
                f"invalid handoff at source {index + 1}: {exc}", source_index=index
            ) from exc
    return tuple(inspected)


def _source_record(source: RefreshedSource) -> dict[str, object]:
    return {
        "manifest_path": str(source.manifest_path),
        "registry_commit": source.registry_commit,
        "source_sha256": source.source_sha256,
        "retrieved_at": source.retrieved_at,
        "source_age_days": source.source_age_days,
        "ingestion_receipt_path": str(source.ingestion_receipt_path),
        "curation_receipt_path": str(source.curation_receipt_path),
        "objects_published": source.objects_published,
        "objects_reused": source.objects_reused,
        "bytes_verified": source.bytes_verified,
        "parquet_published": source.parquet_published,
        "parquet_reused": source.parquet_reused,
        "source_rows": source.source_rows,
        "rows_with_pay": source.rows_with_pay,
        "rows_without_pay": source.rows_without_pay,
        "silver_rows": source.silver_rows,
        "geography_year_rows": source.geography_year_rows,
        "local_authority_yoy_rows": source.local_authority_yoy_rows,
    }


def _write_audit(path: Path, document: dict[str, Any]) -> None:
    """Publish one complete success/failure audit by atomic same-dir rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".ons_pay_refresh_", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _metrics(sources: list[RefreshedSource]) -> dict[str, int]:
    """Aggregates count rows across *vintages*, not unique source observations."""
    return {
        "vintages_completed": len(sources),
        "objects_published": sum(item.objects_published for item in sources),
        "objects_reused": sum(item.objects_reused for item in sources),
        "bytes_verified": sum(item.bytes_verified for item in sources),
        "parquet_published": sum(item.parquet_published for item in sources),
        "parquet_reused": sum(item.parquet_reused for item in sources),
        "source_rows": sum(item.source_rows for item in sources),
        "rows_with_pay": sum(item.rows_with_pay for item in sources),
        "rows_without_pay": sum(item.rows_without_pay for item in sources),
        "max_source_age_days_observed": max((item.source_age_days for item in sources), default=0),
    }


def refresh_ons_pay(
    *,
    sources: Sequence[RefreshSource],
    store: BlobStore,
    output_root: Path,
    max_source_age_days: int | None = None,
    as_of: date | None = None,
) -> RefreshResult:
    """Verify, ingest, curate and audit one or more pinned ONS snapshots.

    A partial failure does not undo prior published immutable objects. Every
    rerun rechecks them and reuses matching bytes. An audit records the last
    completed source and failing phase, making backfills safely resumable.
    """
    started = datetime.now(UTC)
    run_id = uuid4().hex
    reference_date = as_of if as_of is not None else started.date()
    output_root = output_root.resolve()
    if any(output_root.is_relative_to(source.registry_root.resolve()) for source in sources):
        # Do not write even a *failure* audit into the canonical source checkout.
        raise RefreshError("output_root must be outside each canonical registry checkout")
    receipt_path = output_root / "manifests" / f"ons_pay_refresh_{run_id}.json"
    completed: list[RefreshedSource] = []
    phase = "preflight"
    source_index: int | None = None
    active_ingestion_receipt: Path | None = None
    try:
        planned = plan_ons_pay_refresh(
            sources, max_source_age_days=max_source_age_days, as_of=reference_date
        )
        for index, item in enumerate(planned):
            source_index = index
            phase = "ingestion"
            active_ingestion_receipt = None
            ingested = ingest_handoff(
                manifest_path=item.source.manifest_path,
                registry_root=item.source.registry_root,
                store=store,
                receipt_dir=output_root / "manifests",
            )
            active_ingestion_receipt = ingested.receipt_path
            phase = "curation"
            curated = curate_ons_pay(
                input_receipt=ingested.receipt_path, store=store, output_root=output_root
            )
            by_name = {artifact.path.name: artifact for artifact in curated.outputs}
            completed.append(
                RefreshedSource(
                    manifest_path=item.source.manifest_path.resolve(),
                    registry_commit=item.registry_commit,
                    source_sha256=item.source_sha256,
                    retrieved_at=item.retrieved_at,
                    source_age_days=item.age_days,
                    ingestion_receipt_path=ingested.receipt_path,
                    curation_receipt_path=curated.receipt_path,
                    objects_published=ingested.files_published,
                    objects_reused=ingested.files_reused,
                    bytes_verified=ingested.bytes_verified,
                    parquet_published=sum(not artifact.reused for artifact in curated.outputs),
                    parquet_reused=sum(artifact.reused for artifact in curated.outputs),
                    source_rows=curated.source_rows,
                    rows_with_pay=curated.rows_with_pay,
                    rows_without_pay=curated.rows_without_pay,
                    silver_rows=by_name["observations.parquet"].rows,
                    geography_year_rows=by_name["geography_year.parquet"].rows,
                    local_authority_yoy_rows=by_name["local_authority_yoy.parquet"].rows,
                )
            )
    except Exception as exc:
        if phase == "preflight" and isinstance(exc, RefreshError):
            source_index = exc.source_index
        failure: dict[str, object] = {
            "phase": phase,
            "source_index": source_index,
            "error_type": type(exc).__name__,
            "message": str(exc),
        }
        if active_ingestion_receipt is not None and phase == "curation":
            failure["ingestion_receipt_path"] = str(active_ingestion_receipt)
        _write_audit(
            receipt_path,
            _audit_document(
                run_id, started, "failed", completed, max_source_age_days, reference_date, failure
            ),
        )
        raise RefreshError(
            f"ONS pay refresh failed in {phase}; audit: {receipt_path}",
            receipt_path=receipt_path,
            source_index=source_index,
        ) from exc

    _write_audit(
        receipt_path,
        _audit_document(
            run_id, started, "success", completed, max_source_age_days, reference_date, None
        ),
    )
    return RefreshResult(run_id, receipt_path, tuple(completed))


def _audit_document(
    run_id: str,
    started: datetime,
    status: str,
    completed: list[RefreshedSource],
    max_source_age_days: int | None,
    reference_date: date,
    failure: dict[str, object] | None,
) -> dict[str, Any]:
    finished = datetime.now(UTC)
    return {
        "pipeline_name": "ons_pay_refresh",
        "schema_version": 1,
        "model_version": MODEL_VERSION,
        "run_id": run_id,
        "status": status,
        "started_at": started.isoformat(),
        "created_at": finished.isoformat(),
        "duration_seconds": (finished - started).total_seconds(),
        "policy": {
            "max_source_age_days": max_source_age_days,
            "as_of_utc_date": reference_date.isoformat(),
        },
        "metrics": _metrics(completed),
        "sources": [_source_record(item) for item in completed],
        "failure": failure,
    }
