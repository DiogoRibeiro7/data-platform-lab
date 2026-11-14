"""Read-only verification of local raw objects from completed ONS load evidence."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from data_platform_lab.orchestration.ons_pay_refresh import parse_retrieved_date
from data_platform_lab.orchestration.ons_pay_transform import (
    TransformError,
    TransformSource,
    plan_ons_pay_transform,
)
from data_platform_lab.storage.local import normalize_key

_CHUNK_BYTES = 64 * 1024
Integrity = Literal[
    "verified", "missing", "unreadable", "unsafe", "size_mismatch", "checksum_mismatch"
]


class LoadVerificationError(ValueError):
    """Input policy or load evidence cannot support a complete verification report."""


@dataclass(frozen=True, slots=True)
class LoadObjectCheck:
    """One raw source's integrity and independent source-age result."""

    registry_commit: str
    source_sha256: str
    storage_key: str
    ingestion_receipt_path: str
    ingestion_receipt_sha256: str
    retrieved_at: str
    source_age_days: int
    freshness_passed: bool | None
    expected_size_bytes: int
    stored_size_bytes: int | None
    bytes_read: int
    observed_sha256: str | None
    integrity: Integrity


@dataclass(frozen=True, slots=True)
class LoadVerificationReport:
    """A point-in-time comparison; this is not a new ingestion or curation audit."""

    schema_version: int
    status: Literal["healthy", "unhealthy"]
    writes: bool
    checked_at_utc: str
    load_audit_path: str
    load_audit_sha256: str
    load_run_id: str
    load_job_sha256: str | None
    object_root: str
    max_source_age_days: int | None
    max_object_bytes: int
    max_total_bytes: int
    objects_expected: int
    objects_verified: int
    bytes_verified: int
    sources: tuple[LoadObjectCheck, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-compatible report without filesystem writes."""
        return asdict(self)


def _check_object(
    root: Path, item: TransformSource, age: int, max_age: int | None
) -> LoadObjectCheck:
    source = item.source
    path = root / normalize_key(source.storage_key)
    stored_size = None
    bytes_read = 0
    observed_sha = None
    integrity: Integrity = "unreadable"
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or path.resolve(strict=True) != path:
            integrity = "unsafe"
        else:
            # No BlobStore constructor: opening a local store would create a missing root.
            # Nonblocking/no-follow flags also prevent a swapped FIFO or leaf symlink
            # from blocking this inspection where the OS supports those flags.
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
            with os.fdopen(os.open(path, flags), "rb") as stream:
                opened = os.fstat(stream.fileno())
                if not stat.S_ISREG(opened.st_mode):
                    integrity = "unsafe"
                else:
                    stored_size = opened.st_size
                    if stored_size != source.size_bytes:
                        integrity = "size_mismatch"
                    else:
                        digest = hashlib.sha256()
                        # One overflow probe detects growth after the size check.
                        while chunk := stream.read(
                            min(_CHUNK_BYTES, source.size_bytes - bytes_read + 1)
                        ):
                            bytes_read += len(chunk)
                            if bytes_read > source.size_bytes:
                                break
                            digest.update(chunk)
                        if bytes_read != source.size_bytes:
                            integrity = "size_mismatch"
                        else:
                            observed_sha = digest.hexdigest()
                            integrity = (
                                "verified"
                                if observed_sha == source.source_sha256
                                else "checksum_mismatch"
                            )
    except FileNotFoundError:
        integrity = "missing"
    except (OSError, ValueError, RuntimeError):
        # File paths, exception strings and rows are not diagnostic payloads.
        integrity = "unreadable"
    return LoadObjectCheck(
        registry_commit=source.registry_commit,
        source_sha256=source.source_sha256,
        storage_key=source.storage_key,
        ingestion_receipt_path=str(item.receipt_path),
        ingestion_receipt_sha256=item.receipt_sha256,
        retrieved_at=source.source["retrieved_at"],
        source_age_days=age,
        freshness_passed=age <= max_age if max_age is not None else None,
        expected_size_bytes=source.size_bytes,
        stored_size_bytes=stored_size,
        bytes_read=bytes_read,
        observed_sha256=observed_sha,
        integrity=integrity,
    )


def verify_ons_pay_load(
    *,
    load_audit: Path,
    object_root: Path,
    expected_load_sha256: str | None = None,
    max_source_age_days: int | None = None,
    max_object_bytes: int = 16 * 1024 * 1024,
    max_total_bytes: int = 64 * 1024 * 1024,
    now: datetime | None = None,
) -> LoadVerificationReport:
    """Check all local raw sources without fetching, repairing, locking or writing.

    Reuse transformation's whole-batch receipt preflight. Byte budgets bound the
    expected raw bytes before any object is opened; reads use at most one additional
    probe byte per object. Duplicate content in different vintages is checked and
    counted per source. Filesystem contents are not an atomic snapshot.
    """
    try:
        if any(
            type(value) is not int or value <= 0 for value in (max_object_bytes, max_total_bytes)
        ):
            raise LoadVerificationError("Verification byte limits must be positive integers.")
        if max_source_age_days is not None and (
            type(max_source_age_days) is not int or max_source_age_days < 0
        ):
            raise LoadVerificationError("Source age limit must be a non-negative integer.")
        checked_at = now if now is not None else datetime.now(UTC)
        if checked_at.tzinfo is None or checked_at.utcoffset() is None:
            raise LoadVerificationError("Verification time must carry a timezone.")
        checked_at = checked_at.astimezone(UTC)
        plan = plan_ons_pay_transform(
            load_audit=load_audit, expected_load_sha256=expected_load_sha256
        )
        sizes = [item.source.size_bytes for item in plan.sources]
        if any(size > max_object_bytes for size in sizes) or sum(sizes) > max_total_bytes:
            raise LoadVerificationError("Expected raw bytes exceed the verification limits.")
        ages = [
            (checked_at.date() - parse_retrieved_date(item.source.source["retrieved_at"])).days
            for item in plan.sources
        ]
        if any(age < 0 for age in ages):
            raise LoadVerificationError("Source retrieval date is in the future.")
        root = Path(os.path.abspath(object_root.expanduser()))
        if root.resolve() != root or root.is_symlink() or (root.exists() and not root.is_dir()):
            raise LoadVerificationError("Object root must be a local directory without symlinks.")
    except LoadVerificationError:
        raise
    except (TransformError, OSError, ValueError, TypeError, RuntimeError, OverflowError):
        raise LoadVerificationError(
            "Cannot validate verification arguments or load evidence."
        ) from None

    checks = tuple(
        _check_object(root, item, age, max_source_age_days)
        for item, age in zip(plan.sources, ages, strict=True)
    )
    healthy = all(
        item.integrity == "verified" and item.freshness_passed is not False for item in checks
    )
    return LoadVerificationReport(
        schema_version=1,
        status="healthy" if healthy else "unhealthy",
        writes=False,
        checked_at_utc=checked_at.isoformat(),
        load_audit_path=str(plan.load_audit_path),
        load_audit_sha256=plan.load_audit_sha256,
        load_run_id=plan.load_run_id,
        load_job_sha256=plan.load_job_sha256,
        object_root=str(root),
        max_source_age_days=max_source_age_days,
        max_object_bytes=max_object_bytes,
        max_total_bytes=max_total_bytes,
        objects_expected=len(checks),
        objects_verified=sum(item.integrity == "verified" for item in checks),
        bytes_verified=sum(
            item.expected_size_bytes for item in checks if item.integrity == "verified"
        ),
        sources=checks,
    )
