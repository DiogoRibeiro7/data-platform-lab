"""Read-only verification of stored raw objects from completed ONS load evidence."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO, Literal

from data_platform_lab.orchestration.ons_pay_refresh import parse_retrieved_date
from data_platform_lab.orchestration.ons_pay_transform import (
    TransformError,
    TransformSource,
    plan_ons_pay_transform,
)
from data_platform_lab.storage.config import S3Storage
from data_platform_lab.storage.local import normalize_key
from data_platform_lab.storage.s3 import S3BlobStore

_CHUNK_BYTES = 64 * 1024
Integrity = Literal[
    "verified", "missing", "unreadable", "unsafe", "size_mismatch", "checksum_mismatch"
]


class LoadVerificationError(ValueError):
    """Input policy or load evidence cannot support a complete verification report."""


class LoadVerificationStorageError(RuntimeError):
    """The configured object-store client could not be initialized."""


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
    object_root: str | None
    max_source_age_days: int | None
    max_object_bytes: int
    max_total_bytes: int
    objects_expected: int
    objects_verified: int
    bytes_verified: int
    sources: tuple[LoadObjectCheck, ...]
    object_store: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON-compatible report without filesystem writes."""
        result = asdict(self)
        # Preserve the existing local JSON contract exactly.
        result.pop("object_root" if self.object_store is not None else "object_store")
        return result


@dataclass(slots=True)
class _ReadCheck:
    """Retain read progress even when a body fails partway through inspection."""

    stored_size: int | None = None
    bytes_read: int = 0
    observed_sha: str | None = None
    integrity: Integrity = "unreadable"


def _hash_stream(stream: BinaryIO, item: TransformSource, check: _ReadCheck) -> None:
    """Hash bounded chunks only when the opened object's size matches its receipt."""
    expected = item.source.size_bytes
    if check.stored_size != expected:
        check.integrity = "size_mismatch"
        return
    digest = hashlib.sha256()
    while True:
        size = min(_CHUNK_BYTES, expected - check.bytes_read + 1)
        chunk = stream.read(size)
        if not isinstance(chunk, bytes) or len(chunk) > size:
            raise ValueError("Invalid bounded stream response.")
        if not chunk:
            break
        check.bytes_read += len(chunk)
        if check.bytes_read > expected:
            break
        digest.update(chunk)
    if check.bytes_read != expected:
        check.integrity = "size_mismatch"
    else:
        check.observed_sha = digest.hexdigest()
        check.integrity = (
            "verified" if check.observed_sha == item.source.source_sha256 else "checksum_mismatch"
        )


def _object_check(
    item: TransformSource, age: int, max_age: int | None, check: _ReadCheck
) -> LoadObjectCheck:
    """Attach evidence identity and independent freshness to one stored-object check."""
    source = item.source
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
        stored_size_bytes=check.stored_size,
        bytes_read=check.bytes_read,
        observed_sha256=check.observed_sha,
        integrity=check.integrity,
    )


def _check_object(
    root: Path, item: TransformSource, age: int, max_age: int | None
) -> LoadObjectCheck:
    source = item.source
    path = root / normalize_key(source.storage_key)
    check = _ReadCheck()
    try:
        metadata = path.lstat()
        if not stat.S_ISREG(metadata.st_mode) or path.resolve(strict=True) != path:
            check.integrity = "unsafe"
        else:
            # No BlobStore constructor: opening a local store would create a missing root.
            # Nonblocking/no-follow flags also prevent a swapped FIFO or leaf symlink
            # from blocking this inspection where the OS supports those flags.
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
            with os.fdopen(os.open(path, flags), "rb") as stream:
                opened = os.fstat(stream.fileno())
                if not stat.S_ISREG(opened.st_mode):
                    check.integrity = "unsafe"
                else:
                    check.stored_size = opened.st_size
                    _hash_stream(stream, item, check)
    except FileNotFoundError:
        check.integrity = "missing"
    except (OSError, ValueError, RuntimeError):
        # File paths, exception strings and rows are not diagnostic payloads.
        check.integrity = "unreadable"
    return _object_check(item, age, max_age, check)


def _check_s3_object(
    store: S3BlobStore, item: TransformSource, age: int, max_age: int | None
) -> LoadObjectCheck:
    """Read one S3 response; keep failures per source and never expose SDK diagnostics."""
    check = _ReadCheck()
    try:
        with store.open_reader(item.source.storage_key) as (size, stream):
            check.stored_size = size
            _hash_stream(stream, item, check)
    except FileNotFoundError:
        check.integrity = "missing"
    except Exception:
        check.integrity = "unreadable"
        check.observed_sha = None
    return _object_check(item, age, max_age, check)


def verify_ons_pay_load(
    *,
    load_audit: Path,
    object_root: Path | None = None,
    object_store: S3Storage | None = None,
    expected_load_sha256: str | None = None,
    max_source_age_days: int | None = None,
    max_object_bytes: int = 16 * 1024 * 1024,
    max_total_bytes: int = 64 * 1024 * 1024,
    now: datetime | None = None,
) -> LoadVerificationReport:
    """Check local or S3 raw sources without source fetching, repairing or writing.

    Reuse transformation's whole-batch receipt preflight. Byte budgets bound the
    expected raw bytes before any object is opened; reads use at most one additional
    probe byte per object. Duplicate content in different vintages is checked and
    counted per source. Stored contents are not an atomic batch snapshot. S3 client
    construction happens only after all evidence, ages and byte limits are validated.
    """
    try:
        if (object_root is None) == (object_store is None) or (
            object_store is not None and not isinstance(object_store, S3Storage)
        ):
            raise LoadVerificationError("Select exactly one local or S3 object store.")
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
        root = None
        if object_root is not None:
            root = Path(os.path.abspath(object_root.expanduser()))
            if root.resolve() != root or root.is_symlink() or (root.exists() and not root.is_dir()):
                raise LoadVerificationError(
                    "Object root must be a local directory without symlinks."
                )
    except LoadVerificationError:
        raise
    except (TransformError, OSError, ValueError, TypeError, RuntimeError, OverflowError):
        raise LoadVerificationError(
            "Cannot validate verification arguments or load evidence."
        ) from None

    if object_store is not None:
        try:
            store = object_store.open()
        except Exception:
            raise LoadVerificationStorageError("Unable to access verification storage.") from None
        checks = tuple(
            _check_s3_object(store, item, age, max_source_age_days)
            for item, age in zip(plan.sources, ages, strict=True)
        )
    else:
        assert root is not None
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
        object_root=str(root) if root is not None else None,
        object_store=object_store.to_dict() if object_store is not None else None,
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
