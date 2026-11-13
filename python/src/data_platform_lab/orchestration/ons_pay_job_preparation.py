"""Prepare pinned refresh configuration without executing ingestion or curation."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from data_platform_lab.orchestration.ons_pay_refresh import (
    RefreshInput,
    RefreshSource,
    RemoteRefreshSource,
    plan_ons_pay_refresh,
)
from data_platform_lab.orchestration.ons_pay_refresh_job import (
    _MAX_SOURCES,
    RefreshJob,
    RefreshJobError,
    _parse_refresh_job,
)


class RefreshJobWriteError(RefreshJobError):
    """A complete prepared job could not be published without replacement."""


@dataclass(frozen=True, slots=True)
class PreparedRefreshJob:
    """Exact configuration bytes and their intended location, ready for review."""

    path: Path
    content: bytes
    sha256: str


def _target_path(path: Path) -> Path:
    """Resolve the directory, preserving the final name for exclusive creation."""
    if not path.name:
        raise RefreshJobError("A saved job requires a file name.")
    return path.parent.resolve() / path.name


def _protect_checkouts(path: Path, job: RefreshJob) -> None:
    for source in job.sources:
        if isinstance(source, RefreshSource) and any(
            target.is_relative_to(source.registry_root)
            for target in (path, job.object_root, job.output_root)
        ):
            raise RefreshJobError(
                "Job and destination paths must remain outside canonical checkouts."
            )


def _relative_path(path: Path, directory: Path) -> str:
    """Prefer portable relative paths; distinct Windows drives require absolute paths."""
    resolved = path.resolve()
    try:
        return Path(os.path.relpath(resolved, directory)).as_posix()
    except ValueError:
        return resolved.as_posix()


def prepare_refresh_job(
    *,
    path: Path,
    sources: Sequence[RefreshInput],
    object_root: Path,
    output_root: Path,
    max_source_age_days: int | None = None,
    load_only: bool = False,
    as_of: date | None = None,
) -> PreparedRefreshJob:
    """Validate handoffs and construct a v1 job, without network or filesystem writes.

    HTTPS identities remain independently supplied by the caller. Hashes come
    from the existing whole-batch preflight snapshot, not a later manifest read.
    Source bytes, checkout commits and destination integrity are checked at execution.
    """
    try:
        target = _target_path(path)
        if target.exists() or target.is_symlink():
            raise RefreshJobError("Job destination already exists; choose a new file.")
        if not 1 <= len(sources) <= _MAX_SOURCES:
            raise RefreshJobError("Refresh job requires between 1 and 256 sources.")
        if type(load_only) is not bool:
            raise RefreshJobError("Refresh job load_only must be a boolean.")
        plan = plan_ons_pay_refresh(sources, max_source_age_days=max_source_age_days, as_of=as_of)
        entries: list[dict[str, Any]] = []
        shared_fetch: dict[str, int | float] | None = None
        for item in plan:
            source = item.source
            entry: dict[str, Any] = {
                "manifest_path": _relative_path(source.manifest_path, target.parent),
                "manifest_sha256": item.manifest_sha256,
            }
            if isinstance(source, RemoteRefreshSource):
                options = source.fetch_options
                fetch = {
                    "timeout_seconds": options.timeout_seconds,
                    "max_object_bytes": options.max_object_bytes,
                    "max_source_bytes": options.max_total_bytes,
                    "max_attempts": options.max_attempts,
                    "retry_delay_seconds": options.retry_delay_seconds,
                    **({"reuse_verified": True} if options.reuse_verified else {}),
                }
                if shared_fetch is not None and shared_fetch != fetch:
                    raise RefreshJobError("A saved job requires shared HTTPS fetch settings.")
                shared_fetch = fetch
                entry.update(transport="https", registry_commit=options.commit)
            else:
                entry.update(
                    transport="checkout",
                    registry_root=_relative_path(source.registry_root, target.parent),
                )
            entries.append(entry)
        document = {
            "schema_version": 1,
            "sources": entries,
            "object_root": _relative_path(object_root, target.parent),
            "output_root": _relative_path(output_root, target.parent),
            "max_source_age_days": max_source_age_days,
            "load_only": load_only,
            **({"fetch": shared_fetch} if shared_fetch is not None else {}),
        }
        content = (
            json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        ).encode("utf-8")
        job = _parse_refresh_job(target, content)
        _protect_checkouts(target, job)
        return PreparedRefreshJob(target, content, job.sha256)
    except RefreshJobError:
        raise
    except (OSError, ValueError, TypeError, RuntimeError, OverflowError):
        # Preflight and filesystem exceptions can contain supplied metadata or paths.
        raise RefreshJobError(
            "Cannot prepare a valid refresh job; check inputs and policies."
        ) from None


def save_refresh_job(prepared: PreparedRefreshJob) -> None:
    """Publish complete bytes exclusively; never truncate or replace an existing path.

    As with immutable platform evidence, a same-directory temporary file is
    flushed and linked into place. The local filesystem must support hard links.
    Ingestion still revalidates pinned handoffs when the saved job is executed.
    """
    temporary: Path | None = None
    try:
        target = _target_path(prepared.path)
        if (
            target != prepared.path
            or hashlib.sha256(prepared.content).hexdigest() != prepared.sha256
        ):
            raise RefreshJobError("Prepared job location or digest is inconsistent.")
        job = _parse_refresh_job(target, prepared.content)
        _protect_checkouts(target, job)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=target.parent, prefix=".ons-job-", suffix=".tmp", delete=False
        ) as output:
            temporary = Path(output.name)
            output.write(prepared.content)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, target)
    except RefreshJobError:
        raise
    except FileExistsError:
        raise RefreshJobWriteError("Job destination already exists; choose a new file.") from None
    except (OSError, ValueError, TypeError, RuntimeError):
        raise RefreshJobWriteError("Cannot save the prepared refresh job.") from None
    finally:
        if temporary is not None:
            with suppress(OSError):
                temporary.unlink(missing_ok=True)
