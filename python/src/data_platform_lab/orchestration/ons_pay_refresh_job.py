"""Read bounded, explicit ONS refresh jobs using the existing orchestration contracts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from data_platform_lab.curation.ons_pay import DATASET_ID
from data_platform_lab.ingestion.registry_fetch import (
    RegistryFetchOptions,
    _validate_fetch_options,
)
from data_platform_lab.ingestion.registry_handoff import HandoffError
from data_platform_lab.orchestration.ons_pay_refresh import (
    RefreshInput,
    RefreshSource,
    RemoteRefreshSource,
)

_MAX_JOB_BYTES = 256 * 1024
_MAX_SOURCES = 256
_FETCH_KEYS = {
    "timeout_seconds",
    "max_object_bytes",
    "max_source_bytes",
    "max_attempts",
    "retry_delay_seconds",
}


class RefreshJobError(ValueError):
    """A saved job is invalid; messages never echo its contents."""


@dataclass(frozen=True, slots=True)
class RefreshJob:
    """One in-memory configuration snapshot with paths resolved against its file."""

    sources: tuple[RefreshInput, ...]
    object_root: Path
    output_root: Path
    max_source_age_days: int | None
    sha256: str


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject ambiguous duplicate fields at every nesting level."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise RefreshJobError("Refresh job contains a duplicate field.")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise RefreshJobError("Refresh job contains a non-finite JSON number.")


def _fields(value: Any, required: set[str], optional: set[str]) -> dict[str, Any]:
    if (
        not isinstance(value, dict)
        or not required <= value.keys()
        or value.keys() - required - optional
    ):
        raise RefreshJobError("Refresh job has missing or unsupported fields.")
    return value


def _path(value: Any, directory: Path) -> Path:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise RefreshJobError("Refresh job paths must be non-empty strings without null bytes.")
    return (directory / value).resolve()


def load_refresh_job(path: Path) -> RefreshJob:
    """Parse a v1 UTF-8 job without opening handoffs, stores or network connections.

    A job must be trusted as execution configuration. No command execution,
    credential fields, variable expansion or floating HTTPS source selection is supported.
    Manifest pins are verified by the normal whole-batch refresh preflight.
    """
    try:
        with path.open("rb") as handle:
            raw = handle.read(_MAX_JOB_BYTES + 1)
        if len(raw) > _MAX_JOB_BYTES:
            raise RefreshJobError("Refresh job exceeds the 256 KiB limit.")
        document = _fields(
            json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            ),
            {"schema_version", "sources", "object_root", "output_root"},
            {"fetch", "max_source_age_days"},
        )
        if type(document["schema_version"]) is not int or document["schema_version"] != 1:
            raise RefreshJobError("Refresh job requires schema_version 1.")
        entries = document["sources"]
        if not isinstance(entries, list) or not 1 <= len(entries) <= _MAX_SOURCES:
            raise RefreshJobError("Refresh job requires between 1 and 256 sources.")
        max_age = document.get("max_source_age_days")
        if max_age is not None and (type(max_age) is not int or max_age < 0):
            raise RefreshJobError("Refresh job source age must be a non-negative integer or null.")
        fetch = _fields(document.get("fetch", {}), set(), _FETCH_KEYS)
        # Keep defaults and policy validation shared with direct HTTPS ingestion.
        fetch_options = {
            "max_total_bytes" if key == "max_source_bytes" else key: value
            for key, value in fetch.items()
        }
        directory = path.resolve().parent
        sources: list[RefreshInput] = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise RefreshJobError("Refresh job source must be an object.")
            transport = entry.get("transport")
            common = {"transport", "manifest_path", "manifest_sha256"}
            if transport == "https":
                _fields(entry, common | {"registry_commit"}, set())
            elif transport == "checkout":
                _fields(entry, common | {"registry_root"}, set())
            else:
                raise RefreshJobError("Refresh job source transport must be https or checkout.")
            digest = entry["manifest_sha256"]
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise RefreshJobError(
                    "Refresh job requires a lowercase handoff SHA-256 per source."
                )
            manifest_path = _path(entry["manifest_path"], directory)
            if transport == "https":
                options = RegistryFetchOptions(
                    repository="DiogoRibeiro7/data",
                    commit=entry["registry_commit"],
                    dataset_id=DATASET_ID,
                    **fetch_options,
                )
                _validate_fetch_options(options)
                sources.append(RemoteRefreshSource(manifest_path, options, digest))
            else:
                sources.append(
                    RefreshSource(manifest_path, _path(entry["registry_root"], directory), digest)
                )
        if "fetch" in document and not any(isinstance(s, RemoteRefreshSource) for s in sources):
            raise RefreshJobError("Refresh job fetch settings require an HTTPS source.")
        return RefreshJob(
            sources=tuple(sources),
            object_root=_path(document["object_root"], directory),
            output_root=_path(document["output_root"], directory),
            max_source_age_days=max_age,
            sha256=hashlib.sha256(raw).hexdigest(),
        )
    except RefreshJobError:
        raise
    except HandoffError as exc:
        raise RefreshJobError(str(exc)) from None
    except (OSError, UnicodeError, ValueError, TypeError, RuntimeError, OverflowError):
        raise RefreshJobError("Cannot read a valid refresh job.") from None
