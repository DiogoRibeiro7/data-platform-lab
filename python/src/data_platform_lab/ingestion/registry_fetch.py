"""Explicit, bounded HTTPS extraction of approved canonical registry objects."""

from __future__ import annotations

import json
import math
import re
import tempfile
from dataclasses import dataclass
from http.client import HTTPException
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, Request, build_opener

from data_platform_lab.ingestion.registry_handoff import (
    CanonicalObject,
    HandoffError,
    HandoffPlan,
    IngestionResult,
    _ingest_verified_handoff,
    _verify_manifest_bytes,
    _verify_source_bytes,
    parse_handoff,
)
from data_platform_lab.storage import BlobStore

_MANIFEST_BYTES = 1024 * 1024
_MAX_OBJECTS = 256
_CHUNK_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class RegistryFetchOptions:
    """Independently selected source identity and explicit extraction budgets."""

    repository: str
    commit: str
    dataset_id: str
    timeout_seconds: float = 30
    max_object_bytes: int = 16 * 1024 * 1024
    max_total_bytes: int = 64 * 1024 * 1024


def _prepare_fetch(manifest_path: Path, options: RegistryFetchOptions) -> tuple[bytes, HandoffPlan]:
    """Validate configuration and the entire plan before opening a connection."""
    if (
        not isinstance(options.repository, str)
        or re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", options.repository) is None
        or any(part in {".", ".."} for part in options.repository.split("/"))
        or not isinstance(options.commit, str)
        or re.fullmatch(r"[0-9a-f]{40}", options.commit) is None
        or not isinstance(options.dataset_id, str)
        or re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", options.dataset_id) is None
    ):
        raise HandoffError("Fetch requires a valid repository, full commit and dataset ID.")
    if (
        type(options.timeout_seconds) not in (int, float)
        or not 0 < options.timeout_seconds <= 300
        or not math.isfinite(options.timeout_seconds)
    ):
        raise HandoffError("Fetch timeout must be positive finite seconds, at most 300.")
    if any(
        type(value) is not int or value <= 0
        for value in (options.max_object_bytes, options.max_total_bytes)
    ):
        raise HandoffError("Fetch byte limits must be positive integers.")
    try:
        with manifest_path.open("rb") as handle:
            raw = handle.read(_MANIFEST_BYTES + 1)
        if len(raw) > _MANIFEST_BYTES:
            raise HandoffError("Fetch manifest exceeds the 1 MiB limit.")
        plan = parse_handoff(json.loads(raw))
    except (OSError, UnicodeError, ValueError, TypeError, RecursionError) as exc:
        if isinstance(exc, HandoffError):
            raise
        raise HandoffError("Cannot read a valid fetch manifest.") from None
    if (plan.repository, plan.commit, plan.dataset_id) != (
        options.repository,
        options.commit,
        options.dataset_id,
    ):
        raise HandoffError("Handoff differs from the independently selected source identity.")
    if len(plan.objects) > _MAX_OBJECTS:
        raise HandoffError("Fetch manifest exceeds the 256 object limit.")
    sizes = [item.size_bytes for item in plan.objects if item.size_bytes is not None]
    if (
        any(size > options.max_object_bytes for size in sizes)
        or sum(sizes) > options.max_total_bytes
    ):
        raise HandoffError("Declared source sizes exceed the fetch byte limits.")
    return raw, plan


def plan_registry_fetch(*, manifest_path: Path, options: RegistryFetchOptions) -> HandoffPlan:
    """Inspect a bounded pinned handoff without network access or filesystem writes."""
    return _prepare_fetch(manifest_path, options)[1]


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        """Keep retrieval bound to the exact approved URL, including its Git commit."""
        return None


class _HttpReader:
    """One run's bounded downloader; partial responses never reach the BlobStore."""

    def __init__(self, options: RegistryFetchOptions) -> None:
        self.options = options
        self.downloaded = 0
        self.opener = build_opener(_NoRedirect())

    def __call__(self, item: CanonicalObject) -> bytes:
        limit = min(self.options.max_object_bytes, self.options.max_total_bytes - self.downloaded)
        if item.size_bytes is not None and item.size_bytes > limit:
            raise HandoffError("Source exceeds the remaining fetch byte budget.")
        request = Request(
            item.source_uri,
            headers={
                "Accept-Encoding": "identity",
                "User-Agent": "data-platform-lab/registry-fetch",
            },
            method="GET",
        )
        try:
            with self.opener.open(request, timeout=self.options.timeout_seconds) as response:
                if response.status != 200 or response.geturl() != item.source_uri:
                    raise HandoffError(
                        "Fetch requires an unchanged URL and a complete HTTP 200 response."
                    )
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise HandoffError(
                        "Encoded HTTP responses are not accepted for pinned source bytes."
                    )
                length = response.headers.get("Content-Length")
                declared = None
                if length is not None:
                    if not re.fullmatch(r"[0-9]+", length):
                        raise HandoffError("Invalid HTTP response byte count.")
                    declared = int(length)
                    if declared > limit or (
                        item.size_bytes is not None and declared != item.size_bytes
                    ):
                        raise HandoffError(
                            "HTTP response byte count differs from the source or budget."
                        )
                size = 0
                with tempfile.TemporaryFile(mode="w+b") as temporary:
                    while True:
                        chunk = response.read(min(_CHUNK_BYTES, limit - size + 1))
                        if not chunk:
                            break
                        size += len(chunk)
                        self.downloaded += len(chunk)
                        if size > limit:
                            raise HandoffError("Downloaded source exceeds the fetch byte limits.")
                        temporary.write(chunk)
                    if declared is not None and size != declared:
                        raise HandoffError("HTTP response ended before its declared byte count.")
                    temporary.seek(0)
                    payload = temporary.read()
        except HTTPError as exc:
            exc.close()
            raise HandoffError(
                "Registry HTTP fetch failed; no redirects or automatic retries are allowed."
            ) from None
        except (OSError, URLError, HTTPException, ValueError) as exc:
            if isinstance(exc, HandoffError):
                raise
            raise HandoffError("Registry source transfer failed.") from None
        _verify_source_bytes(item, payload)
        return payload


def ingest_registry_fetch(
    *,
    manifest_path: Path,
    options: RegistryFetchOptions,
    store: BlobStore,
    receipt_dir: Path,
    expected_manifest_sha256: str | None = None,
) -> IngestionResult:
    """Fetch declared HTTPS objects, verify them, and load through the existing contract.

    Only immutable raw.githubusercontent.com URLs accepted by parse_handoff are
    fetched. Operators must trust their manifest and independent source pins.
    An optional expected manifest digest binds execution to a prior preflight.
    No checkout verification or authenticity of upstream attribution is claimed.
    """
    raw, plan = _prepare_fetch(manifest_path, options)
    _verify_manifest_bytes(raw, expected_manifest_sha256)
    return _ingest_verified_handoff(
        plan=plan,
        raw_manifest=raw,
        read_source=_HttpReader(options),
        store=store,
        receipt_dir=receipt_dir,
        source_transport="https",
    )
