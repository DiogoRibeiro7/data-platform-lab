"""Explicit, bounded HTTPS extraction of approved canonical registry objects."""

from __future__ import annotations

import json
import math
import re
import socket
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from http.client import HTTPException, IncompleteRead, RemoteDisconnected
from pathlib import Path
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, OpenerDirector, Request, build_opener

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
_RETRY_STATUS = {408, 429, 500, 502, 503, 504}
_MAX_RETRY_DELAY = 30


@dataclass(frozen=True, slots=True)
class RegistryFetchOptions:
    """Independently selected source identity and explicit extraction budgets."""

    repository: str
    commit: str
    dataset_id: str
    timeout_seconds: float = 30
    max_object_bytes: int = 16 * 1024 * 1024
    max_total_bytes: int = 64 * 1024 * 1024
    max_attempts: int = 1
    retry_delay_seconds: float = 1
    reuse_verified: bool = False


def _validate_fetch_options(options: RegistryFetchOptions) -> None:
    """Validate source identity and transfer policy without reading files or connecting."""
    if type(options.reuse_verified) is not bool:
        raise HandoffError("Fetch reuse_verified must be a boolean.")
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
    if type(options.max_attempts) is not int or not 1 <= options.max_attempts <= 5:
        raise HandoffError("Fetch max attempts must be an integer from 1 to 5.")
    if (
        type(options.retry_delay_seconds) not in (int, float)
        or not 0 < options.retry_delay_seconds <= _MAX_RETRY_DELAY
        or not math.isfinite(options.retry_delay_seconds)
    ):
        raise HandoffError("Fetch retry delay must be positive finite seconds, at most 30.")


def _prepare_fetch(manifest_path: Path, options: RegistryFetchOptions) -> tuple[bytes, HandoffPlan]:
    """Validate configuration and the entire plan before opening a connection."""
    _validate_fetch_options(options)
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


def _transient_transfer_error(exc: object) -> bool:
    """Retry known temporary transport failures, never TLS or local file errors."""
    reason = exc.reason if isinstance(exc, URLError) else exc
    return isinstance(
        reason, (TimeoutError, ConnectionError, IncompleteRead, RemoteDisconnected)
    ) or (isinstance(reason, socket.gaierror) and reason.errno == socket.EAI_AGAIN)


def _retry_delay(header: str | None, base: float, attempt: int) -> float:
    """Respect a server's minimum delay without exceeding the local wait cap."""
    delay = min(base * (1 << attempt), _MAX_RETRY_DELAY)
    if header is None:
        return delay
    value = header.strip()
    if re.fullmatch(r"[0-9]+", value):
        # Bound conversion as well as sleep, including excessively long headers.
        digits = value.lstrip("0") or "0"
        if len(digits) > 2 or int(digits) > _MAX_RETRY_DELAY:
            raise HandoffError("Server retry delay exceeds the 30 second wait limit.")
        server_delay = float(digits)
    else:
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                return delay
            server_delay = max(0, (when - datetime.now(UTC)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return delay
    if server_delay > _MAX_RETRY_DELAY:
        raise HandoffError("Server retry delay exceeds the 30 second wait limit.")
    return max(delay, server_delay)


class _HttpReader:
    """One run's bounded downloader; partial responses never reach the BlobStore."""

    def __init__(self, options: RegistryFetchOptions) -> None:
        self.options = options
        self.statistics = {"attempts": 0, "retries": 0, "downloaded_bytes": 0}
        # An all-cached run must not even construct an HTTP transport.
        self.opener: OpenerDirector | None = None

    def __call__(self, item: CanonicalObject) -> bytes:
        for attempt in range(self.options.max_attempts):
            # A retry never replenishes bytes consumed by a failed attempt.
            limit = self._remaining_limit(item)
            self.statistics["attempts"] += 1
            self.statistics["retries"] += int(attempt > 0)
            retry_after = None
            try:
                return self._read_once(item, limit)
            except HTTPError as exc:
                retryable = exc.code in _RETRY_STATUS and exc.geturl() == item.source_uri
                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                exc.close()
                message = "Registry HTTP fetch failed."
            except (OSError, HTTPException, ValueError) as exc:
                if isinstance(exc, HandoffError):
                    raise
                retryable = _transient_transfer_error(exc)
                message = "Registry source transfer failed."
            if not retryable or attempt + 1 == self.options.max_attempts:
                raise HandoffError(message) from None
            self._remaining_limit(item)
            sleep(_retry_delay(retry_after, self.options.retry_delay_seconds, attempt))
        raise AssertionError("validated fetch attempts must be positive")

    def _remaining_limit(self, item: CanonicalObject) -> int:
        remaining = (
            self.options.max_total_bytes
            - self.statistics["downloaded_bytes"]
            - self.statistics.get("cached_bytes", 0)
        )
        limit = min(self.options.max_object_bytes, remaining)
        if remaining <= 0 and item.size_bytes != 0:
            raise HandoffError("Source exceeds the remaining fetch byte budget.")
        if item.size_bytes is not None and item.size_bytes > limit:
            raise HandoffError("Source exceeds the remaining fetch byte budget.")
        return limit

    def _read_once(self, item: CanonicalObject, limit: int) -> bytes:
        """Discard partial bodies; only a complete verified attempt reaches storage."""
        if self.opener is None:
            self.opener = build_opener(_NoRedirect())
        request = Request(
            item.source_uri,
            headers={
                "Accept-Encoding": "identity",
                "User-Agent": "data-platform-lab/registry-fetch",
            },
            method="GET",
        )
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
                    try:
                        chunk = response.read(min(_CHUNK_BYTES, limit - size + 1))
                    except IncompleteRead as exc:
                        self.statistics["downloaded_bytes"] += len(exc.partial)
                        if size + len(exc.partial) > limit:
                            raise HandoffError(
                                "Downloaded source exceeds the fetch byte limits."
                            ) from None
                        raise
                    if not chunk:
                        break
                    size += len(chunk)
                    self.statistics["downloaded_bytes"] += len(chunk)
                    if size > limit:
                        raise HandoffError("Downloaded source exceeds the fetch byte limits.")
                    temporary.write(chunk)
                if declared is not None and size > declared:
                    raise HandoffError("HTTP body exceeds its declared byte count.")
                if declared is not None and size < declared:
                    raise IncompleteRead(b"", declared - size)
                temporary.seek(0)
                payload = temporary.read()
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
    Explicit reuse_verified checks existing store bytes before HTTPS fallback for
    missing objects. Corrupt/unreadable objects fail without repair or download.
    """
    raw, plan = _prepare_fetch(manifest_path, options)
    _verify_manifest_bytes(raw, expected_manifest_sha256)
    reader = _HttpReader(options)
    object_transports: dict[str, str] = {}
    if options.reuse_verified:
        reader.statistics.update(cached_objects=0, cached_bytes=0)

    def read_source(item: CanonicalObject) -> bytes:
        if options.reuse_verified and store.exists(item.storage_key):
            limit = reader._remaining_limit(item)
            payload = store.get_bytes(item.storage_key)
            if len(payload) > limit:
                raise HandoffError("Stored source exceeds the fetch byte limits.")
            _verify_source_bytes(item, payload)
            reader.statistics["cached_objects"] += 1
            reader.statistics["cached_bytes"] += len(payload)
            object_transports[item.storage_key] = "verified_store"
            return payload
        payload = reader(item)
        object_transports[item.storage_key] = "https"
        return payload

    return _ingest_verified_handoff(
        plan=plan,
        raw_manifest=raw,
        read_source=read_source,
        store=store,
        receipt_dir=receipt_dir,
        source_transport="https_or_verified_store" if options.reuse_verified else "https",
        fetch_statistics=reader.statistics,
        object_transports=object_transports if options.reuse_verified else None,
    )
