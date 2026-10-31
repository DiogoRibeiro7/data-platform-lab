"""Consume immutable canonical registry handoffs through the BlobStore contract.

The upstream registry defines *what* may be ingested; this consumer verifies
the pinned checkout and exact bytes before publishing them to a selected store.
No cloud provider, mutable table, or implicit network request is required.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from urllib.parse import quote
from uuid import uuid4

from data_platform_lab.storage import BlobStore, LocalBlobStore, S3BlobStore

_REPOSITORY = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_DATASET_ID = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


class HandoffError(ValueError):
    """Raised when a handoff, checkout, or destination fails integrity checks."""


@dataclass(frozen=True, slots=True)
class CanonicalObject:
    """A SHA-pinned source and its immutable storage address."""

    source_path: str
    source_uri: str
    sha256: str
    role: Literal["raw", "derived"]
    format: str
    storage_key: str
    size_bytes: int | None


@dataclass(frozen=True, slots=True)
class HandoffPlan:
    """Validated subset of the registry's v1 handoff contract."""

    dataset_id: str
    repository: str
    commit: str
    title: str
    lifecycle_status: str
    source_publisher: str
    source_url: str
    source_snapshot: str
    retrieved_at: str
    citation: str
    license_name: str
    license_url: str
    plan_locally_verified: bool
    objects: tuple[CanonicalObject, ...]


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Completed run statistics and durable receipt path."""

    run_id: str
    dataset_id: str
    files_published: int
    files_reused: int
    bytes_verified: int
    receipt_path: Path


def _mapping(value: object, context: str, required: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise HandoffError(f"{context} must be a JSON object")
    mapping = cast(dict[str, object], value)
    missing = required - mapping.keys()
    if missing:
        raise HandoffError(f"{context} missing fields: {', '.join(sorted(missing))}")
    return mapping


def _text(mapping: dict[str, object], field: str, context: str) -> str:
    value = mapping.get(field)
    if not isinstance(value, str) or not value.strip():
        raise HandoffError(f"{context}.{field} must be non-empty text")
    return value


def _canonical_path(path: str) -> bool:
    """Reject traversal and alternative spellings of a repository/object path."""
    pure = PurePosixPath(path)
    return (
        bool(pure.parts)
        and not pure.is_absolute()
        and "\\" not in path
        and not any(ord(char) < 32 or ord(char) == 127 for char in path)
        and ".." not in pure.parts
        and pure.as_posix() == path
    )


def parse_handoff(value: object) -> HandoffPlan:
    """Validate security-critical fields of a registry handoff v1 document.

    The upstream JSON schema remains authoritative; these runtime checks also
    bind the source URI and destination key to the same immutable identity.
    """
    manifest = _mapping(
        value,
        "handoff",
        {
            "manifest_version",
            "dataset_id",
            "title",
            "lifecycle_status",
            "registry",
            "source",
            "license",
            "citation",
            "local_bytes_verified",
            "objects",
        },
    )
    if type(manifest["manifest_version"]) is not int or manifest["manifest_version"] != 1:
        raise HandoffError("only handoff manifest_version 1 is supported")
    dataset_id = _text(manifest, "dataset_id", "handoff")
    if not _DATASET_ID.fullmatch(dataset_id):
        raise HandoffError("invalid canonical dataset ID")
    title = _text(manifest, "title", "handoff")
    if manifest["lifecycle_status"] not in {"active", "deprecated", "superseded"}:
        raise HandoffError("unrecognized lifecycle_status")
    if type(manifest["local_bytes_verified"]) is not bool:
        raise HandoffError("local_bytes_verified must be a boolean")

    registry = _mapping(manifest["registry"], "registry", {"repository", "commit"})
    repository = _text(registry, "repository", "registry")
    commit = _text(registry, "commit", "registry")
    if not _REPOSITORY.fullmatch(repository) or not _COMMIT.fullmatch(commit):
        raise HandoffError("registry must pin a GitHub owner/repo and full lowercase Git SHA")

    source = _mapping(
        manifest["source"], "source", {"publisher", "url", "snapshot", "retrieved_at"}
    )
    source_publisher = _text(source, "publisher", "source")
    source_url = _text(source, "url", "source")
    source_snapshot = _text(source, "snapshot", "source")
    retrieved_at = _text(source, "retrieved_at", "source")
    license_info = _mapping(manifest["license"], "license", {"name", "url", "redistribution"})
    if license_info["redistribution"] != "allowed":
        raise HandoffError("canonical object ingestion requires redistribution=allowed")
    license_name = _text(license_info, "name", "license")
    license_url = _text(license_info, "url", "license")
    citation = _text(manifest, "citation", "handoff")

    raw_objects = manifest["objects"]
    if not isinstance(raw_objects, list) or not raw_objects:
        raise HandoffError("handoff.objects must be a non-empty array")

    objects: list[CanonicalObject] = []
    seen_paths: set[str] = set()
    seen_keys: set[str] = set()
    for index, raw_object in enumerate(raw_objects):
        name = f"objects[{index}]"
        item = _mapping(
            raw_object,
            name,
            {"source_path", "source_uri", "sha256", "role", "format", "storage_key", "size_bytes"},
        )
        source_path = _text(item, "source_path", name)
        source_uri = _text(item, "source_uri", name)
        sha256 = _text(item, "sha256", name)
        role = _text(item, "role", name)
        storage_key = _text(item, "storage_key", name)
        format_name = _text(item, "format", name)
        if not _SHA256.fullmatch(sha256) or role not in {"raw", "derived"}:
            raise HandoffError(f"{name}: invalid SHA-256 or object role")
        prefix = f"datasets/{dataset_id}/{role}/"
        if not _canonical_path(source_path) or not source_path.startswith(prefix):
            raise HandoffError(f"{name}: source_path must be within its canonical dataset role")

        relative_path = source_path.removeprefix(f"datasets/{dataset_id}/")
        expected_key = f"canonical/{dataset_id}/{sha256}/{relative_path}"
        if storage_key != expected_key:
            raise HandoffError(f"{name}: storage_key does not match the content address")
        encoded_path = "/".join(quote(part, safe="") for part in PurePosixPath(source_path).parts)
        expected_uri = f"https://raw.githubusercontent.com/{repository}/{commit}/{encoded_path}"
        if source_uri != expected_uri:
            raise HandoffError(f"{name}: source_uri does not match the pinned source")

        size = item["size_bytes"]
        if size is not None and (type(size) is not int or size < 0):
            raise HandoffError(f"{name}: size_bytes must be a non-negative integer or null")
        if source_path in seen_paths or storage_key in seen_keys:
            raise HandoffError(f"{name}: duplicate source_path or storage_key")
        seen_paths.add(source_path)
        seen_keys.add(storage_key)
        objects.append(
            CanonicalObject(
                source_path=source_path,
                source_uri=source_uri,
                sha256=sha256,
                role=cast(Literal["raw", "derived"], role),
                format=format_name,
                storage_key=storage_key,
                size_bytes=size,
            )
        )

    if manifest.get("local_bytes_verified") is True and any(
        item.size_bytes is None for item in objects
    ):
        raise HandoffError("locally verified handoffs require size_bytes for each object")
    return HandoffPlan(
        dataset_id=dataset_id,
        repository=repository,
        commit=commit,
        title=title,
        lifecycle_status=manifest["lifecycle_status"],
        source_publisher=source_publisher,
        source_url=source_url,
        source_snapshot=source_snapshot,
        retrieved_at=retrieved_at,
        citation=citation,
        license_name=license_name,
        license_url=license_url,
        plan_locally_verified=manifest["local_bytes_verified"],
        objects=tuple(objects),
    )


def _verify_checkout(root: Path, expected_commit: str) -> Path:
    """Require a checkout rooted at the exact manifest commit (not floating main)."""
    root = root.resolve(strict=True)
    try:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--show-toplevel", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise HandoffError("registry root must be a readable Git checkout") from exc
    lines = result.stdout.splitlines()
    if len(lines) != 2 or Path(lines[0]).resolve() != root or lines[1] != expected_commit:
        raise HandoffError("registry checkout root/HEAD differs from the handoff's pinned commit")
    return root


def _destination_uri(store: BlobStore, key: str) -> str:
    if isinstance(store, LocalBlobStore):
        return (store.root / key).as_uri()
    if isinstance(store, S3BlobStore):
        prefix = f"{store.key_prefix}/" if store.key_prefix else ""
        return f"s3://{store.bucket}/{prefix}{key}"
    # Implementations of the BlobStore protocol can supply their own URI scheme.
    return f"blob:///{key}"


def _verify_committed_blob(root: Path, path: str, source: Path) -> None:
    """Reject dirty/untracked bytes even if a forged plan matches their SHA."""
    try:
        tracked = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", f"HEAD:{path}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        checked_out = subprocess.run(
            ["git", "-C", str(root), "hash-object", "--", str(source)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise HandoffError(f"source is not a readable pinned Git blob: {path}") from exc
    if tracked != checked_out:
        raise HandoffError(f"source differs from its pinned Git blob: {path}")


def ingest_handoff(
    *,
    manifest_path: Path,
    registry_root: Path,
    store: BlobStore,
    receipt_dir: Path,
) -> IngestionResult:
    """Ingest and verify approved objects, then atomically write a success receipt.

    Existing content-addressed objects are rechecked and reused. Corrupt
    destinations are never silently overwritten. A failed multi-file run may
    leave already verified objects, but never an erroneous success receipt;
    rerunning reconciles the previous successful writes.
    """
    raw_manifest = manifest_path.read_bytes()
    try:
        plan = parse_handoff(json.loads(raw_manifest))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HandoffError("handoff must be valid UTF-8 JSON") from exc
    root = _verify_checkout(registry_root, plan.commit)
    if receipt_dir.resolve().is_relative_to(root):
        raise HandoffError("receipt directory must be outside the canonical registry")
    if isinstance(store, LocalBlobStore) and store.root.is_relative_to(root):
        raise HandoffError("destination store must be outside the canonical registry")
    started_at = datetime.now(UTC)
    run_id = uuid4().hex
    published = 0
    reused = 0
    verified_bytes = 0
    records: list[dict[str, Any]] = []

    for item in plan.objects:
        try:
            source = (root / item.source_path).resolve(strict=True)
        except OSError as exc:
            raise HandoffError(f"unsafe or missing source: {item.source_path}") from exc
        if not source.is_relative_to(root) or not source.is_file():
            raise HandoffError(f"unsafe or missing source: {item.source_path}")
        payload = source.read_bytes()
        if hashlib.sha256(payload).hexdigest() != item.sha256:
            raise HandoffError(f"source checksum mismatch: {item.source_path}")
        if item.size_bytes is not None and item.size_bytes != len(payload):
            raise HandoffError(f"source byte count mismatch: {item.source_path}")
        _verify_committed_blob(root, item.source_path, source)
        verified_bytes += len(payload)

        if store.exists(item.storage_key):
            existing = store.get_bytes(item.storage_key)
            if hashlib.sha256(existing).hexdigest() != item.sha256:
                raise HandoffError(f"existing destination is corrupt: {item.storage_key}")
            status = "reused"
            reused += 1
        else:
            store.put_bytes(item.storage_key, payload)
            # Read after write so a success receipt asserts destination integrity.
            if hashlib.sha256(store.get_bytes(item.storage_key)).hexdigest() != item.sha256:
                raise HandoffError(f"destination verification failed: {item.storage_key}")
            status = "published"
            published += 1

        records.append(
            {
                "source_path": item.source_path,
                "source_uri": item.source_uri,
                "sha256": item.sha256,
                "role": item.role,
                "format": item.format,
                "size_bytes": len(payload),
                "storage_key": item.storage_key,
                "destination_uri": _destination_uri(store, item.storage_key),
                "status": status,
            }
        )

    finished_at = datetime.now(UTC)
    receipt: dict[str, Any] = {
        "pipeline_name": "registry_handoff_ingestion",
        "run_id": run_id,
        "created_at": finished_at.isoformat(),
        "started_at": started_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "status": "success",
        "manifest_version": 1,
        "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
        "dataset_id": plan.dataset_id,
        "title": plan.title,
        "lifecycle_status": plan.lifecycle_status,
        "plan_locally_verified": plan.plan_locally_verified,
        "registry": {"repository": plan.repository, "commit": plan.commit},
        "source": {
            "publisher": plan.source_publisher,
            "url": plan.source_url,
            "snapshot": plan.source_snapshot,
            "retrieved_at": plan.retrieved_at,
        },
        "license_name": plan.license_name,
        "license_url": plan.license_url,
        "citation": plan.citation,
        "files_published": published,
        "files_reused": reused,
        "bytes_verified": verified_bytes,
        "objects": records,
    }
    receipt_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = receipt_dir / f"registry_handoff_ingestion_{run_id}.json"
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=receipt_dir, prefix=".registry_receipt_", delete=False
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(receipt, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temporary_path.replace(receipt_path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    return IngestionResult(run_id, plan.dataset_id, published, reused, verified_bytes, receipt_path)
