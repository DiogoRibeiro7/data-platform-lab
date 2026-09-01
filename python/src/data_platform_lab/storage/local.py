"""Local filesystem implementation of the platform object-store boundary."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable


class StorageKeyError(ValueError):
    """Raised when a storage key is unsafe or invalid."""


@dataclass(frozen=True, slots=True)
class StoredObject:
    """Metadata for one object stored by a :class:`BlobStore`."""

    key: str
    size_bytes: int


@runtime_checkable
class BlobStore(Protocol):
    """Minimal object-storage contract used by platform workflows."""

    def put_bytes(self, key: str, payload: bytes) -> StoredObject:
        """Persist *payload* under *key* and return object metadata."""

    def get_bytes(self, key: str) -> bytes:
        """Return the bytes stored under *key*."""

    def exists(self, key: str) -> bool:
        """Return whether *key* exists."""

    def list_objects(self, prefix: str = "") -> list[StoredObject]:
        """List stored objects below *prefix* in deterministic key order."""


def normalize_key(key: str) -> str:
    """Validate and normalize a portable object key.

    Keys are POSIX-style relative paths. Absolute paths, empty paths, current
    directory markers, and parent traversal are rejected so callers cannot
    escape the configured storage root.
    """
    if not isinstance(key, str):
        raise TypeError("key must be a string")

    candidate = key.strip().replace("\\", "/")
    path = PurePosixPath(candidate)

    if not candidate or candidate in {".", "/"}:
        raise StorageKeyError("key must not be empty")
    if path.is_absolute():
        raise StorageKeyError("key must be relative")
    if any(part in {"", ".", ".."} for part in path.parts):
        raise StorageKeyError("key must not contain traversal components")

    return path.as_posix()


class LocalBlobStore:
    """Filesystem-backed :class:`BlobStore` for local development and tests."""

    def __init__(self, root: str | Path) -> None:
        """Create a store rooted at *root*, creating the directory if needed."""
        if not isinstance(root, (str, Path)):
            raise TypeError("root must be a string or Path")

        self._root = Path(root).expanduser().resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        """Return the resolved storage root."""
        return self._root

    def _resolve(self, key: str) -> Path:
        """Resolve *key* below the store root after validating it."""
        normalized = normalize_key(key)
        target = (self._root / normalized).resolve()

        if target != self._root and self._root not in target.parents:
            raise StorageKeyError("key resolves outside storage root")
        return target

    def put_bytes(self, key: str, payload: bytes) -> StoredObject:
        """Persist bytes atomically under *key*."""
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        normalized = normalize_key(key)
        target = self._resolve(normalized)
        target.parent.mkdir(parents=True, exist_ok=True)

        temporary = target.with_name(f".{target.name}.tmp")
        temporary.write_bytes(payload)
        temporary.replace(target)

        return StoredObject(key=normalized, size_bytes=len(payload))

    def get_bytes(self, key: str) -> bytes:
        """Read bytes stored under *key*."""
        return self._resolve(key).read_bytes()

    def exists(self, key: str) -> bool:
        """Return whether *key* exists as a regular file."""
        return self._resolve(key).is_file()

    def list_objects(self, prefix: str = "") -> list[StoredObject]:
        """List all objects whose keys start with *prefix*."""
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")

        normalized_prefix = ""
        if prefix.strip():
            normalized_prefix = normalize_key(prefix)

        objects: list[StoredObject] = []
        for path in self._root.rglob("*"):
            if not path.is_file():
                continue
            key = path.relative_to(self._root).as_posix()
            if normalized_prefix and not key.startswith(normalized_prefix):
                continue
            objects.append(StoredObject(key=key, size_bytes=path.stat().st_size))

        return sorted(objects, key=lambda item: item.key)
