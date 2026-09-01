"""Storage contracts and local adapters for platform workflows."""

from data_platform_lab.storage.local import (
    BlobStore,
    LocalBlobStore,
    StorageKeyError,
    StoredObject,
    normalize_key,
)

__all__ = [
    "BlobStore",
    "LocalBlobStore",
    "StorageKeyError",
    "StoredObject",
    "normalize_key",
]
