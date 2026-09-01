"""Storage contracts and local/S3-compatible adapters for platform workflows."""

from data_platform_lab.storage.local import (
    BlobStore,
    LocalBlobStore,
    StorageKeyError,
    StoredObject,
    normalize_key,
)
from data_platform_lab.storage.s3 import S3BlobStore

__all__ = [
    "BlobStore",
    "LocalBlobStore",
    "S3BlobStore",
    "StorageKeyError",
    "StoredObject",
    "normalize_key",
]
