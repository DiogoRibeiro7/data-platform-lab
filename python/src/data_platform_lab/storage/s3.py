"""S3-compatible implementation of the platform object-store boundary."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any, Protocol, cast

from data_platform_lab.storage.local import StoredObject, normalize_key


class S3Client(Protocol):
    """Subset of the boto3 S3 client used by :class:`S3BlobStore`."""

    def put_object(self, **kwargs: object) -> Mapping[str, Any]:
        """Store one object."""

    def get_object(self, **kwargs: object) -> Mapping[str, Any]:
        """Fetch one object."""

    def head_object(self, **kwargs: object) -> Mapping[str, Any]:
        """Fetch object metadata."""

    def list_objects_v2(self, **kwargs: object) -> Mapping[str, Any]:
        """List one page of objects."""


def _is_not_found(error: Exception) -> bool:
    """Return whether an SDK-style exception represents a missing object."""
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return False

    error_block = response.get("Error")
    if isinstance(error_block, Mapping):
        code = error_block.get("Code")
        if str(code) in {"404", "NoSuchKey", "NotFound"}:
            return True

    metadata = response.get("ResponseMetadata")
    if isinstance(metadata, Mapping):
        return metadata.get("HTTPStatusCode") == 404

    return False


def _read_body(body: object) -> bytes:
    """Convert a boto3 streaming body or bytes-like value to bytes."""
    if isinstance(body, bytes):
        return body
    if isinstance(body, bytearray):
        return bytes(body)

    read = getattr(body, "read", None)
    if not callable(read):
        raise TypeError("S3 get_object response Body must be readable")

    payload = read()
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    raise TypeError("S3 response Body.read() must return bytes")


class S3BlobStore:
    """S3-compatible :class:`~data_platform_lab.storage.BlobStore` adapter."""

    def __init__(self, client: S3Client, bucket: str, key_prefix: str = "") -> None:
        """Create a store backed by *bucket* using an injected S3 client."""
        if not isinstance(bucket, str):
            raise TypeError("bucket must be a string")
        if not bucket.strip():
            raise ValueError("bucket must not be empty")
        if not isinstance(key_prefix, str):
            raise TypeError("key_prefix must be a string")

        normalized_prefix = key_prefix.strip().replace("\\", "/").rstrip("/")
        self._client = client
        self._bucket = bucket.strip()
        self._key_prefix = normalize_key(normalized_prefix) if normalized_prefix else ""

    @property
    def bucket(self) -> str:
        """Return the configured bucket name."""
        return self._bucket

    @property
    def key_prefix(self) -> str:
        """Return the logical root prefix inside the bucket."""
        return self._key_prefix

    def _remote_key(self, key: str) -> str:
        """Map a logical object key to its physical S3 key."""
        normalized = normalize_key(key)
        if not self._key_prefix:
            return normalized
        return f"{self._key_prefix}/{normalized}"

    def _logical_key(self, remote_key: str) -> str | None:
        """Map a physical S3 key back to the logical namespace."""
        if not self._key_prefix:
            return remote_key

        namespace = f"{self._key_prefix}/"
        if not remote_key.startswith(namespace):
            return None
        logical = remote_key[len(namespace) :]
        return logical or None

    def put_bytes(self, key: str, payload: bytes) -> StoredObject:
        """Persist bytes under *key* as one complete S3 object."""
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")

        normalized = normalize_key(key)
        self._client.put_object(
            Bucket=self._bucket,
            Key=self._remote_key(normalized),
            Body=payload,
        )
        return StoredObject(key=normalized, size_bytes=len(payload))

    def get_bytes(self, key: str) -> bytes:
        """Read bytes stored under *key*."""
        response = self._client.get_object(Bucket=self._bucket, Key=self._remote_key(key))
        if "Body" not in response:
            raise RuntimeError("S3 get_object response did not contain Body")
        return _read_body(response["Body"])

    def exists(self, key: str) -> bool:
        """Return whether *key* exists without downloading the object."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._remote_key(key))
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise
        return True

    def list_objects(self, prefix: str = "") -> list[StoredObject]:
        """List logical objects below *prefix* across all S3 result pages."""
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")

        logical_prefix = ""
        if prefix.strip():
            logical_prefix = normalize_key(prefix)

        if self._key_prefix and logical_prefix:
            remote_prefix = f"{self._key_prefix}/{logical_prefix}"
        elif self._key_prefix:
            remote_prefix = f"{self._key_prefix}/"
        else:
            remote_prefix = logical_prefix

        objects: list[StoredObject] = []
        continuation_token: str | None = None

        while True:
            request: dict[str, object] = {
                "Bucket": self._bucket,
                "Prefix": remote_prefix,
            }
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token

            response = self._client.list_objects_v2(**request)
            contents = response.get("Contents", [])
            if isinstance(contents, list):
                for item in contents:
                    if not isinstance(item, Mapping):
                        continue
                    remote_key = item.get("Key")
                    size_bytes = item.get("Size")
                    if not isinstance(remote_key, str) or not isinstance(size_bytes, int):
                        continue
                    logical_key = self._logical_key(remote_key)
                    if logical_key is None:
                        continue
                    objects.append(StoredObject(key=logical_key, size_bytes=size_bytes))

            next_token = response.get("NextContinuationToken")
            if not response.get("IsTruncated") or not isinstance(next_token, str) or not next_token:
                break
            continuation_token = next_token

        return sorted(objects, key=lambda item: item.key)

    @classmethod
    def from_boto3(
        cls,
        *,
        bucket: str,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        key_prefix: str = "",
        force_path_style: bool | None = None,
    ) -> S3BlobStore:
        """Build the adapter with the official boto3 S3 client.

        A custom endpoint defaults to path-style addressing because that is the
        most portable mode across local S3-compatible services such as Garage.
        AWS credentials may be omitted to use boto3's normal credential chain.
        """
        if (access_key_id is None) != (secret_access_key is None):
            raise ValueError("access_key_id and secret_access_key must be provided together")

        try:
            boto3_module = import_module("boto3")
            botocore_config_module = import_module("botocore.config")
        except ModuleNotFoundError as exc:
            raise RuntimeError("boto3 is required for the S3 infrastructure adapter") from exc

        client_factory = getattr(boto3_module, "client", None)
        config_factory = getattr(botocore_config_module, "Config", None)
        if not callable(client_factory) or not callable(config_factory):
            raise RuntimeError("boto3/botocore installation does not expose the expected S3 API")

        resolved_path_style = (
            endpoint_url is not None if force_path_style is None else force_path_style
        )
        config_kwargs: dict[str, Any] = {"signature_version": "s3v4"}
        if resolved_path_style:
            config_kwargs["s3"] = {"addressing_style": "path"}

        client_kwargs: dict[str, Any] = {"config": config_factory(**config_kwargs)}
        if endpoint_url is not None:
            client_kwargs["endpoint_url"] = endpoint_url
        if region_name is not None:
            client_kwargs["region_name"] = region_name
        if access_key_id is not None and secret_access_key is not None:
            client_kwargs["aws_access_key_id"] = access_key_id
            client_kwargs["aws_secret_access_key"] = secret_access_key

        client = client_factory("s3", **client_kwargs)
        return cls(cast(S3Client, client), bucket=bucket, key_prefix=key_prefix)
