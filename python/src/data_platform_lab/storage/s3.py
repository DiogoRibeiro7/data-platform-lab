"""S3-compatible implementation of the platform object-store boundary."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any, Protocol, cast

from dataexcept import DependencyError, StorageError

from data_platform_lab.storage.local import StoredObject, normalize_key


class S3Client(Protocol):
    """Subset of the boto3 S3 client used by :class:`S3BlobStore`."""

    def put_object(self, **kwargs: object) -> Mapping[str, Any]: ...
    def get_object(self, **kwargs: object) -> Mapping[str, Any]: ...
    def head_object(self, **kwargs: object) -> Mapping[str, Any]: ...
    def list_objects_v2(self, **kwargs: object) -> Mapping[str, Any]: ...


def _is_not_found(error: Exception) -> bool:
    """Return whether an SDK-style exception represents a missing object."""
    response = getattr(error, "response", None)
    if not isinstance(response, Mapping):
        return False
    error_block = response.get("Error")
    if isinstance(error_block, Mapping) and str(error_block.get("Code")) in {
        "404",
        "NoSuchKey",
        "NotFound",
    }:
        return True
    metadata = response.get("ResponseMetadata")
    return isinstance(metadata, Mapping) and metadata.get("HTTPStatusCode") == 404


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
        return self._bucket

    @property
    def key_prefix(self) -> str:
        return self._key_prefix

    def _remote_key(self, key: str) -> str:
        normalized = normalize_key(key)
        return f"{self._key_prefix}/{normalized}" if self._key_prefix else normalized

    def _location(self, key: str = "") -> str:
        remote = self._remote_key(key) if key else self._key_prefix
        suffix = f"/{remote}" if remote else ""
        return f"s3://{self._bucket}{suffix}"

    def _logical_key(self, remote_key: str) -> str | None:
        if not self._key_prefix:
            return remote_key
        namespace = f"{self._key_prefix}/"
        if not remote_key.startswith(namespace):
            return None
        logical = remote_key[len(namespace) :]
        return logical or None

    def put_bytes(self, key: str, payload: bytes) -> StoredObject:
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        normalized = normalize_key(key)
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._remote_key(normalized),
                Body=payload,
            )
        except Exception as exc:
            raise StorageError(
                self._location(normalized),
                "write",
                f"S3 write failed: {exc}",
            ) from exc
        return StoredObject(key=normalized, size_bytes=len(payload))

    def get_bytes(self, key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._remote_key(key))
        except Exception as exc:
            raise StorageError(self._location(key), "read", f"S3 read failed: {exc}") from exc
        if "Body" not in response:
            raise StorageError(self._location(key), "read", "S3 response did not contain Body")
        return _read_body(response["Body"])

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._remote_key(key))
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise StorageError(
                self._location(key),
                "stat",
                f"S3 metadata lookup failed: {exc}",
            ) from exc
        return True

    def list_objects(self, prefix: str = "") -> list[StoredObject]:
        if not isinstance(prefix, str):
            raise TypeError("prefix must be a string")
        logical_prefix = normalize_key(prefix) if prefix.strip() else ""
        if self._key_prefix and logical_prefix:
            remote_prefix = f"{self._key_prefix}/{logical_prefix}"
        elif self._key_prefix:
            remote_prefix = f"{self._key_prefix}/"
        else:
            remote_prefix = logical_prefix

        objects: list[StoredObject] = []
        continuation_token: str | None = None
        while True:
            request: dict[str, object] = {"Bucket": self._bucket, "Prefix": remote_prefix}
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token
            try:
                response = self._client.list_objects_v2(**request)
            except Exception as exc:
                raise StorageError(
                    self._location(logical_prefix),
                    "list",
                    f"S3 listing failed: {exc}",
                ) from exc
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
                    if logical_key is not None:
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
        if (access_key_id is None) != (secret_access_key is None):
            raise ValueError("access_key_id and secret_access_key must be provided together")
        try:
            boto3_module = import_module("boto3")
            botocore_config_module = import_module("botocore.config")
        except ModuleNotFoundError as exc:
            raise DependencyError(
                "boto3",
                "boto3 is required for the S3 infrastructure adapter",
            ) from exc
        client_factory = getattr(boto3_module, "client", None)
        config_factory = getattr(botocore_config_module, "Config", None)
        if not callable(client_factory) or not callable(config_factory):
            raise DependencyError(
                "boto3",
                "boto3/botocore installation does not expose the expected S3 API",
            )
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
        try:
            client = client_factory("s3", **client_kwargs)
        except Exception as exc:
            raise StorageError(
                endpoint_url or "aws-s3",
                "connect",
                f"S3 client construction failed: {exc}",
            ) from exc
        return cls(cast(S3Client, client), bucket=bucket, key_prefix=key_prefix)
