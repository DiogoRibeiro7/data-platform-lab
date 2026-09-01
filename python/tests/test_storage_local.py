"""Tests for the local object-store adapter."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_platform_lab.storage import LocalBlobStore, StorageKeyError, normalize_key


def test_normalize_key_rejects_traversal() -> None:
    """Parent traversal must never be accepted as an object key."""
    with pytest.raises(StorageKeyError):
        normalize_key("../outside.txt")


def test_normalize_key_converts_backslashes() -> None:
    """Object keys remain portable across operating systems."""
    assert normalize_key(r"silver\orders\part-000.csv") == "silver/orders/part-000.csv"


def test_put_get_exists_and_list(tmp_path: Path) -> None:
    """The local store implements the complete minimal BlobStore contract."""
    store = LocalBlobStore(tmp_path / "objects")

    first = store.put_bytes("bronze/events/part-001.jsonl", b"one\n")
    second = store.put_bytes("bronze/events/part-002.jsonl", b"two\n")
    store.put_bytes("silver/events.csv", b"id\n1\n")

    assert first.size_bytes == 4
    assert second.key == "bronze/events/part-002.jsonl"
    assert store.exists(first.key)
    assert store.get_bytes(first.key) == b"one\n"
    assert [item.key for item in store.list_objects("bronze/events")] == [
        "bronze/events/part-001.jsonl",
        "bronze/events/part-002.jsonl",
    ]


def test_put_bytes_overwrites_atomically(tmp_path: Path) -> None:
    """Writing an existing key replaces its complete contents."""
    store = LocalBlobStore(tmp_path)
    store.put_bytes("gold/report.json", b"old")
    stored = store.put_bytes("gold/report.json", b"new-value")

    assert stored.size_bytes == 9
    assert store.get_bytes("gold/report.json") == b"new-value"
    assert not (tmp_path / "gold" / ".report.json.tmp").exists()
