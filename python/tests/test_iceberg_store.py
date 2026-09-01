"""Tests for the application-level Iceberg table boundary."""

from __future__ import annotations

from typing import Any

import pytest

from data_platform_lab.iceberg import IcebergTableStore


class FakeArrowTable:
    num_rows = 3


class FakeScan:
    def to_arrow(self) -> FakeArrowTable:
        return FakeArrowTable()


class FakeTable:
    def __init__(self) -> None:
        self.appended: list[Any] = []

    def append(self, table_data: Any) -> None:
        self.appended.append(table_data)

    def scan(self) -> FakeScan:
        return FakeScan()


class FakeCatalog:
    def __init__(self) -> None:
        self.namespaces: list[tuple[str, ...]] = []
        self.tables: dict[str, FakeTable] = {}

    def list_namespaces(self) -> list[tuple[str, ...]]:
        return list(self.namespaces)

    def create_namespace(self, namespace: str) -> None:
        self.namespaces.append((namespace,))

    def create_table_if_not_exists(self, identifier: str, schema: Any) -> FakeTable:
        del schema
        return self.tables.setdefault(identifier, FakeTable())

    def load_table(self, identifier: str) -> FakeTable:
        return self.tables[identifier]


def test_iceberg_store_creates_namespace_and_table_once() -> None:
    catalog = FakeCatalog()
    store = IcebergTableStore(catalog)

    first = store.ensure_table("analytics.events", object())
    second = store.ensure_table("analytics.events", object())

    assert first is second
    assert catalog.namespaces == [("analytics",)]


def test_iceberg_store_append_and_scan() -> None:
    catalog = FakeCatalog()
    store = IcebergTableStore(catalog)
    store.ensure_table("analytics.events", object())

    payload = object()
    store.append("analytics.events", payload)

    assert catalog.tables["analytics.events"].appended == [payload]
    assert store.row_count("analytics.events") == 3


def test_iceberg_identifier_requires_namespace() -> None:
    store = IcebergTableStore(FakeCatalog())

    with pytest.raises(ValueError, match="namespace"):
        store.ensure_table("events", object())
