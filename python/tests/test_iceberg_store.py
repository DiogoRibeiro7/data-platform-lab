"""Tests for the application-level Iceberg table boundary."""

from __future__ import annotations

from typing import Any

import pytest

from data_platform_lab.iceberg import IcebergTableStore


class FakeColumn:
    def __init__(self, values: list[object]) -> None:
        self._values = values

    def to_pylist(self) -> list[object]:
        return list(self._values)


class FakeArrowTable:
    def __init__(self, appended: list[Any]) -> None:
        self._appended = appended
        self.num_rows = sum(int(getattr(payload, "num_rows", 0)) for payload in appended)

    def column(self, field_name: str) -> FakeColumn:
        values: list[object] = []
        for payload in self._appended:
            values.extend(getattr(payload, "values", {}).get(field_name, []))
        if not values:
            raise KeyError(field_name)
        return FakeColumn(values)


class FakePayload:
    def __init__(self, num_rows: int, values: dict[str, list[object]] | None = None) -> None:
        self.num_rows = num_rows
        self.values = values or {}


class FakeScan:
    def __init__(self, appended: list[Any]) -> None:
        self._appended = appended

    def to_arrow(self) -> FakeArrowTable:
        return FakeArrowTable(self._appended)


class FakeTable:
    def __init__(self) -> None:
        self.appended: list[Any] = []

    def append(self, table_data: Any) -> None:
        self.appended.append(table_data)

    def scan(self, selected_fields: tuple[str, ...] | None = None) -> FakeScan:
        del selected_fields
        return FakeScan(self.appended)


class FakeCatalog:
    def __init__(self) -> None:
        self.namespaces: list[tuple[str, ...]] = []
        self.tables: dict[str, FakeTable] = {}

    def create_namespace_if_not_exists(self, namespace: tuple[str, ...]) -> None:
        if namespace not in self.namespaces:
            self.namespaces.append(namespace)

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


def test_iceberg_store_supports_nested_namespaces() -> None:
    catalog = FakeCatalog()
    store = IcebergTableStore(catalog)

    store.ensure_table("warehouse.analytics.events", object())

    assert catalog.namespaces == [("warehouse", "analytics")]


def test_iceberg_store_append_and_scan_reflects_appended_rows() -> None:
    catalog = FakeCatalog()
    store = IcebergTableStore(catalog)
    store.ensure_table("analytics.events", object())

    first = FakePayload(2)
    second = FakePayload(1)
    store.append("analytics.events", first)
    store.append("analytics.events", second)

    assert catalog.tables["analytics.events"].appended == [first, second]
    assert store.row_count("analytics.events") == 3


def test_iceberg_store_reconciles_by_ingestion_id() -> None:
    catalog = FakeCatalog()
    store = IcebergTableStore(catalog)
    store.ensure_table("analytics.events", object())
    store.append(
        "analytics.events",
        FakePayload(1, {"ingestion_id": ["events:0:7"]}),
    )

    assert store.contains_value("analytics.events", "ingestion_id", "events:0:7")
    assert not store.contains_value("analytics.events", "ingestion_id", "events:0:8")


@pytest.mark.parametrize(
    "identifier",
    [
        "events",
        ".events",
        "analytics.",
        "analytics..events",
        " analytics.events",
        "analytics.events ",
    ],
)
def test_iceberg_identifier_rejects_malformed_components(identifier: str) -> None:
    store = IcebergTableStore(FakeCatalog())

    with pytest.raises(ValueError, match="namespace"):
        store.ensure_table(identifier, object())
