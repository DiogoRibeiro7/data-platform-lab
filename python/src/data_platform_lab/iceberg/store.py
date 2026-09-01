"""Application-level Iceberg table operations."""

from __future__ import annotations

from typing import Any, Protocol


class Catalog(Protocol):
    """Subset of the PyIceberg catalog surface used by the platform."""

    def list_namespaces(self) -> list[tuple[str, ...]]: ...

    def create_namespace(self, namespace: str) -> None: ...

    def create_table_if_not_exists(self, identifier: str, schema: Any) -> Any: ...

    def load_table(self, identifier: str) -> Any: ...


class IcebergTableStore:
    """Create, append to, and scan Iceberg analytical tables."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    @staticmethod
    def _namespace(identifier: str) -> str:
        """Return the namespace from a qualified Iceberg table identifier."""
        if not isinstance(identifier, str):
            raise TypeError("Iceberg identifier must be a string")

        namespace, separator, table_name = identifier.partition(".")
        if not separator or not namespace.strip() or not table_name.strip():
            raise ValueError("Iceberg identifiers must include a namespace")
        return namespace

    def ensure_namespace(self, namespace: str) -> None:
        """Create *namespace* once if it is not already present."""
        if (namespace,) not in self._catalog.list_namespaces():
            self._catalog.create_namespace(namespace)

    def ensure_table(self, identifier: str, schema: Any) -> Any:
        """Return an existing table or create it using *schema*."""
        namespace = self._namespace(identifier)
        self.ensure_namespace(namespace)
        return self._catalog.create_table_if_not_exists(identifier, schema)

    def append(self, identifier: str, table_data: Any) -> None:
        """Append one Arrow table to an existing Iceberg table."""
        self._namespace(identifier)
        table = self._catalog.load_table(identifier)
        table.append(table_data)

    def row_count(self, identifier: str) -> int:
        """Scan an Iceberg table and return its current row count."""
        self._namespace(identifier)
        table = self._catalog.load_table(identifier)
        arrow_table = table.scan().to_arrow()
        return int(arrow_table.num_rows)
