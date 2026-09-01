"""Application-level Iceberg table operations."""

from __future__ import annotations

from typing import Any, Protocol

Namespace = tuple[str, ...]


class Catalog(Protocol):
    """Subset of the PyIceberg catalog surface used by the platform."""

    def create_namespace_if_not_exists(self, namespace: Namespace) -> None: ...

    def create_table_if_not_exists(self, identifier: str, schema: Any) -> Any: ...

    def load_table(self, identifier: str) -> Any: ...


class IcebergTableStore:
    """Create, append to, scan, and reconcile Iceberg analytical tables."""

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog

    @staticmethod
    def _namespace(identifier: str) -> Namespace:
        """Return the full namespace from a qualified Iceberg table identifier."""
        if not isinstance(identifier, str):
            raise TypeError("Iceberg identifier must be a string")

        components = tuple(identifier.split("."))
        if len(components) < 2 or any(
            not component or component != component.strip() for component in components
        ):
            raise ValueError(
                "Iceberg identifiers must contain non-empty namespace and table components"
            )
        return components[:-1]

    def ensure_namespace(self, namespace: Namespace) -> None:
        """Create *namespace* idempotently, including under concurrent initialization."""
        self._catalog.create_namespace_if_not_exists(namespace)

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

    def contains_value(self, identifier: str, field_name: str, value: object) -> bool:
        """Return whether *field_name* already contains *value* in the table.

        This intentionally uses a full Arrow scan. It is a simple reconciliation
        primitive for the local recovery demo, not a substitute for a production
        index or merge-on-read strategy.
        """
        self._namespace(identifier)
        if not isinstance(field_name, str) or not field_name.strip():
            raise ValueError("field_name must not be empty")

        table = self._catalog.load_table(identifier)
        arrow_table = table.scan(selected_fields=(field_name,)).to_arrow()
        try:
            column = arrow_table.column(field_name)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Iceberg table does not contain field: {field_name}") from exc
        return value in column.to_pylist()

    def row_count(self, identifier: str) -> int:
        """Scan an Iceberg table and return its current row count."""
        self._namespace(identifier)
        table = self._catalog.load_table(identifier)
        arrow_table = table.scan().to_arrow()
        return int(arrow_table.num_rows)
