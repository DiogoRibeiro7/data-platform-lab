"""Supplier-neutral, read-only publishing plans for verified ONS pay Gold data.

Two deterministic shapes model prospective relational rows and document
records. Neither shape writes to a database or chooses a hosted provider.
All source files must satisfy the existing pinned curation contracts first.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

import duckdb

from data_platform_lab.curation.ons_pay import DATASET_ID, MODEL_VERSION
from data_platform_lab.serving.catalog import CatalogError, CuratedCatalog, VintageKey

_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_SHA = re.compile(r"^[0-9a-f]{64}$")
_MAX_ROWS = 100_000
_TEXT_FIELDS = frozenset(
    {"geography", "area_code", "area_name", "status_code", "source_sha256", "registry_commit"}
)
_INTEGER_FIELDS = frozenset(
    {"pay_year", "areas_total", "areas_with_pay", "areas_without_pay", "previous_year"}
)
_NUMERIC_FIELDS = frozenset(
    {
        "min_area_weekly_pay_gbp",
        "median_area_weekly_pay_gbp",
        "max_area_weekly_pay_gbp",
        "weekly_pay_gbp",
        "previous_weekly_pay_gbp",
        "yoy_nominal_pct",
    }
)
_NULLABLE_FIELDS = frozenset(
    {
        "min_area_weekly_pay_gbp",
        "median_area_weekly_pay_gbp",
        "max_area_weekly_pay_gbp",
        "weekly_pay_gbp",
        "status_code",
        "previous_year",
        "previous_weekly_pay_gbp",
        "yoy_nominal_pct",
    }
)

TableName = Literal["geography_year", "local_authority_yoy"]
SinkShape = Literal["relational", "document"]
JsonScalar: TypeAlias = str | int | float | None
JsonValue: TypeAlias = JsonScalar | dict[str, JsonScalar]
ProjectedRecord: TypeAlias = dict[str, JsonValue]


@dataclass(frozen=True, slots=True)
class _TableContract:
    """Fixed v1 output columns, natural grain and deterministic sort order."""

    columns: tuple[str, ...]
    grain: tuple[str, ...]
    ordering: str


_TABLES: dict[TableName, _TableContract] = {
    "geography_year": _TableContract(
        columns=(
            "geography",
            "pay_year",
            "areas_total",
            "areas_with_pay",
            "areas_without_pay",
            "min_area_weekly_pay_gbp",
            "median_area_weekly_pay_gbp",
            "max_area_weekly_pay_gbp",
            "source_sha256",
            "registry_commit",
        ),
        grain=("geography", "pay_year"),
        ordering="geography, pay_year",
    ),
    "local_authority_yoy": _TableContract(
        columns=(
            "area_code",
            "area_name",
            "reference_date",
            "pay_year",
            "weekly_pay_gbp",
            "status_code",
            "previous_year",
            "previous_weekly_pay_gbp",
            "yoy_nominal_pct",
            "source_sha256",
            "registry_commit",
        ),
        grain=("area_code", "reference_date"),
        ordering="area_code, reference_date",
    ),
}


class PublishPlanError(ValueError):
    """An export limit, data contract or source provenance check failed."""


@dataclass(frozen=True, slots=True)
class PublishPlan:
    """Immutable preview for a potential SQL or document-store ingestion."""

    schema_version: int
    dataset_id: str
    table: TableName
    shape: SinkShape
    vintage: VintageKey
    source_artifact_sha256: str
    source_rows: int
    records_sha256: str
    records: tuple[ProjectedRecord, ...]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the exact planned records and their content fingerprint."""
        return asdict(self)


def _canonical_json(value: object) -> bytes:
    """Produce stable UTF-8 bytes independent of JSON formatting choices."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _validate_request(key: VintageKey, table: TableName, shape: SinkShape, max_rows: int) -> None:
    """Check all public arguments before opening a curated catalog."""
    if (
        type(key.model_version) is not int
        or key.model_version != MODEL_VERSION
        or not isinstance(key.registry_commit, str)
        or _COMMIT.fullmatch(key.registry_commit) is None
        or not isinstance(key.source_sha256, str)
        or _SHA.fullmatch(key.source_sha256) is None
    ):
        raise PublishPlanError("publishing requires exact v1 registry/source pins")
    if type(table) is not str or table not in _TABLES:
        raise PublishPlanError("table must be geography_year or local_authority_yoy")
    if type(shape) is not str or shape not in {"relational", "document"}:
        raise PublishPlanError("shape must be relational or document")
    if type(max_rows) is not int or not 1 <= max_rows <= _MAX_ROWS:
        raise PublishPlanError("max_rows must be an integer from 1 to 100000")


def _scalar(value: object) -> JsonScalar:
    """Convert DuckDB date values and reject unsupported/non-finite scalars."""
    if value is None or type(value) in {str, int}:
        return cast(JsonScalar, value)
    if type(value) is date:
        return cast(date, value).isoformat()
    if type(value) is float and math.isfinite(value):
        return cast(float, value)
    raise PublishPlanError("curated Gold data contains an unsafe scalar")


def _typed_field(name: str, value: object) -> JsonScalar:
    """Enforce the versioned Gold field contract, not just JSON compatibility."""
    converted = _scalar(value)
    if value is None:
        if name not in _NULLABLE_FIELDS:
            raise PublishPlanError(f"non-nullable Gold field is missing: {name}")
        return None
    if name in _TEXT_FIELDS and type(value) is str:
        return converted
    if name in _INTEGER_FIELDS and type(value) is int:
        return converted
    if name in _NUMERIC_FIELDS and type(value) in {int, float}:
        return converted
    if name == "reference_date" and type(value) is date:
        return converted
    raise PublishPlanError(f"curated Gold field has an unexpected type: {name}")


def _id(*, table: TableName, vintage: VintageKey, grain: dict[str, JsonScalar]) -> str:
    """Key each row by its complete vintage and exact business grain."""
    return hashlib.sha256(
        _canonical_json(
            {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "table": table,
                "vintage": asdict(vintage),
                "grain": grain,
            }
        )
    ).hexdigest()


def _convert_row(
    *,
    columns: tuple[str, ...],
    values: tuple[object, ...],
    contract: _TableContract,
    vintage: VintageKey,
) -> dict[str, JsonScalar]:
    """Validate lineage and non-null grain before any sink projection."""
    if columns != contract.columns or len(values) != len(columns):
        raise PublishPlanError("curated Gold columns differ from the v1 publishing schema")
    row = {name: _typed_field(name, value) for name, value in zip(columns, values, strict=True)}
    if (
        row["source_sha256"] != vintage.source_sha256
        or row["registry_commit"] != vintage.registry_commit
    ):
        raise PublishPlanError("curated Gold row does not match its pinned provenance")
    for name in contract.grain:
        value = row[name]
        if value is None or (type(value) is str and not value):
            raise PublishPlanError("curated Gold row has a missing business key")
    if "areas_total" in row and row["areas_total"] != (
        cast(int, row["areas_with_pay"]) + cast(int, row["areas_without_pay"])
    ):
        raise PublishPlanError("curated Gold area-count measures do not reconcile")
    # Source validation owns upstream metrics; the export only normalizes
    # portable types and prevents silent key or lineage loss.
    return row


def _project(
    *,
    table: TableName,
    shape: SinkShape,
    vintage: VintageKey,
    contract: _TableContract,
    row: dict[str, JsonScalar],
) -> ProjectedRecord:
    """Map one verified Gold row to a flat or nested logical target model."""
    grain = {name: row[name] for name in contract.grain}
    record_id = _id(table=table, vintage=vintage, grain=grain)
    if shape == "relational":
        return {"record_id": record_id, "model_version": vintage.model_version, **row}
    return {
        "_id": record_id,
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "table": table,
        "vintage": {
            "model_version": vintage.model_version,
            "registry_commit": vintage.registry_commit,
            "source_sha256": vintage.source_sha256,
        },
        "data": row,
    }


def _verify_after_read(path: Path, expected_sha256: str) -> None:
    """Invalidate a plan if the local Gold artifact changed during its read."""
    try:
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
    except OSError as exc:
        raise PublishPlanError("curated Gold artifact became unreadable") from exc
    if digest != expected_sha256:
        raise PublishPlanError("curated Gold artifact changed during publishing preview")


def plan_ons_pay_publishing(
    *,
    data_root: Path,
    vintage: VintageKey,
    table: TableName,
    shape: SinkShape,
    max_rows: int = _MAX_ROWS,
) -> PublishPlan:
    """Prepare a deterministic, SHA-pinned target batch without any writes.

    All published Silver/Gold artifacts are validated before selecting the
    table. The bounded in-memory representation is for audit and portability
    testing, not a production bulk-load or a hosted database connector.
    """
    _validate_request(vintage, table, shape, max_rows)
    catalog = CuratedCatalog(data_root)
    curated = catalog.get_vintage(vintage)
    catalog.verify_vintage_tables(curated)
    artifact = curated.artifacts[table]
    if artifact.rows > max_rows:
        raise PublishPlanError("curated Gold table exceeds the explicit max_rows budget")

    contract = _TABLES[table]
    conn = duckdb.connect(":memory:")
    try:
        # The table name, ordering and path are drawn only from verified,
        # fixed contracts; there is no interpolation of caller SQL input.
        result = conn.execute(
            f"SELECT * FROM read_parquet(?) ORDER BY {contract.ordering}",
            [str(artifact.path)],
        )
        columns = tuple(str(column[0]) for column in result.description)
        if columns != contract.columns:
            raise PublishPlanError("curated Gold columns differ from the publishing contract")
        records: list[ProjectedRecord] = []
        seen: set[str] = set()
        for values in result.fetchall():
            row = _convert_row(
                columns=columns, values=tuple(values), contract=contract, vintage=vintage
            )
            record = _project(table=table, shape=shape, vintage=vintage, contract=contract, row=row)
            identifier = cast(str, record["record_id" if shape == "relational" else "_id"])
            if identifier in seen:
                raise PublishPlanError("curated Gold output contains duplicate business keys")
            seen.add(identifier)
            records.append(record)
            if len(records) > max_rows:
                raise PublishPlanError("curated Gold table exceeded max_rows during reading")
    except duckdb.Error as exc:
        raise CatalogError("curated Gold artifact cannot be read for publishing") from exc
    finally:
        conn.close()

    _verify_after_read(artifact.path, artifact.sha256)
    if len(records) != artifact.rows:
        raise PublishPlanError("export row count disagrees with the curation receipt")
    return PublishPlan(
        schema_version=1,
        dataset_id=DATASET_ID,
        table=table,
        shape=shape,
        vintage=vintage,
        source_artifact_sha256=artifact.sha256,
        source_rows=artifact.rows,
        records_sha256=hashlib.sha256(_canonical_json(records)).hexdigest(),
        records=tuple(records),
    )
