"""Fail-closed, SHA-pinned discovery and DuckDB querying of curated pay tables.

Only the three fixed paths produced by the v1 ONS curation pipeline are ever
opened. A receipt is a local integrity record, not a cryptographic signature;
this is designed for a trusted, single-writer local data directory.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import duckdb

from data_platform_lab.curation.ons_pay import DATASET_ID, MODEL_VERSION

_REGISTRY = "DiogoRibeiro7/data"
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_HASH = re.compile(r"^[0-9a-f]{64}$")
_OUTPUTS = {
    "observations": ("silver", "observations.parquet"),
    "geography_year": ("gold", "geography_year.parquet"),
    "local_authority_yoy": ("gold", "local_authority_yoy.parquet"),
}
_COLUMNS = {
    "observations": (
        "area_code",
        "area_name",
        "geography",
        "reference_date",
        "pay_year",
        "weekly_pay_gbp",
        "lci_95_gbp",
        "uci_95_gbp",
        "status_code",
        "notes",
        "source_sha256",
        "registry_commit",
    ),
    "geography_year": (
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
    "local_authority_yoy": (
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
}


class CatalogError(ValueError):
    """Local receipt or materialized output violates the serving contract."""


class VintageNotFound(LookupError):
    """No successful curation receipt identifies the requested vintage."""


@dataclass(frozen=True, slots=True)
class VintageKey:
    """Full identity of one immutable SQL-model/source/registry combination."""

    model_version: int
    registry_commit: str
    source_sha256: str


@dataclass(frozen=True, slots=True)
class ParquetArtifact:
    path: Path
    rows: int
    sha256: str


@dataclass(frozen=True, slots=True)
class CuratedVintage:
    key: VintageKey
    manifest_sha256: str
    source_rows: int
    rows_with_pay: int
    rows_without_pay: int
    source_publisher: str
    source_url: str
    source_snapshot: str
    source_retrieved_at: str
    citation: str
    license_name: str
    license_url: str
    artifacts: dict[str, ParquetArtifact]


@dataclass(frozen=True, slots=True)
class QueryPage:
    total: int
    rows: tuple[dict[str, Any], ...]


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise CatalogError(f"invalid {name}")
    return cast(dict[str, object], value)


def _text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CatalogError(f"invalid {name}")
    return value


def _count(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise CatalogError(f"invalid {name}")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise CatalogError("curated output cannot be read") from exc
    return digest.hexdigest()


class CuratedCatalog:
    """Discover vetted receipts and query fixed Parquet tables without writes."""

    def __init__(self, data_root: Path) -> None:
        self.data_root = data_root.resolve()

    def _read_receipt(self, receipt_path: Path) -> CuratedVintage:
        try:
            # Neither the manifest directory nor a receipt may escape data_root.
            if receipt_path.resolve(strict=True) != receipt_path:
                raise CatalogError("symlinked curation receipt is not allowed")
            raw = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError, RuntimeError) as exc:
            raise CatalogError("cannot read a valid curation receipt") from exc
        data = _object(raw, "curation receipt")
        if (
            data.get("pipeline_name") != "ons_pay_curation"
            or data.get("status") != "success"
            or type(data.get("model_version")) is not int
            or data.get("model_version") != MODEL_VERSION
        ):
            raise CatalogError("receipt does not describe a successful v1 ONS curation")
        _text(data.get("run_id"), "run_id")
        provenance = _object(data.get("input"), "input provenance")
        commit = _text(provenance.get("registry_commit"), "registry_commit")
        sha = _text(provenance.get("source_sha256"), "source_sha256")
        if (
            provenance.get("dataset_id") != DATASET_ID
            or provenance.get("registry_repository") != _REGISTRY
            or not _COMMIT.fullmatch(commit)
            or not _HASH.fullmatch(sha)
        ):
            raise CatalogError("unexpected ONS registry/source identity")
        expected_uri = (
            f"https://raw.githubusercontent.com/{_REGISTRY}/{commit}/"
            f"datasets/{DATASET_ID}/raw/gross-median-weekly-pay.csv"
        )
        expected_key = f"canonical/{DATASET_ID}/{sha}/raw/gross-median-weekly-pay.csv"
        if (
            provenance.get("source_uri") != expected_uri
            or provenance.get("storage_key") != expected_key
        ):
            raise CatalogError("source location does not match canonical identity")
        for field in ("manifest_sha256", "ingestion_receipt_sha256"):
            if not _HASH.fullmatch(_text(provenance.get(field), field)):
                raise CatalogError(f"invalid {field}")
        source = _object(provenance.get("source"), "source attribution")
        attribution = {
            field: _text(source.get(field), f"source.{field}")
            for field in ("publisher", "url", "snapshot", "retrieved_at")
        }

        quality = _object(data.get("quality"), "quality")
        source_rows = _count(quality.get("source_rows"), "source_rows")
        with_pay = _count(quality.get("rows_with_pay"), "rows_with_pay")
        without_pay = _count(quality.get("rows_without_pay"), "rows_without_pay")
        if source_rows == 0 or with_pay == 0 or source_rows != with_pay + without_pay:
            raise CatalogError("curation source row counts are inconsistent")
        if any(key.startswith("invalid_") and value != 0 for key, value in quality.items()):
            raise CatalogError("curation quality gates were not all satisfied")

        outputs = data.get("outputs")
        if not isinstance(outputs, list) or len(outputs) != len(_OUTPUTS):
            raise CatalogError("curation must publish exactly three Parquet outputs")
        found: dict[str, ParquetArtifact] = {}
        for item in outputs:
            output = _object(item, "output")
            matches = [
                (name, layer, filename)
                for name, (layer, filename) in _OUTPUTS.items()
                if output.get("layer") == layer
                and isinstance(output.get("path"), str)
                and Path(cast(str, output["path"])).name == filename
            ]
            if len(matches) != 1:
                raise CatalogError("unknown curation output")
            name, layer, filename = matches[0]
            if name in found or output.get("status") not in {"published", "reused"}:
                raise CatalogError("duplicate or unsuccessful curation output")
            destination = (
                self.data_root / layer / DATASET_ID / f"v{MODEL_VERSION}" / commit / sha / filename
            )
            if output.get("path") != str(destination):
                raise CatalogError("curation output path differs from its pinned contract")
            hash_value = _text(output.get("sha256"), "output SHA-256")
            if not _HASH.fullmatch(hash_value):
                raise CatalogError("invalid output SHA-256")
            rows = _count(output.get("rows"), "output rows")
            if name == "observations" and rows != source_rows:
                raise CatalogError("Silver output must conserve the source row count")
            found[name] = ParquetArtifact(destination, rows, hash_value)

        return CuratedVintage(
            key=VintageKey(MODEL_VERSION, commit, sha),
            manifest_sha256=_text(provenance.get("manifest_sha256"), "manifest_sha256"),
            source_rows=source_rows,
            rows_with_pay=with_pay,
            rows_without_pay=without_pay,
            source_publisher=attribution["publisher"],
            source_url=attribution["url"],
            source_snapshot=attribution["snapshot"],
            source_retrieved_at=attribution["retrieved_at"],
            citation=_text(provenance.get("citation"), "citation"),
            license_name=_text(provenance.get("license_name"), "license_name"),
            license_url=_text(provenance.get("license_url"), "license_url"),
            artifacts=found,
        )

    @staticmethod
    def _verify(vintage: CuratedVintage) -> None:
        for artifact in vintage.artifacts.values():
            try:
                if (
                    artifact.path.resolve(strict=True) != artifact.path
                    or not artifact.path.is_file()
                ):
                    raise CatalogError("unsafe or missing curated output")
            except (OSError, RuntimeError) as exc:
                raise CatalogError("missing curated output") from exc
            if _sha256(artifact.path) != artifact.sha256:
                raise CatalogError("curated Parquet no longer matches its receipt SHA-256")

    @staticmethod
    def _same_contents(left: CuratedVintage, right: CuratedVintage) -> bool:
        """Execution IDs/statuses may differ on a safe idempotent curation rerun."""
        return (
            left.key == right.key
            and left.manifest_sha256 == right.manifest_sha256
            and left.source_rows == right.source_rows
            and left.rows_with_pay == right.rows_with_pay
            and left.rows_without_pay == right.rows_without_pay
            and left.source_publisher == right.source_publisher
            and left.source_url == right.source_url
            and left.source_snapshot == right.source_snapshot
            and left.source_retrieved_at == right.source_retrieved_at
            and left.citation == right.citation
            and left.license_name == right.license_name
            and left.license_url == right.license_url
            and left.artifacts == right.artifacts
        )

    def list_vintages(self) -> tuple[CuratedVintage, ...]:
        manifests = self.data_root / "manifests"
        if manifests.is_symlink():
            raise CatalogError("symlinked curation manifest directory")
        if not manifests.exists():
            return ()
        if manifests.resolve() != manifests or not manifests.is_dir():
            raise CatalogError("unsafe curation manifest directory")
        unique: dict[VintageKey, CuratedVintage] = {}
        for receipt in sorted(manifests.glob("ons_pay_curation_*.json")):
            vintage = self._read_receipt(receipt)
            previous = unique.get(vintage.key)
            if previous is not None and not self._same_contents(previous, vintage):
                raise CatalogError("conflicting curation receipts for the same data vintage")
            unique[vintage.key] = vintage
        for vintage in unique.values():
            self._verify(vintage)
        return tuple(
            sorted(
                unique.values(),
                key=lambda item: (
                    item.key.model_version,
                    item.key.registry_commit,
                    item.key.source_sha256,
                ),
            )
        )

    def get_vintage(self, key: VintageKey) -> CuratedVintage:
        for vintage in self.list_vintages():
            if vintage.key == key:
                return vintage
        raise VintageNotFound("the requested pinned ONS pay vintage was not curated")

    @staticmethod
    def _assert_artifact_rows(
        conn: duckdb.DuckDBPyConnection,
        artifact: ParquetArtifact,
        key: VintageKey,
    ) -> None:
        count = conn.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(artifact.path)]
        ).fetchone()
        if count is None or count[0] != artifact.rows:
            raise CatalogError("Parquet row count differs from its curation receipt")
        mismatched = conn.execute(
            "SELECT COUNT(*) FROM read_parquet(?) "
            "WHERE source_sha256 IS DISTINCT FROM ? OR registry_commit IS DISTINCT FROM ?",
            [str(artifact.path), key.source_sha256, key.registry_commit],
        ).fetchone()
        if mismatched is None or mismatched[0] != 0:
            raise CatalogError("Parquet rows differ from requested lineage pin")

    @classmethod
    def verify_vintage_tables(cls, vintage: CuratedVintage) -> None:
        """Check all Silver/Gold schemas, row counts, lineage and pay missingness.

        Call after catalog discovery, which already re-hashes every fixed file.
        This is an offline readiness/monitoring gate, not a source mutation.
        """
        conn = duckdb.connect(":memory:")
        try:
            for name, expected_columns in _COLUMNS.items():
                artifact = vintage.artifacts[name]
                columns = conn.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [str(artifact.path)]
                ).fetchall()
                if tuple(str(column[0]) for column in columns) != expected_columns:
                    raise CatalogError(f"curated {name} table schema has drifted")
                cls._assert_artifact_rows(conn, artifact, vintage.key)
            silver = vintage.artifacts["observations"]
            reported = conn.execute(
                "SELECT COUNT(weekly_pay_gbp) FROM read_parquet(?)", [str(silver.path)]
            ).fetchone()
            if reported is None or reported[0] != vintage.rows_with_pay:
                raise CatalogError("Silver reported/missing pay counts differ from receipt")
        except duckdb.Error as exc:
            raise CatalogError("curated Parquet does not satisfy its fixed table contract") from exc
        finally:
            conn.close()

    @classmethod
    def _query(
        cls,
        artifact: ParquetArtifact,
        key: VintageKey,
        clauses: tuple[str, ...],
        values: list[object],
        order_by: str,
        limit: int,
        offset: int,
    ) -> QueryPage:
        # Clauses and ordering are exclusively fixed strings in this module.
        # The Parquet path and *all* request values are DuckDB parameters.
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        args = [str(artifact.path), *values]
        conn = duckdb.connect(":memory:")
        try:
            cls._assert_artifact_rows(conn, artifact, key)
            count = conn.execute(f"SELECT COUNT(*) FROM read_parquet(?){where}", args).fetchone()
            if count is None or not isinstance(count[0], int):
                raise CatalogError("cannot count filtered Parquet rows")
            result = conn.execute(
                f"SELECT * FROM read_parquet(?){where} ORDER BY {order_by} LIMIT ? OFFSET ?",
                [*args, limit, offset],
            )
            names = [str(column[0]) for column in result.description]
            rows = tuple(dict(zip(names, row, strict=True)) for row in result.fetchall())
            return QueryPage(total=count[0], rows=rows)
        except duckdb.Error as exc:
            raise CatalogError("curated Parquet cannot satisfy the serving table contract") from exc
        finally:
            conn.close()

    def geography_year(
        self,
        vintage: CuratedVintage,
        *,
        geography: str | None,
        pay_year: int | None,
        limit: int,
        offset: int,
    ) -> QueryPage:
        clauses: list[str] = []
        values: list[object] = []
        if geography is not None:
            clauses.append("geography = ?")
            values.append(geography)
        if pay_year is not None:
            clauses.append("pay_year = ?")
            values.append(pay_year)
        return self._query(
            vintage.artifacts["geography_year"],
            vintage.key,
            tuple(clauses),
            values,
            "geography, pay_year",
            limit,
            offset,
        )

    def area_trend(
        self,
        vintage: CuratedVintage,
        *,
        area_code: str,
        start_year: int | None,
        end_year: int | None,
        limit: int,
        offset: int,
    ) -> QueryPage:
        clauses = ["area_code = ?"]
        values: list[object] = [area_code]
        if start_year is not None:
            clauses.append("pay_year >= ?")
            values.append(start_year)
        if end_year is not None:
            clauses.append("pay_year <= ?")
            values.append(end_year)
        return self._query(
            vintage.artifacts["local_authority_yoy"],
            vintage.key,
            tuple(clauses),
            values,
            "pay_year, area_code",
            limit,
            offset,
        )
