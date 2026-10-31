"""Reproducible ONS pay Silver/Gold models from a verified ingestion receipt.

The analytical layer reads immutable bytes through ``BlobStore``. It writes
local, content-addressed Parquet outputs and never mutates the source registry.
DuckDB is an optional runtime dependency; base platform commands remain usable
without installing the ``analytics`` extra.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module, resources
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import Any, Protocol, cast
from uuid import uuid4

from dataexcept import DependencyError

from data_platform_lab.storage import BlobStore

DATASET_ID = "ons-gross-median-weekly-pay"
MODEL_VERSION = 1
_REGISTRY = "DiogoRibeiro7/data"
_SOURCE_PATH = f"datasets/{DATASET_ID}/raw/gross-median-weekly-pay.csv"
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_EXPECTED_COLUMNS = (
    "areacd",
    "areanm",
    "geography",
    "indicator",
    "period",
    "observation",
    "measure",
    "unit",
    "lci_95",
    "uci_95",
    "status",
    "notes",
)
_OUTPUTS = (
    ("silver", "silver_pay", "observations.parquet", "area_code, reference_date"),
    ("gold", "gold_geography_year", "geography_year.parquet", "geography, pay_year"),
    ("gold", "gold_local_authority_yoy", "local_authority_yoy.parquet", "area_code, pay_year"),
)


class CurationError(ValueError):
    """Raised for malformed lineage, failed data-quality checks, or output drift."""


class _DuckConnection(Protocol):
    @property
    def description(self) -> list[tuple[object, ...]]: ...

    def execute(self, query: str, parameters: list[object] | None = None) -> _DuckConnection: ...
    def fetchone(self) -> tuple[object, ...] | None: ...
    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class IngestedPay:
    """Validated identity of the single approved ONS pay source object."""

    run_id: str
    registry_commit: str
    source_sha256: str
    source_uri: str
    storage_key: str
    size_bytes: int
    manifest_sha256: str
    source: dict[str, str]
    citation: str
    license_name: str
    license_url: str


@dataclass(frozen=True, slots=True)
class CuratedArtifact:
    """One published Parquet table with a fixed analytical grain."""

    layer: str
    path: Path
    rows: int
    sha256: str
    reused: bool


@dataclass(frozen=True, slots=True)
class CurationResult:
    """Successful curation summary with its auditable execution receipt."""

    run_id: str
    source_rows: int
    rows_with_pay: int
    rows_without_pay: int
    outputs: tuple[CuratedArtifact, ...]
    receipt_path: Path


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value):
        raise CurationError(f"{field} must be a JSON object")
    return cast(dict[str, object], value)


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CurationError(f"{field} must be non-empty text")
    return value


def parse_ingestion_receipt(value: object) -> IngestedPay:
    """Require the successful registry handoff receipt for this exact ONS source."""
    receipt = _mapping(value, "receipt")
    if (
        receipt.get("pipeline_name") != "registry_handoff_ingestion"
        or receipt.get("status") != "success"
        or type(receipt.get("manifest_version")) is not int
        or receipt.get("manifest_version") != 1
        or receipt.get("dataset_id") != DATASET_ID
    ):
        raise CurationError("expected a successful ONS registry-handoff v1 receipt")

    run_id = _required_text(receipt.get("run_id"), "run_id")
    manifest_sha = _required_text(receipt.get("manifest_sha256"), "manifest_sha256")
    if not _HASH_RE.fullmatch(manifest_sha):
        raise CurationError("invalid source manifest SHA-256")

    registry = _mapping(receipt.get("registry"), "registry")
    commit = _required_text(registry.get("commit"), "registry.commit")
    if registry.get("repository") != _REGISTRY or not _COMMIT_RE.fullmatch(commit):
        raise CurationError("receipt must identify the exact canonical registry commit")

    records = receipt.get("objects")
    if not isinstance(records, list):
        raise CurationError("receipt.objects must be a JSON array")
    objects = [_mapping(item, "receipt.objects item") for item in records]
    if any(
        item.get("role") not in {"raw", "derived"}
        or item.get("status") not in {"published", "reused"}
        for item in objects
    ):
        raise CurationError("receipt contains an invalid object role or publication status")
    raw_objects = [item for item in objects if item.get("role") == "raw"]
    if len(raw_objects) != 1:
        raise CurationError("ONS pay curation requires exactly one raw source object")
    raw = raw_objects[0]
    if raw.get("source_path") != _SOURCE_PATH or raw.get("format") != "csv":
        raise CurationError("unexpected canonical source path or file format")
    sha = _required_text(raw.get("sha256"), "objects.sha256")
    if not _HASH_RE.fullmatch(sha):
        raise CurationError("invalid canonical source SHA-256")
    expected_key = f"canonical/{DATASET_ID}/{sha}/raw/gross-median-weekly-pay.csv"
    expected_uri = f"https://raw.githubusercontent.com/{_REGISTRY}/{commit}/{_SOURCE_PATH}"
    if raw.get("storage_key") != expected_key or raw.get("source_uri") != expected_uri:
        raise CurationError("source object identity differs from the pinned registry contract")
    if raw.get("status") not in {"published", "reused"}:
        raise CurationError("input object was not successfully published or reused")
    size = raw.get("size_bytes")
    if type(size) is not int or size < 0:
        raise CurationError("objects.size_bytes must be a non-negative integer")

    source = _mapping(receipt.get("source"), "source")
    attribution = {
        field: _required_text(source.get(field), f"source.{field}")
        for field in ("publisher", "url", "snapshot", "retrieved_at")
    }
    return IngestedPay(
        run_id=run_id,
        registry_commit=commit,
        source_sha256=sha,
        source_uri=expected_uri,
        storage_key=expected_key,
        size_bytes=size,
        manifest_sha256=manifest_sha,
        source=attribution,
        citation=_required_text(receipt.get("citation"), "citation"),
        license_name=_required_text(receipt.get("license_name"), "license_name"),
        license_url=_required_text(receipt.get("license_url"), "license_url"),
    )


def _sql(filename: str) -> str:
    """Load installed SQL assets (also works from a built wheel)."""
    return (
        resources.files("data_platform_lab.curation")
        .joinpath("sql", filename)
        .read_text(encoding="utf-8")
    )


def _connect() -> _DuckConnection:
    try:
        module = import_module("duckdb")
    except ModuleNotFoundError as exc:
        raise DependencyError(
            "duckdb", "install the analytics extra: pip install -e '.[analytics]'"
        ) from exc
    factory = getattr(module, "connect", None)
    if not callable(factory):
        raise DependencyError("duckdb", "DuckDB installation has no connect function")
    return cast(_DuckConnection, factory(":memory:"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_csv_header(payload: bytes) -> None:
    """Fail on schema drift before DuckDB can infer or rename incoming columns."""
    try:
        with TextIOWrapper(BytesIO(payload), encoding="utf-8-sig", newline="") as stream:
            header = next(csv.reader(stream, strict=True))
    except (UnicodeDecodeError, csv.Error, StopIteration) as exc:
        raise CurationError("source is not a valid UTF-8 CSV header") from exc
    if tuple(header) != _EXPECTED_COLUMNS:
        raise CurationError(f"ONS CSV schema drift: expected {_EXPECTED_COLUMNS}, got {header}")


def _quality_metrics(connection: _DuckConnection) -> dict[str, int]:
    """Execute source-level quality gates before any Silver/Gold writes."""
    query = _sql("ons_pay_quality.sql")
    row = connection.execute(query).fetchone()
    if row is None:
        raise CurationError("quality query returned no result")
    columns = [str(column[0]) for column in connection.description]
    metrics: dict[str, int] = {}
    for name, value in zip(columns, row, strict=True):
        if type(value) is not int:
            raise CurationError(f"quality metric {name} must be an integer")
        metrics[name] = value

    duplicates = connection.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT areacd, EXTRACT(YEAR FROM TRY_CAST(period AS DATE)), COUNT(*)
            FROM raw_input
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()
    if duplicates is None or type(duplicates[0]) is not int:
        raise CurationError("duplicate-grain quality check did not return an integer")
    metrics["invalid_duplicate_area_year"] = duplicates[0]
    violations = {
        name: count for name, count in metrics.items() if name.startswith("invalid_") and count
    }
    if metrics.get("source_rows", 0) < 1 or metrics.get("rows_with_pay", 0) < 1:
        raise CurationError("ONS pay input must contain at least one reported observation")
    if violations:
        raise CurationError(f"ONS pay quality gates failed: {violations}")
    return metrics


def _row_count(connection: _DuckConnection, table: str) -> int:
    # Names come from a fixed internal table map, never user input.
    row = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    if row is None or type(row[0]) is not int:
        raise CurationError(f"could not count {table} rows")
    return row[0]


def _save_receipt(path: Path, data: dict[str, Any]) -> None:
    """Publish only a fully successful execution record by atomic rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=".ons_pay_", delete=False
        ) as handle:
            temporary = Path(handle.name)
            json.dump(data, handle, sort_keys=True, indent=2)
            handle.write("\n")
        temporary.replace(path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def curate_ons_pay(
    *,
    input_receipt: Path,
    store: BlobStore,
    output_root: Path,
) -> CurationResult:
    """Model one verified ONS pay snapshot with audited, immutable Parquet output.

    An existing identical output is reused without touching its mtime. If a
    model produces different bytes under an existing versioned key, it fails
    closed; intentionally changing SQL semantics requires ``MODEL_VERSION`` to
    be advanced. Multi-file output is single-writer, not a distributed commit.
    """
    raw_receipt = input_receipt.read_bytes()
    try:
        source = parse_ingestion_receipt(json.loads(raw_receipt))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CurationError("ingestion receipt must be valid UTF-8 JSON") from exc

    payload = store.get_bytes(source.storage_key)
    if (
        len(payload) != source.size_bytes
        or hashlib.sha256(payload).hexdigest() != source.source_sha256
    ):
        raise CurationError("ingested raw object no longer matches its SHA-256/byte count")
    _validate_csv_header(payload)

    output_root = output_root.resolve()
    staging = output_root / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC)
    run_id = uuid4().hex
    subpath = Path(DATASET_ID) / f"v{MODEL_VERSION}" / source.registry_commit / source.source_sha256

    with tempfile.TemporaryDirectory(prefix="ons-pay-", dir=staging) as scratch:
        temp_dir = Path(scratch)
        csv_input = temp_dir / "source.csv"
        csv_input.write_bytes(payload)
        connection = _connect()
        try:
            connection.execute(
                "CREATE TEMP TABLE raw_input AS "
                "SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                [str(csv_input)],
            )
            metrics = _quality_metrics(connection)
            connection.execute(
                _sql("ons_pay_silver.sql"), [source.source_sha256, source.registry_commit]
            )
            if _row_count(connection, "silver_pay") != metrics["source_rows"]:
                raise CurationError("Silver row conservation failed")
            connection.execute(_sql("ons_pay_geography_year.sql"))
            connection.execute(_sql("ons_pay_local_authority_yoy.sql"))

            prepared: list[tuple[str, Path, Path, int, str]] = []
            for layer, table, filename, order_by in _OUTPUTS:
                staged_file = temp_dir / filename
                # All SQL identifiers are fixed in _OUTPUTS; the path is parameterized.
                connection.execute(
                    f"COPY (SELECT * FROM {table} ORDER BY {order_by}) "
                    "TO ? (FORMAT PARQUET, COMPRESSION zstd)",
                    [str(staged_file)],
                )
                rows = _row_count(connection, table)
                # Check the materialized table, not merely the in-memory query.
                persisted = connection.execute(
                    "SELECT COUNT(*) FROM read_parquet(?)", [str(staged_file)]
                ).fetchone()
                if persisted is None or persisted[0] != rows:
                    raise CurationError(f"Parquet row count mismatch: {filename}")
                destination = output_root / layer / subpath / filename
                prepared.append((layer, staged_file, destination, rows, _sha256_file(staged_file)))
        finally:
            connection.close()

        # Preflight *all* existing destinations before publishing any new file.
        for _, _, destination, _, expected_sha in prepared:
            if destination.exists() and (
                not destination.is_file() or _sha256_file(destination) != expected_sha
            ):
                raise CurationError(f"existing immutable Parquet output differs: {destination}")

        outputs: list[CuratedArtifact] = []
        for layer, staged_file, destination, rows, expected_sha in prepared:
            reused = destination.exists()
            if not reused:
                destination.parent.mkdir(parents=True, exist_ok=True)
                staged_file.replace(destination)
                if _sha256_file(destination) != expected_sha:
                    raise CurationError(f"destination Parquet verification failed: {destination}")
            outputs.append(CuratedArtifact(layer, destination, rows, expected_sha, reused))

    finished_at = datetime.now(UTC)
    execution_receipt = output_root / "manifests" / f"ons_pay_curation_{run_id}.json"
    receipt: dict[str, Any] = {
        "pipeline_name": "ons_pay_curation",
        "status": "success",
        "run_id": run_id,
        "created_at": finished_at.isoformat(),
        "started_at": started_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "model_version": MODEL_VERSION,
        "input": {
            "ingestion_run_id": source.run_id,
            "ingestion_receipt_sha256": hashlib.sha256(raw_receipt).hexdigest(),
            "manifest_sha256": source.manifest_sha256,
            "dataset_id": DATASET_ID,
            "registry_repository": _REGISTRY,
            "registry_commit": source.registry_commit,
            "source_sha256": source.source_sha256,
            "source_uri": source.source_uri,
            "storage_key": source.storage_key,
            "source": source.source,
            "license_name": source.license_name,
            "license_url": source.license_url,
            "citation": source.citation,
        },
        "quality": metrics,
        "outputs": [
            {
                "layer": item.layer,
                "path": str(item.path),
                "rows": item.rows,
                "sha256": item.sha256,
                "status": "reused" if item.reused else "published",
            }
            for item in outputs
        ],
    }
    _save_receipt(execution_receipt, receipt)
    return CurationResult(
        run_id=run_id,
        source_rows=metrics["source_rows"],
        rows_with_pay=metrics["rows_with_pay"],
        rows_without_pay=metrics["rows_without_pay"],
        outputs=tuple(outputs),
        receipt_path=execution_receipt,
    )
