"""Shared manifest writer — platform convention for recording pipeline outputs.

A manifest is a lightweight JSON file that records what a pipeline run produced:
source inputs, output files, row counts, timestamps, status, and schema hints.

See docs/platform-conventions.md for the canonical field definitions.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def generate_run_id() -> str:
    """Generate a timestamp-based run ID (YYYYMMDD_HHMMSS)."""
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


def write_manifest(
    pipeline_name: str,
    run_id: str,
    *,
    source: str | list[str],
    output: str | list[str],
    row_count: int,
    status: str = "success",
    schema_hint: list[str] | None = None,
    warnings: list[str] | None = None,
    extras: dict[str, Any] | None = None,
    manifest_dir: str | Path = "data/manifests",
) -> Path:
    """Write a manifest JSON file following platform conventions.

    Parameters
    ----------
    pipeline_name : str
        Which pipeline created this output.
    run_id : str
        Unique run identifier.
    source : str or list[str]
        Input path(s) or URL(s).
    output : str or list[str]
        Output path(s).
    row_count : int
        Number of rows/events in the primary output.
    status : str
        ``"success"`` or ``"failed"``.
    schema_hint : list[str] or None
        Column names or top-level keys in the output.
    warnings : list[str] or None
        Any warnings produced during the run.
    extras : dict or None
        Additional pipeline-specific metadata.
    manifest_dir : str or Path
        Directory to write the manifest file to.

    Returns
    -------
    Path
        Path to the written manifest file.
    """
    manifest_dir = Path(manifest_dir)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source": source,
        "output": output,
        "row_count": row_count,
        "status": status,
    }

    if schema_hint is not None:
        manifest["schema_hint"] = schema_hint
    if warnings:
        manifest["warnings"] = warnings
    if extras:
        manifest.update(extras)

    file_path = manifest_dir / f"{pipeline_name}_{run_id}.json"
    with file_path.open("w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2)

    logger.info("Manifest written to %s", file_path)
    return file_path


def read_manifest(path: str | Path) -> dict[str, Any]:
    """Read and parse a manifest JSON file."""
    path = Path(path)
    with path.open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)
    return data


MANIFEST_REQUIRED_KEYS = frozenset({
    "pipeline_name",
    "run_id",
    "created_at",
    "source",
    "output",
    "row_count",
    "status",
})


def validate_manifest(data: dict[str, Any]) -> list[str]:
    """Validate a manifest dict, return list of missing required keys."""
    return [k for k in MANIFEST_REQUIRED_KEYS if k not in data]
