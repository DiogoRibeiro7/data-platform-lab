"""CSV ingestion and cleaning pipeline.

Reads multiple CSV files from a directory, validates, standardises,
deduplicates, and writes a single cleaned output CSV.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Summary produced by a single pipeline run."""

    files_processed: list[str] = field(default_factory=list)
    files_rejected: list[str] = field(default_factory=list)
    rows_read: int = 0
    rows_written: int = 0
    duplicates_removed: int = 0


def read_csv_file(path: Path) -> tuple[list[str], list[list[str]]]:
    """Read a single CSV file and return (headers, rows).

    Raises :class:`ValueError` if the file is empty (no header row).
    """
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            headers = next(reader)
        except StopIteration:
            raise ValueError(f"CSV file is empty: {path}") from None
        rows = list(reader)
    return headers, rows


def validate_columns(headers: list[str], required: list[str]) -> list[str]:
    """Return a list of required columns that are missing from *headers*."""
    header_set = set(headers)
    return [col for col in required if col not in header_set]


def standardize_headers(headers: list[str]) -> list[str]:
    """Lowercase, strip whitespace, and replace spaces with underscores."""
    return [h.strip().lower().replace(" ", "_") for h in headers]


def trim_fields(rows: list[list[str]]) -> list[list[str]]:
    """Strip leading/trailing whitespace from every field."""
    return [[cell.strip() for cell in row] for row in rows]


def deduplicate(
    rows: list[list[str]],
) -> tuple[list[list[str]], int]:
    """Remove exact-duplicate rows.

    Returns (unique_rows, count_removed).
    """
    seen: set[tuple[str, ...]] = set()
    unique: list[list[str]] = []
    for row in rows:
        key = tuple(row)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    removed = len(rows) - len(unique)
    return unique, removed


def _write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    """Write *headers* and *rows* to a CSV at *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)


def run_pipeline(
    input_dir: Path,
    output_path: Path,
    required_columns: list[str] | None = None,
) -> PipelineResult:
    """Orchestrate the full CSV ingestion pipeline.

    1. Glob ``*.csv`` in *input_dir*.
    2. Read, validate, standardise headers, and trim each file.
    3. Merge rows, deduplicate, write the cleaned output, and return a summary.
    """
    result = PipelineResult()
    csv_files = sorted(input_dir.glob("*.csv"))

    if not csv_files:
        logger.warning("No CSV files found in %s", input_dir)
        _write_csv(output_path, [], [])
        return result

    merged_headers: list[str] | None = None
    all_rows: list[list[str]] = []

    for csv_file in csv_files:
        try:
            raw_headers, raw_rows = read_csv_file(csv_file)
        except Exception:
            reason = f"{csv_file.name}: unable to read file"
            logger.warning(reason)
            result.files_rejected.append(reason)
            continue

        std_headers = standardize_headers(raw_headers)

        # Validate required columns against the *standardised* headers.
        if required_columns is not None:
            missing = validate_columns(std_headers, required_columns)
            if missing:
                reason = (
                    f"{csv_file.name}: missing required columns "
                    f"{missing}"
                )
                logger.warning(reason)
                result.files_rejected.append(reason)
                continue

        # Establish the canonical header set from the first valid file.
        if merged_headers is None:
            merged_headers = std_headers

        # Only keep rows whose width matches the header width.
        trimmed = trim_fields(raw_rows)
        valid_rows = [
            r for r in trimmed if len(r) == len(std_headers)
        ]
        skipped = len(trimmed) - len(valid_rows)
        if skipped:
            logger.info(
                "%s: skipped %d malformed rows (column count mismatch)",
                csv_file.name,
                skipped,
            )

        result.rows_read += len(raw_rows)
        all_rows.extend(valid_rows)
        result.files_processed.append(csv_file.name)

    if merged_headers is None:
        merged_headers = []

    unique_rows, dups = deduplicate(all_rows)
    result.duplicates_removed = dups
    result.rows_written = len(unique_rows)

    _write_csv(output_path, merged_headers, unique_rows)
    return result
