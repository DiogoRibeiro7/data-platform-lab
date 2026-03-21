"""Shared test fixtures and helpers.

Provides reusable path constants and file-writing utilities used across
multiple test modules.  Only genuinely duplicated helpers live here —
module-specific factories (like ``make_event``) stay in their test files
to keep behaviour visible where it matters.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Repository path constants
# ---------------------------------------------------------------------------

REPO_ROOT: Path = Path(__file__).resolve().parent.parent.parent
"""Absolute path to the repository root (``data-platform-lab/``)."""

SAMPLE_DIR: Path = REPO_ROOT / "data" / "sample"
"""``data/sample/`` — committed sample datasets."""

SQL_DIR: Path = REPO_ROOT / "sql"
"""``sql/`` — SQL asset directory (DDL, DML, analytics, warehouse)."""


# ---------------------------------------------------------------------------
# File writers
# ---------------------------------------------------------------------------


def write_csv_text(path: Path, text: str) -> Path:
    """Write raw text as a CSV file and return the path.

    Useful for tests that need precise control over CSV content,
    including malformed files.
    """
    path.write_text(text, encoding="utf-8")
    return path


def write_csv_rows(
    path: Path,
    headers: list[str],
    rows: list[list[str]],
) -> Path:
    """Write a CSV file from structured headers and rows."""
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)
    return path


def write_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    """Write a list of dicts as a newline-delimited JSON (JSONL) file."""
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_dir() -> Path:
    """Return the path to ``data/sample/``."""
    return SAMPLE_DIR


@pytest.fixture()
def sql_dir() -> Path:
    """Return the path to ``sql/``."""
    return SQL_DIR
