"""CDC snapshot comparison tool.

Compares two CSV snapshots (old vs new) and produces a detailed diff
identifying inserted, updated, deleted, and unchanged rows.  Useful for
change-data-capture pipelines that rely on periodic full-table exports.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ColumnChange:
    """A single column-level change within an updated row."""

    column: str
    old_value: str
    new_value: str


@dataclass
class RowChange:
    """A changed row with its change type and details."""

    change_type: str  # "insert", "update", "delete"
    key: dict[str, str]  # primary key columns -> values
    row: dict[str, str]  # full row data (new for insert/update, old for delete)
    changed_columns: list[ColumnChange] = field(default_factory=list)


@dataclass
class DiffSummary:
    """Summary of a snapshot comparison."""

    old_row_count: int
    new_row_count: int
    inserts: int
    updates: int
    deletes: int
    unchanged: int
    changes: list[RowChange] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_snapshot(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read a CSV snapshot, return (headers, list of row dicts).

    Uses the stdlib csv module.  Strip whitespace from headers and values.
    """
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        raw_fieldnames: list[str] = list(reader.fieldnames or [])
        headers = [h.strip() for h in raw_fieldnames]

        rows: list[dict[str, str]] = []
        for raw_row in reader:
            row = {k.strip(): v.strip() for k, v in raw_row.items()}
            rows.append(row)

    logger.info("Read %d rows from %s", len(rows), path)
    return headers, rows


# ---------------------------------------------------------------------------
# Indexing
# ---------------------------------------------------------------------------


def index_by_key(
    rows: list[dict[str, str]],
    key_columns: list[str],
) -> dict[tuple[str, ...], dict[str, str]]:
    """Index a list of row dicts by composite primary key.

    Returns a dict mapping key tuples to row dicts.
    Raises ``ValueError`` if a duplicate key is found.
    Raises ``KeyError`` if a key column is missing from the data.
    """
    if rows:
        first_row = rows[0]
        for col in key_columns:
            if col not in first_row:
                raise KeyError(
                    f"Key column '{col}' not found in data. "
                    f"Available columns: {sorted(first_row.keys())}"
                )

    index: dict[tuple[str, ...], dict[str, str]] = {}
    for row in rows:
        key = tuple(row[col] for col in key_columns)
        if key in index:
            raise ValueError(
                f"Duplicate key {dict(zip(key_columns, key, strict=False))} found in data"
            )
        index[key] = row

    return index


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def compare_rows(
    old_row: dict[str, str],
    new_row: dict[str, str],
    key_columns: list[str],
    ignore_columns: list[str] | None = None,
) -> list[ColumnChange]:
    """Compare two rows and return a list of :class:`ColumnChange` objects.

    Excludes key columns and ignored columns from comparison.
    """
    skip = set(key_columns)
    if ignore_columns:
        skip.update(ignore_columns)

    changes: list[ColumnChange] = []
    for col in old_row:
        if col in skip:
            continue
        old_val = old_row[col]
        new_val = new_row.get(col, "")
        if old_val != new_val:
            changes.append(ColumnChange(column=col, old_value=old_val, new_value=new_val))

    return changes


# ---------------------------------------------------------------------------
# Main comparison
# ---------------------------------------------------------------------------


def compare_snapshots(
    old_path: Path,
    new_path: Path,
    key_columns: list[str],
    ignore_columns: list[str] | None = None,
) -> DiffSummary:
    """Compare two CSV snapshots and produce a :class:`DiffSummary`.

    Algorithm:
    1. Read both snapshots
    2. Index both by *key_columns*
    3. Keys in new but not old -> inserts
    4. Keys in old but not new -> deletes
    5. Keys in both -> compare columns; if any differ -> update, else -> unchanged
    6. Sort changes by key for determinism

    Raises ``ValueError`` if duplicate keys found in either snapshot.
    Raises ``KeyError`` if *key_columns* are missing from headers.
    """
    _old_headers, old_rows = read_snapshot(old_path)
    _new_headers, new_rows = read_snapshot(new_path)

    old_index = index_by_key(old_rows, key_columns)
    new_index = index_by_key(new_rows, key_columns)

    changes: list[RowChange] = []
    unchanged_count = 0

    # Inserts: keys in new but not in old
    for key, row in new_index.items():
        if key not in old_index:
            changes.append(
                RowChange(
                    change_type="insert",
                    key=dict(zip(key_columns, key, strict=False)),
                    row=row,
                )
            )

    # Deletes: keys in old but not in new
    for key, row in old_index.items():
        if key not in new_index:
            changes.append(
                RowChange(
                    change_type="delete",
                    key=dict(zip(key_columns, key, strict=False)),
                    row=row,
                )
            )

    # Updates / unchanged: keys in both
    for key in old_index:
        if key in new_index:
            col_changes = compare_rows(old_index[key], new_index[key], key_columns, ignore_columns)
            if col_changes:
                changes.append(
                    RowChange(
                        change_type="update",
                        key=dict(zip(key_columns, key, strict=False)),
                        row=new_index[key],
                        changed_columns=col_changes,
                    )
                )
            else:
                unchanged_count += 1

    # Sort for determinism
    changes.sort(key=lambda c: tuple(c.key.values()))

    insert_count = sum(1 for c in changes if c.change_type == "insert")
    update_count = sum(1 for c in changes if c.change_type == "update")
    delete_count = sum(1 for c in changes if c.change_type == "delete")

    summary = DiffSummary(
        old_row_count=len(old_rows),
        new_row_count=len(new_rows),
        inserts=insert_count,
        updates=update_count,
        deletes=delete_count,
        unchanged=unchanged_count,
        changes=changes,
    )

    logger.info(
        "Snapshot diff complete: %d inserts, %d updates, %d deletes, %d unchanged",
        insert_count,
        update_count,
        delete_count,
        unchanged_count,
    )

    return summary


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def write_diff_files(
    summary: DiffSummary,
    output_dir: Path,
) -> dict[str, Path]:
    """Write inserts.csv, updates.csv, deletes.csv, and summary.json to *output_dir*.

    For ``updates.csv``, include all columns plus a ``changed_columns`` column
    listing the names of columns that changed (comma-separated).

    Returns a dict mapping file type to path.  Only writes files that have data.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    result: dict[str, Path] = {}

    inserts = [c for c in summary.changes if c.change_type == "insert"]
    updates = [c for c in summary.changes if c.change_type == "update"]
    deletes = [c for c in summary.changes if c.change_type == "delete"]

    # --- inserts.csv ---
    if inserts:
        path = output_dir / "inserts.csv"
        fieldnames = list(inserts[0].row.keys())
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for change in inserts:
                writer.writerow(change.row)
        result["inserts"] = path

    # --- updates.csv ---
    if updates:
        path = output_dir / "updates.csv"
        fieldnames = [*list(updates[0].row.keys()), "changed_columns"]
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for change in updates:
                row = dict(change.row)
                row["changed_columns"] = ",".join(cc.column for cc in change.changed_columns)
                writer.writerow(row)
        result["updates"] = path

    # --- deletes.csv ---
    if deletes:
        path = output_dir / "deletes.csv"
        fieldnames = list(deletes[0].row.keys())
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for change in deletes:
                writer.writerow(change.row)
        result["deletes"] = path

    # --- summary.json ---
    summary_path = output_dir / "summary.json"
    summary_dict = asdict(summary)
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary_dict, fh, indent=2)
    result["summary"] = summary_path

    logger.info("Wrote diff files to %s", output_dir)
    return result


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------


def format_summary(summary: DiffSummary) -> str:
    """Return a human-readable summary string."""
    lines = [
        "Snapshot Diff Summary",
        "=" * 40,
        f"Old snapshot rows : {summary.old_row_count}",
        f"New snapshot rows : {summary.new_row_count}",
        "-" * 40,
        f"Inserts           : {summary.inserts}",
        f"Updates           : {summary.updates}",
        f"Deletes           : {summary.deletes}",
        f"Unchanged         : {summary.unchanged}",
        "-" * 40,
    ]

    if summary.changes:
        lines.append("Details:")
        for change in summary.changes:
            key_str = ", ".join(f"{k}={v}" for k, v in change.key.items())
            if change.change_type == "update":
                col_details = "; ".join(
                    f"{cc.column}: {cc.old_value!r} -> {cc.new_value!r}"
                    for cc in change.changed_columns
                )
                lines.append(f"  [{change.change_type.upper()}] {key_str} ({col_details})")
            else:
                lines.append(f"  [{change.change_type.upper()}] {key_str}")

    lines.append("=" * 40)
    return "\n".join(lines)
