"""Tests for the CDC snapshot comparison tool."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from data_platform_lab.transform.snapshot_diff import (
    ColumnChange,
    DiffSummary,
    RowChange,
    compare_rows,
    compare_snapshots,
    format_summary,
    index_by_key,
    read_snapshot,
    write_diff_files,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def write_csv(path: Path, headers: list[str], rows: list[list[str]]) -> None:
    """Write a CSV file from headers and rows."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


# ===================================================================
# read_snapshot
# ===================================================================


class TestReadSnapshot:
    """Tests for :func:`read_snapshot`."""

    def test_read_snapshot(self, tmp_path: Path) -> None:
        """Reads CSV, returns headers and row dicts."""
        path = tmp_path / "data.csv"
        write_csv(
            path,
            ["id", "name", "value"],
            [["1", "Alice", "100"], ["2", "Bob", "200"]],
        )

        headers, rows = read_snapshot(path)

        assert headers == ["id", "name", "value"]
        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "Alice", "value": "100"}
        assert rows[1] == {"id": "2", "name": "Bob", "value": "200"}

    def test_read_snapshot_strips_whitespace(self, tmp_path: Path) -> None:
        """Whitespace in headers and values is trimmed."""
        path = tmp_path / "data.csv"
        # Write raw content with extra whitespace
        path.write_text(
            " id , name , value \n 1 , Alice , 100 \n 2 , Bob , 200 \n",
            encoding="utf-8",
        )

        headers, rows = read_snapshot(path)

        assert headers == ["id", "name", "value"]
        assert rows[0] == {"id": "1", "name": "Alice", "value": "100"}
        assert rows[1] == {"id": "2", "name": "Bob", "value": "200"}


# ===================================================================
# index_by_key
# ===================================================================


class TestIndexByKey:
    """Tests for :func:`index_by_key`."""

    def test_index_by_key_single_column(self) -> None:
        """Indexes rows by a single PK column."""
        rows = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": "Bob"},
        ]

        index = index_by_key(rows, ["id"])

        assert index[("1",)] == {"id": "1", "name": "Alice"}
        assert index[("2",)] == {"id": "2", "name": "Bob"}
        assert len(index) == 2

    def test_index_by_key_composite(self) -> None:
        """Indexes by composite key (two columns)."""
        rows = [
            {"region": "EU", "id": "1", "name": "Alice"},
            {"region": "US", "id": "1", "name": "Bob"},
            {"region": "EU", "id": "2", "name": "Carla"},
        ]

        index = index_by_key(rows, ["region", "id"])

        assert index[("EU", "1")] == {"region": "EU", "id": "1", "name": "Alice"}
        assert index[("US", "1")] == {"region": "US", "id": "1", "name": "Bob"}
        assert index[("EU", "2")] == {"region": "EU", "id": "2", "name": "Carla"}
        assert len(index) == 3

    def test_index_by_key_duplicate_raises(self) -> None:
        """Raises ValueError on duplicate keys."""
        rows = [
            {"id": "1", "name": "Alice"},
            {"id": "1", "name": "Bob"},
        ]

        with pytest.raises(ValueError, match="Duplicate key"):
            index_by_key(rows, ["id"])

    def test_index_by_key_missing_column_raises(self) -> None:
        """Raises KeyError when PK column not in data."""
        rows = [{"id": "1", "name": "Alice"}]

        with pytest.raises(KeyError, match="not found in data"):
            index_by_key(rows, ["missing_column"])


# ===================================================================
# compare_rows
# ===================================================================


class TestCompareRows:
    """Tests for :func:`compare_rows`."""

    def test_compare_rows_no_changes(self) -> None:
        """Identical rows return empty list."""
        row = {"id": "1", "name": "Alice", "value": "100"}

        changes = compare_rows(row, dict(row), key_columns=["id"])

        assert changes == []

    def test_compare_rows_with_changes(self) -> None:
        """Differing rows return correct ColumnChange list."""
        old_row = {"id": "1", "name": "Alice", "value": "100"}
        new_row = {"id": "1", "name": "Alice", "value": "200"}

        changes = compare_rows(old_row, new_row, key_columns=["id"])

        assert len(changes) == 1
        assert changes[0].column == "value"
        assert changes[0].old_value == "100"
        assert changes[0].new_value == "200"

    def test_compare_rows_ignores_key_columns(self) -> None:
        """Key columns excluded from comparison."""
        old_row = {"id": "1", "name": "Alice"}
        new_row = {"id": "2", "name": "Alice"}

        changes = compare_rows(old_row, new_row, key_columns=["id"])

        # Even though 'id' differs, it should be excluded
        assert changes == []

    def test_compare_rows_ignores_specified_columns(self) -> None:
        """ignore_columns respected."""
        old_row = {"id": "1", "name": "Alice", "updated_at": "2024-01-01"}
        new_row = {"id": "1", "name": "Alice", "updated_at": "2024-06-01"}

        changes = compare_rows(
            old_row, new_row, key_columns=["id"], ignore_columns=["updated_at"]
        )

        assert changes == []


# ===================================================================
# compare_snapshots
# ===================================================================


class TestCompareSnapshots:
    """Tests for :func:`compare_snapshots`."""

    def test_compare_snapshots_pure_inserts(self, tmp_path: Path) -> None:
        """New snapshot has only new rows."""
        old_path = tmp_path / "old.csv"
        new_path = tmp_path / "new.csv"

        write_csv(old_path, ["id", "name"], [["1", "Alice"]])
        write_csv(
            new_path,
            ["id", "name"],
            [["1", "Alice"], ["2", "Bob"], ["3", "Carla"]],
        )

        result = compare_snapshots(old_path, new_path, key_columns=["id"])

        assert result.inserts == 2
        assert result.updates == 0
        assert result.deletes == 0
        assert result.unchanged == 1

    def test_compare_snapshots_pure_deletes(self, tmp_path: Path) -> None:
        """Old snapshot rows missing from new."""
        old_path = tmp_path / "old.csv"
        new_path = tmp_path / "new.csv"

        write_csv(
            old_path,
            ["id", "name"],
            [["1", "Alice"], ["2", "Bob"], ["3", "Carla"]],
        )
        write_csv(new_path, ["id", "name"], [["1", "Alice"]])

        result = compare_snapshots(old_path, new_path, key_columns=["id"])

        assert result.inserts == 0
        assert result.updates == 0
        assert result.deletes == 2
        assert result.unchanged == 1

    def test_compare_snapshots_pure_updates(self, tmp_path: Path) -> None:
        """Same keys, changed values."""
        old_path = tmp_path / "old.csv"
        new_path = tmp_path / "new.csv"

        write_csv(
            old_path,
            ["id", "name", "value"],
            [["1", "Alice", "100"], ["2", "Bob", "200"]],
        )
        write_csv(
            new_path,
            ["id", "name", "value"],
            [["1", "Alice", "150"], ["2", "Bob", "250"]],
        )

        result = compare_snapshots(old_path, new_path, key_columns=["id"])

        assert result.inserts == 0
        assert result.updates == 2
        assert result.deletes == 0
        assert result.unchanged == 0

        # Verify the column change details
        update_1 = next(c for c in result.changes if c.key == {"id": "1"})
        assert len(update_1.changed_columns) == 1
        assert update_1.changed_columns[0].column == "value"
        assert update_1.changed_columns[0].old_value == "100"
        assert update_1.changed_columns[0].new_value == "150"

    def test_compare_snapshots_mixed(self) -> None:
        """Use the actual sample data. Expected: 2 inserts, 1 delete, 3 updates, 3 unchanged."""
        old_path = Path(__file__).parent.parent.parent / "data" / "sample" / "old_snapshot.csv"
        new_path = Path(__file__).parent.parent.parent / "data" / "sample" / "new_snapshot.csv"

        result = compare_snapshots(old_path, new_path, key_columns=["customer_id"])

        assert result.inserts == 2
        assert result.deletes == 1
        assert result.updates == 3
        assert result.unchanged == 3
        assert result.old_row_count == 7
        assert result.new_row_count == 8

        # Verify specific changes
        inserts = [c for c in result.changes if c.change_type == "insert"]
        deletes = [c for c in result.changes if c.change_type == "delete"]
        updates = [c for c in result.changes if c.change_type == "update"]

        insert_keys = {c.key["customer_id"] for c in inserts}
        assert insert_keys == {"C008", "C009"}

        delete_keys = {c.key["customer_id"] for c in deletes}
        assert delete_keys == {"C004"}

        update_keys = {c.key["customer_id"] for c in updates}
        assert update_keys == {"C001", "C002", "C005"}

    def test_compare_snapshots_no_changes(self, tmp_path: Path) -> None:
        """Identical snapshots."""
        path = tmp_path / "snapshot.csv"
        write_csv(
            path,
            ["id", "name"],
            [["1", "Alice"], ["2", "Bob"]],
        )

        result = compare_snapshots(path, path, key_columns=["id"])

        assert result.inserts == 0
        assert result.updates == 0
        assert result.deletes == 0
        assert result.unchanged == 2
        assert result.changes == []

    def test_compare_snapshots_duplicate_keys_raises(self, tmp_path: Path) -> None:
        """Duplicate PK in either file raises ValueError."""
        old_path = tmp_path / "old.csv"
        new_path = tmp_path / "new.csv"

        write_csv(
            old_path,
            ["id", "name"],
            [["1", "Alice"], ["1", "Bob"]],
        )
        write_csv(new_path, ["id", "name"], [["1", "Alice"]])

        with pytest.raises(ValueError, match="Duplicate key"):
            compare_snapshots(old_path, new_path, key_columns=["id"])

    def test_compare_snapshots_missing_key_raises(self, tmp_path: Path) -> None:
        """Missing PK column raises KeyError."""
        old_path = tmp_path / "old.csv"
        new_path = tmp_path / "new.csv"

        write_csv(old_path, ["id", "name"], [["1", "Alice"]])
        write_csv(new_path, ["id", "name"], [["1", "Alice"]])

        with pytest.raises(KeyError, match="not found in data"):
            compare_snapshots(old_path, new_path, key_columns=["nonexistent"])

    def test_compare_snapshots_with_ignore_columns(self, tmp_path: Path) -> None:
        """Ignored columns not counted as changes."""
        old_path = tmp_path / "old.csv"
        new_path = tmp_path / "new.csv"

        write_csv(
            old_path,
            ["id", "name", "updated_at"],
            [["1", "Alice", "2024-01-01"]],
        )
        write_csv(
            new_path,
            ["id", "name", "updated_at"],
            [["1", "Alice", "2024-06-01"]],
        )

        result = compare_snapshots(
            old_path, new_path, key_columns=["id"], ignore_columns=["updated_at"]
        )

        assert result.updates == 0
        assert result.unchanged == 1


# ===================================================================
# write_diff_files
# ===================================================================


class TestWriteDiffFiles:
    """Tests for :func:`write_diff_files`."""

    def test_write_diff_files(self, tmp_path: Path) -> None:
        """Writes correct CSV files and summary.json."""
        summary = DiffSummary(
            old_row_count=3,
            new_row_count=4,
            inserts=1,
            updates=1,
            deletes=1,
            unchanged=1,
            changes=[
                RowChange(
                    change_type="delete",
                    key={"id": "2"},
                    row={"id": "2", "name": "Bob", "value": "200"},
                ),
                RowChange(
                    change_type="insert",
                    key={"id": "3"},
                    row={"id": "3", "name": "Carla", "value": "300"},
                ),
                RowChange(
                    change_type="update",
                    key={"id": "1"},
                    row={"id": "1", "name": "Alice", "value": "150"},
                    changed_columns=[
                        ColumnChange(column="value", old_value="100", new_value="150"),
                    ],
                ),
            ],
        )

        output_dir = tmp_path / "output"
        result = write_diff_files(summary, output_dir)

        # All four files should be written
        assert "inserts" in result
        assert "updates" in result
        assert "deletes" in result
        assert "summary" in result

        # Check inserts.csv
        with result["inserts"].open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            insert_rows = list(reader)
        assert len(insert_rows) == 1
        assert insert_rows[0]["id"] == "3"
        assert insert_rows[0]["name"] == "Carla"

        # Check updates.csv
        with result["updates"].open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            update_rows = list(reader)
        assert len(update_rows) == 1
        assert update_rows[0]["id"] == "1"
        assert update_rows[0]["value"] == "150"
        assert update_rows[0]["changed_columns"] == "value"

        # Check deletes.csv
        with result["deletes"].open("r", newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            delete_rows = list(reader)
        assert len(delete_rows) == 1
        assert delete_rows[0]["id"] == "2"

        # Check summary.json
        with result["summary"].open("r", encoding="utf-8") as fh:
            summary_data = json.load(fh)
        assert summary_data["inserts"] == 1
        assert summary_data["updates"] == 1
        assert summary_data["deletes"] == 1
        assert summary_data["unchanged"] == 1
        assert summary_data["old_row_count"] == 3
        assert summary_data["new_row_count"] == 4

    def test_write_diff_files_skips_empty(self, tmp_path: Path) -> None:
        """Doesn't write files for empty categories."""
        summary = DiffSummary(
            old_row_count=2,
            new_row_count=3,
            inserts=1,
            updates=0,
            deletes=0,
            unchanged=2,
            changes=[
                RowChange(
                    change_type="insert",
                    key={"id": "3"},
                    row={"id": "3", "name": "Carla"},
                ),
            ],
        )

        output_dir = tmp_path / "output"
        result = write_diff_files(summary, output_dir)

        assert "inserts" in result
        assert "updates" not in result
        assert "deletes" not in result
        assert "summary" in result

        # Confirm files do not exist on disk
        assert not (output_dir / "updates.csv").exists()
        assert not (output_dir / "deletes.csv").exists()


# ===================================================================
# format_summary
# ===================================================================


class TestFormatSummary:
    """Tests for :func:`format_summary`."""

    def test_format_summary(self) -> None:
        """Returns human-readable string."""
        summary = DiffSummary(
            old_row_count=7,
            new_row_count=8,
            inserts=2,
            updates=3,
            deletes=1,
            unchanged=3,
            changes=[
                RowChange(
                    change_type="delete",
                    key={"customer_id": "C004"},
                    row={"customer_id": "C004", "name": "David"},
                ),
                RowChange(
                    change_type="insert",
                    key={"customer_id": "C008"},
                    row={"customer_id": "C008", "name": "Hugo"},
                ),
                RowChange(
                    change_type="update",
                    key={"customer_id": "C001"},
                    row={"customer_id": "C001", "name": "Alice"},
                    changed_columns=[
                        ColumnChange(column="city", old_value="Lisbon", new_value="Porto"),
                    ],
                ),
            ],
        )

        text = format_summary(summary)

        assert "Snapshot Diff Summary" in text
        assert "Old snapshot rows : 7" in text
        assert "New snapshot rows : 8" in text
        assert "Inserts           : 2" in text
        assert "Updates           : 3" in text
        assert "Deletes           : 1" in text
        assert "Unchanged         : 3" in text
        assert "[INSERT]" in text
        assert "[DELETE]" in text
        assert "[UPDATE]" in text
        assert "city: 'Lisbon' -> 'Porto'" in text
