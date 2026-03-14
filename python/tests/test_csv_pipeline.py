"""Tests for the CSV ingestion and cleaning pipeline."""

from __future__ import annotations

from pathlib import Path

from data_platform_lab.ingestion.csv_pipeline import (
    PipelineResult,
    deduplicate,
    read_csv_file,
    run_pipeline,
    standardize_headers,
    trim_fields,
    validate_columns,
)


# ---------------------------------------------------------------------------
# Helper to write a small CSV inside a tmp directory
# ---------------------------------------------------------------------------

def _write_csv(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


class TestReadCsvFile:
    def test_read_csv_file(self, tmp_path: Path) -> None:
        """Reads a valid CSV and returns headers + rows."""
        csv_text = "id,name,age\n1,Alice,30\n2,Bob,25\n"
        p = _write_csv(tmp_path / "data.csv", csv_text)

        headers, rows = read_csv_file(p)

        assert headers == ["id", "name", "age"]
        assert rows == [["1", "Alice", "30"], ["2", "Bob", "25"]]


class TestValidateColumns:
    def test_validate_columns_pass(self) -> None:
        """All required columns are present."""
        headers = ["id", "name", "email"]
        missing = validate_columns(headers, ["id", "email"])
        assert missing == []

    def test_validate_columns_missing(self) -> None:
        """Some required columns are missing."""
        headers = ["id", "name"]
        missing = validate_columns(headers, ["id", "email", "phone"])
        assert missing == ["email", "phone"]


class TestStandardizeHeaders:
    def test_standardize_headers(self) -> None:
        """Spaces, mixed case, and leading/trailing whitespace."""
        raw = [" First Name ", "LAST NAME", "  Email Address  ", "age"]
        result = standardize_headers(raw)
        assert result == ["first_name", "last_name", "email_address", "age"]


class TestTrimFields:
    def test_trim_fields(self) -> None:
        """Strips whitespace from every cell."""
        rows = [["  a ", " b", "c  "], ["  x  ", "y", " z "]]
        result = trim_fields(rows)
        assert result == [["a", "b", "c"], ["x", "y", "z"]]


class TestDeduplicate:
    def test_deduplicate(self) -> None:
        """Removes exact duplicates and reports count removed."""
        rows = [
            ["1", "Alice"],
            ["2", "Bob"],
            ["1", "Alice"],
            ["3", "Carla"],
            ["2", "Bob"],
        ]
        unique, removed = deduplicate(rows)
        assert removed == 2
        assert unique == [["1", "Alice"], ["2", "Bob"], ["3", "Carla"]]


# ---------------------------------------------------------------------------
# Integration / pipeline tests
# ---------------------------------------------------------------------------


class TestRunPipeline:
    def test_run_pipeline_valid(self, tmp_path: Path) -> None:
        """Full pipeline merges two valid CSVs, deduplicates, and writes."""
        _write_csv(
            tmp_path / "a.csv",
            "id,name\n1,Alice\n2,Bob\n",
        )
        _write_csv(
            tmp_path / "b.csv",
            "id,name\n2,Bob\n3,Carla\n",
        )
        out = tmp_path / "out.csv"
        result = run_pipeline(tmp_path, out)

        assert isinstance(result, PipelineResult)
        assert set(result.files_processed) == {"a.csv", "b.csv"}
        assert result.files_rejected == []
        assert result.rows_read == 4
        assert result.duplicates_removed == 1
        assert result.rows_written == 3
        assert out.exists()

        lines = out.read_text(encoding="utf-8").splitlines()
        assert lines[0] == "id,name"
        assert len(lines) == 4  # header + 3 data rows

    def test_run_pipeline_missing_columns(self, tmp_path: Path) -> None:
        """Files missing required columns are rejected."""
        _write_csv(
            tmp_path / "good.csv",
            "id,name,email\n1,Alice,a@b.com\n",
        )
        _write_csv(
            tmp_path / "bad.csv",
            "id,name\n2,Bob\n",
        )
        out = tmp_path / "out.csv"
        result = run_pipeline(
            tmp_path, out, required_columns=["id", "name", "email"],
        )

        assert "good.csv" in result.files_processed
        assert any("bad.csv" in r for r in result.files_rejected)
        assert result.rows_written == 1

    def test_run_pipeline_empty_folder(self, tmp_path: Path) -> None:
        """Empty input directory yields an empty result."""
        out = tmp_path / "out.csv"
        result = run_pipeline(tmp_path, out)

        assert result.files_processed == []
        assert result.rows_read == 0
        assert result.rows_written == 0
        assert out.exists()

    def test_run_pipeline_malformed_csv(self, tmp_path: Path) -> None:
        """Handles a CSV with inconsistent column counts gracefully."""
        _write_csv(
            tmp_path / "messy.csv",
            "id,name,email\n1,Alice,a@b.com\n2,Bob\n3,Carla,c@d.com,extra\n",
        )
        out = tmp_path / "out.csv"
        result = run_pipeline(tmp_path, out)

        # The file itself is processed; only individual bad rows are skipped.
        assert "messy.csv" in result.files_processed
        assert result.rows_read == 3
        # Only the row with exactly 3 fields survives.
        assert result.rows_written == 1
