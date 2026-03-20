"""Tests for the benchmark runner module."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from data_platform_lab.benchmark.runner import (
    BenchmarkReport,
    FileResult,
    StrategyResult,
    format_report,
    generate_test_files,
    process_file,
    run_async,
    run_benchmark,
    run_sequential,
    run_threaded,
    save_report,
)

# ===================================================================
# File generation
# ===================================================================


class TestGenerateTestFiles:
    """generate_test_files creates the expected CSV files."""

    def test_generate_test_files(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=5, rows_per_file=20)

        assert len(files) == 5
        for f in files:
            assert f.exists()
            assert f.suffix == ".csv"
            with f.open(newline="", encoding="utf-8") as fh:
                reader = csv.reader(fh)
                rows = list(reader)
            # 1 header + 20 data rows
            assert len(rows) == 21, f"{f.name}: expected 21 rows, got {len(rows)}"

    def test_generate_test_files_quality_issues(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=1, rows_per_file=10)
        with files[0].open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

        empty_emails = [r for r in rows if not r["email"]]
        casing_issues = [
            r
            for r in rows
            if r["country"] != r["country"].title() and r["country"]
        ]

        # ~10% of 10 rows = 1 empty email (row_id % 10 == 0)
        assert len(empty_emails) >= 1
        # ~10% upper + ~10% lower = at least 1 casing issue
        assert len(casing_issues) >= 1


# ===================================================================
# Single-file processing
# ===================================================================


class TestProcessFile:
    """process_file reads, validates, cleans, and writes output."""

    def test_process_file(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=1, rows_per_file=20)
        output_dir = tmp_path / "output"

        result = process_file(files[0], output_dir)

        assert isinstance(result, FileResult)
        assert result.rows_read == 20
        # Generator never produces empty required fields, so all rows valid
        assert result.rows_valid == 20
        assert result.rows_invalid == 0
        assert result.duration_seconds >= 0

    def test_process_file_cleans_country_casing(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=1, rows_per_file=20)
        output_dir = tmp_path / "output"

        process_file(files[0], output_dir)

        output_file = output_dir / files[0].name
        with output_file.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                assert row["country"] == row["country"].title(), (
                    f"Country not title case: {row['country']!r}"
                )

    def test_process_file_lowercases_email(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=1, rows_per_file=20)
        output_dir = tmp_path / "output"

        process_file(files[0], output_dir)

        output_file = output_dir / files[0].name
        with output_file.open(newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                if row["email"]:
                    assert row["email"] == row["email"].lower(), (
                        f"Email not lowercased: {row['email']!r}"
                    )


# ===================================================================
# Strategy implementations
# ===================================================================


class TestRunSequential:
    """run_sequential processes files one at a time."""

    def test_run_sequential(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=3, rows_per_file=10)
        output_dir = tmp_path / "output_seq"

        results = run_sequential(files, output_dir)

        assert len(results) == 3
        for r in results:
            assert isinstance(r, FileResult)
            output_file = output_dir / r.file_name
            assert output_file.exists()


class TestRunThreaded:
    """run_threaded processes files with a thread pool."""

    def test_run_threaded(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=3, rows_per_file=10)
        output_dir = tmp_path / "output_thr"

        results = run_threaded(files, output_dir)

        assert len(results) == 3
        for r in results:
            assert isinstance(r, FileResult)
            output_file = output_dir / r.file_name
            assert output_file.exists()


class TestRunAsync:
    """run_async processes files with asyncio + thread executor."""

    def test_run_async(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=3, rows_per_file=10)
        output_dir = tmp_path / "output_async"

        results = run_async(files, output_dir)

        assert len(results) == 3
        for r in results:
            assert isinstance(r, FileResult)
            output_file = output_dir / r.file_name
            assert output_file.exists()


class TestAllStrategiesProduceSameCounts:
    """All three strategies must produce identical row counts."""

    def test_all_strategies_produce_same_counts(self, tmp_path: Path) -> None:
        files = generate_test_files(tmp_path / "input", num_files=5, rows_per_file=20)

        seq_results = run_sequential(files, tmp_path / "out_seq")
        thr_results = run_threaded(files, tmp_path / "out_thr")
        asc_results = run_async(files, tmp_path / "out_async")

        seq_read = sum(r.rows_read for r in seq_results)
        thr_read = sum(r.rows_read for r in thr_results)
        asc_read = sum(r.rows_read for r in asc_results)

        seq_valid = sum(r.rows_valid for r in seq_results)
        thr_valid = sum(r.rows_valid for r in thr_results)
        asc_valid = sum(r.rows_valid for r in asc_results)

        assert seq_read == thr_read == asc_read
        assert seq_valid == thr_valid == asc_valid


# ===================================================================
# Full benchmark
# ===================================================================


class TestRunBenchmark:
    """run_benchmark orchestrates generation + all strategies."""

    def test_run_benchmark(self, tmp_path: Path) -> None:
        report = run_benchmark(tmp_path, num_files=5, rows_per_file=20)

        assert isinstance(report, BenchmarkReport)
        assert report.num_files == 5
        assert report.rows_per_file == 20
        assert report.total_rows == 100
        assert len(report.strategies) == 3

    def test_run_benchmark_result_shape(self, tmp_path: Path) -> None:
        report = run_benchmark(tmp_path, num_files=5, rows_per_file=20)

        for s in report.strategies:
            assert isinstance(s, StrategyResult)
            assert s.total_seconds > 0
            assert s.files_processed == report.num_files
            assert s.total_rows_read == report.total_rows


# ===================================================================
# Reporting
# ===================================================================


class TestFormatReport:
    """format_report produces human-readable output."""

    def test_format_report(self, tmp_path: Path) -> None:
        report = run_benchmark(tmp_path, num_files=3, rows_per_file=10)
        text = format_report(report)

        assert "Benchmark Report" in text
        assert "sequential" in text
        assert "threaded" in text
        assert "async" in text
        assert "Relative to sequential" in text


class TestSaveReport:
    """save_report writes valid JSON with expected keys."""

    def test_save_report(self, tmp_path: Path) -> None:
        report = run_benchmark(tmp_path / "work", num_files=3, rows_per_file=10)
        json_path = tmp_path / "report.json"

        save_report(report, json_path)

        assert json_path.exists()
        data = json.loads(json_path.read_text(encoding="utf-8"))
        assert "num_files" in data
        assert "rows_per_file" in data
        assert "total_rows" in data
        assert "strategies" in data
        assert len(data["strategies"]) == 3
