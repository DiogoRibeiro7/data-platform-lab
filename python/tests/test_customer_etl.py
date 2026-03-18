"""Tests for the orchestrated customer ETL workflow."""

from __future__ import annotations

from pathlib import Path

from data_platform_lab.orchestration.customer_etl import (
    clean,
    extract,
    load,
    report,
    run_customer_etl,
    validate,
)
from data_platform_lab.orchestration.runner import format_result

SAMPLE_CSV = (
    "customer_id,first_name,last_name,email,city,country,created_at\n"
    "C001,Alice,Martins,alice@example.com,Lisbon,Portugal,2024-01-15\n"
    "C002,Bob,Silva,bob@example.com,Porto,Portugal,2024-02-20\n"
    "C001,Alice,Martins,alice@example.com,Lisbon,Portugal,2024-01-15\n"
)


def _write_sample(tmp_path: Path) -> Path:
    p = tmp_path / "customers.csv"
    p.write_text(SAMPLE_CSV, encoding="utf-8")
    return p


# ===================================================================
# Individual step tests
# ===================================================================


class TestExtractStep:
    def test_extract_reads_csv(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        ctx: dict = {"input_path": str(csv_path)}
        result = extract(ctx)

        assert result["rows_read"] == 3
        assert result["columns"] == 7
        assert len(ctx["headers"]) == 7
        assert ctx["headers"][0] == "customer_id"

    def test_extract_trims_and_standardises(self, tmp_path: Path) -> None:
        p = tmp_path / "messy.csv"
        p.write_text("  First Name , Last Name \nAlice , Martins \n", encoding="utf-8")
        ctx: dict = {"input_path": str(p)}
        extract(ctx)

        assert ctx["headers"] == ["first_name", "last_name"]
        assert ctx["rows"] == [["Alice", "Martins"]]


class TestValidateStep:
    def test_validate_reports_results(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        ctx: dict = {"input_path": str(csv_path)}
        extract(ctx)
        result = validate(ctx)

        assert result["total_checks"] == 4
        assert "validation_report" in ctx

    def test_validate_detects_quality_issues(self, tmp_path: Path) -> None:
        p = tmp_path / "bad.csv"
        p.write_text(
            "customer_id,email,created_at\nC001,a@b.com,2024-01-15\nC001,c@d.com,not-a-date\n",
            encoding="utf-8",
        )
        ctx: dict = {"input_path": str(p)}
        extract(ctx)
        result = validate(ctx)

        assert result["failed"] > 0


class TestCleanStep:
    def test_clean_deduplicates(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        ctx: dict = {"input_path": str(csv_path)}
        extract(ctx)
        result = clean(ctx)

        assert result["duplicates_removed"] == 1
        assert result["rows_after"] == 2


class TestLoadStep:
    def test_load_writes_csv(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        output_path = tmp_path / "output" / "cleaned.csv"
        ctx: dict = {"input_path": str(csv_path), "output_path": str(output_path)}
        extract(ctx)
        clean(ctx)
        result = load(ctx)

        assert result["rows_written"] == 2
        assert output_path.exists()
        lines = output_path.read_text(encoding="utf-8").strip().split("\n")
        assert lines[0] == "customer_id,first_name,last_name,email,city,country,created_at"
        assert len(lines) == 3  # header + 2 data rows


class TestReportStep:
    def test_report_includes_validation(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        ctx: dict = {"input_path": str(csv_path)}
        extract(ctx)
        validate(ctx)
        text = report(ctx)

        assert "Validation Report" in text


# ===================================================================
# End-to-end pipeline tests
# ===================================================================


class TestRunCustomerEtl:
    def test_end_to_end_success(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        output_path = tmp_path / "output" / "cleaned.csv"

        result = run_customer_etl(str(csv_path), str(output_path))

        assert result.status == "success"
        assert result.pipeline_name == "customer_etl"
        assert len(result.steps) == 5
        assert result.steps_passed == 5
        assert result.steps_failed == 0
        assert output_path.exists()

        # Verify step results are accessible
        extract_result = result.steps[0].result
        assert extract_result["rows_read"] == 3

        clean_result = result.steps[2].result
        assert clean_result["duplicates_removed"] == 1

        load_result = result.steps[3].result
        assert load_result["rows_written"] == 2

    def test_end_to_end_with_real_sample_data(self) -> None:
        """Run against the actual sample data file."""
        sample = Path(__file__).parent.parent.parent / "data" / "sample" / "customers.csv"
        if not sample.exists():
            return  # skip if sample data not available

        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "cleaned.csv"
            result = run_customer_etl(str(sample), str(output))

            assert result.status == "success"
            # 13 rows in, 1 duplicate (C003), so 12 out
            assert result.steps[0].result["rows_read"] == 13
            assert result.steps[2].result["duplicates_removed"] == 1
            assert result.steps[3].result["rows_written"] == 12

    def test_format_result_produces_readable_output(self, tmp_path: Path) -> None:
        csv_path = _write_sample(tmp_path)
        output_path = tmp_path / "output" / "cleaned.csv"
        result = run_customer_etl(str(csv_path), str(output_path))
        text = format_result(result)

        assert "customer_etl" in text
        assert "success" in text
        assert "[PASS] extract" in text
        assert "[PASS] load" in text

    def test_missing_file_fails_at_extract(self, tmp_path: Path) -> None:
        result = run_customer_etl(
            str(tmp_path / "nonexistent.csv"),
            str(tmp_path / "out.csv"),
        )

        assert result.status == "failed"
        assert result.steps[0].status == "failed"
        assert result.steps_failed == 1
        # Pipeline stops after extract failure — remaining steps not executed
        assert len(result.steps) == 1
