"""Tests for the end-to-end e-commerce demo pipeline."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.demo import run_demo

SAMPLE_DIR = Path(__file__).parent.parent.parent / "data" / "sample"


class TestRunDemo:
    """End-to-end demo tests using the actual sample data."""

    def test_happy_path(self, tmp_path: Path) -> None:
        """Full run against sample data produces correct counts."""
        output_dir = tmp_path / "silver"
        manifest_dir = tmp_path / "manifests"

        result = run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(output_dir),
            manifest_dir=str(manifest_dir),
        )

        meta = result["metadata"]
        tables = result["tables"]

        # Overall counts
        assert meta.status == "success"
        assert meta.rows_read == 60  # 13+12+15+20
        assert meta.rows_written == 57  # 12+11+15+19
        assert meta.rows_rejected == 3  # 1 dup customer + 1 bad price + 1 dup order item
        assert meta.files_processed == 4

        # Per-table counts
        assert tables["customers"]["rows_read"] == 13
        assert tables["customers"]["rows_out"] == 12
        assert tables["customers"]["duplicates_removed"] == 1

        assert tables["products"]["rows_read"] == 12
        assert tables["products"]["rows_out"] == 11
        assert tables["products"]["rows_filtered"] == 1

        assert tables["orders"]["rows_read"] == 15
        assert tables["orders"]["rows_out"] == 15
        assert tables["orders"]["orphan_customer_ids"] == 1

        assert tables["order_items"]["rows_read"] == 20
        assert tables["order_items"]["rows_out"] == 19
        assert tables["order_items"]["duplicates_removed"] == 1

    def test_output_files_created(self, tmp_path: Path) -> None:
        """Cleaned CSVs are written to the output directory."""
        output_dir = tmp_path / "silver"
        manifest_dir = tmp_path / "manifests"

        run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(output_dir),
            manifest_dir=str(manifest_dir),
        )

        for name in ("customers.csv", "products.csv", "orders.csv", "order_items.csv"):
            path = output_dir / name
            assert path.exists(), f"{name} not created"
            lines = path.read_text(encoding="utf-8").strip().split("\n")
            assert len(lines) >= 2, f"{name} has no data rows"

    def test_manifest_written(self, tmp_path: Path) -> None:
        """A JSON manifest is written with run metadata and table summaries."""
        output_dir = tmp_path / "silver"
        manifest_dir = tmp_path / "manifests"

        result = run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(output_dir),
            manifest_dir=str(manifest_dir),
        )

        manifest_path = Path(result["manifest_path"])
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        assert manifest["run"]["status"] == "success"
        assert manifest["run"]["rows_read"] == 60
        assert "customers" in manifest["tables"]
        assert "products" in manifest["tables"]
        assert "orders" in manifest["tables"]
        assert "order_items" in manifest["tables"]

    def test_warnings_captured(self, tmp_path: Path) -> None:
        """Known data quality issues generate warnings."""
        result = run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(tmp_path / "out"),
            manifest_dir=str(tmp_path / "mf"),
        )

        warnings = result["metadata"].warnings
        assert any("duplicate" in w for w in warnings)
        assert any("invalid price" in w for w in warnings)
        assert any("non-existent customer_id" in w for w in warnings)

    def test_golden_output_customers(self, tmp_path: Path) -> None:
        """Curated customers.csv has exact expected content."""
        output_dir = tmp_path / "silver"

        run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(output_dir),
            manifest_dir=str(tmp_path / "mf"),
        )

        lines = (output_dir / "customers.csv").read_text(encoding="utf-8").strip().split("\n")
        # 12 data rows + 1 header = 13 lines
        assert len(lines) == 13
        assert lines[0] == "customer_id,first_name,last_name,email,city,country,created_at"
        # C003 duplicate removed — should appear exactly once
        c003_rows = [row for row in lines[1:] if row.startswith("C003,")]
        assert len(c003_rows) == 1
        # No negative-price product P009 in products.csv
        product_lines = (
            (output_dir / "products.csv").read_text(encoding="utf-8").strip().split("\n")
        )
        p009_rows = [row for row in product_lines[1:] if row.startswith("P009,")]
        assert len(p009_rows) == 0

    def test_manifest_json_shape(self, tmp_path: Path) -> None:
        """Manifest JSON has the exact expected structure."""
        result = run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(tmp_path / "silver"),
            manifest_dir=str(tmp_path / "mf"),
        )

        manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))

        # Top-level keys
        assert set(manifest.keys()) == {"run", "tables"}

        # Run metadata shape
        run = manifest["run"]
        expected_run_keys = {
            "pipeline_name",
            "run_id",
            "status",
            "started_at",
            "ended_at",
            "duration_seconds",
            "rows_read",
            "rows_written",
            "rows_rejected",
            "files_processed",
            "files_rejected",
            "warnings",
            "errors",
            "extra",
        }
        assert set(run.keys()) == expected_run_keys

        # Table summary shape
        for table_name in ("customers", "products", "orders", "order_items"):
            assert table_name in manifest["tables"]
            table = manifest["tables"][table_name]
            assert "source" in table
            assert "rows_read" in table
            assert "rows_out" in table
            assert "validation_status" in table

    def test_rerun_produces_identical_output(self, tmp_path: Path) -> None:
        """Running the demo twice produces identical silver CSVs."""
        for run_name in ("run1", "run2"):
            run_demo(
                data_dir=str(SAMPLE_DIR),
                output_dir=str(tmp_path / run_name / "silver"),
                manifest_dir=str(tmp_path / run_name / "mf"),
            )

        for name in ("customers.csv", "products.csv", "orders.csv", "order_items.csv"):
            content1 = (tmp_path / "run1" / "silver" / name).read_text(encoding="utf-8")
            content2 = (tmp_path / "run2" / "silver" / name).read_text(encoding="utf-8")
            assert content1 == content2, f"{name} differs between runs"

    def test_customer_country_standardised(self, tmp_path: Path) -> None:
        """Country casing is normalised to title case."""
        output_dir = tmp_path / "silver"

        run_demo(
            data_dir=str(SAMPLE_DIR),
            output_dir=str(output_dir),
            manifest_dir=str(tmp_path / "mf"),
        )

        lines = (output_dir / "customers.csv").read_text(encoding="utf-8").strip().split("\n")
        # All country values should be title case
        header_row = lines[0].split(",")
        country_idx = header_row.index("country")
        for line in lines[1:]:
            country = line.split(",")[country_idx]
            assert country == country.title(), f"Country not title-cased: {country!r}"
