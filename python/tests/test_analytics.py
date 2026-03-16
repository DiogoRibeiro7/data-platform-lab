"""Tests for the SQLite analytics layer."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.analytics import (
    QUERIES,
    create_database,
    run_analytics,
    run_query,
)
from data_platform_lab.demo import run_demo


SAMPLE_DIR = Path(__file__).parent.parent.parent / "data" / "sample"


def _produce_silver(tmp_path: Path) -> Path:
    """Run the demo to produce curated CSVs, return silver dir."""
    silver_dir = tmp_path / "silver"
    run_demo(
        data_dir=str(SAMPLE_DIR),
        output_dir=str(silver_dir),
        manifest_dir=str(tmp_path / "manifests"),
    )
    return silver_dir


class TestCreateDatabase:
    def test_loads_all_tables(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)
        conn = create_database(silver_dir)

        counts = {}
        for table in ("customers", "products", "orders", "order_items"):
            row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
            counts[table] = row["n"]

        assert counts["customers"] == 12
        assert counts["products"] == 11
        assert counts["orders"] == 15
        assert counts["order_items"] == 19

        conn.close()


class TestQueries:
    def test_daily_revenue(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)
        conn = create_database(silver_dir)

        rows = run_query(conn, "daily_revenue", QUERIES[0][2])

        assert len(rows) > 0
        assert "order_date" in rows[0]
        assert "daily_revenue" in rows[0]
        assert all(r["daily_revenue"] > 0 for r in rows)

        conn.close()

    def test_top_products(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)
        conn = create_database(silver_dir)

        rows = run_query(conn, "top_products", QUERIES[1][2])

        assert len(rows) > 0
        assert rows[0]["product_name"] == "Mechanical Keyboard"
        # Revenue should be descending
        revenues = [r["total_revenue"] for r in rows]
        assert revenues == sorted(revenues, reverse=True)

        conn.close()

    def test_customer_orders(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)
        conn = create_database(silver_dir)

        rows = run_query(conn, "customer_orders", QUERIES[2][2])

        assert len(rows) == 12  # 12 unique customers after dedup
        # LEFT JOIN: customers with 0 orders should appear
        zero_order = [r for r in rows if r["order_count"] == 0]
        assert len(zero_order) >= 1

        conn.close()

    def test_orphan_fks(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)
        conn = create_database(silver_dir)

        rows = run_query(conn, "orphan_fks", QUERIES[3][2])

        assert len(rows) == 1
        assert rows[0]["missing_customer_id"] == "C099"

        conn.close()

    def test_duplicate_detection_clean(self, tmp_path: Path) -> None:
        """After the demo pipeline cleans data, no duplicates should remain."""
        silver_dir = _produce_silver(tmp_path)
        conn = create_database(silver_dir)

        rows = run_query(conn, "duplicates", QUERIES[4][2])

        assert len(rows) == 0

        conn.close()


class TestRunAnalytics:
    def test_end_to_end(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)
        report_dir = tmp_path / "gold"

        result = run_analytics(
            silver_dir=str(silver_dir),
            report_dir=str(report_dir),
        )

        # All 5 queries ran
        assert len(result["queries"]) == 5

        # Report CSVs written
        for q in result["queries"]:
            csv_path = report_dir / f"{q['name']}.csv"
            assert csv_path.exists(), f"{q['name']}.csv missing"

        # Summary JSON written
        summary_path = Path(result["summary_path"])
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        assert len(summary["queries"]) == 5
        assert summary["tables_loaded"] == [
            "customers", "products", "orders", "order_items"
        ]

    def test_query_row_counts(self, tmp_path: Path) -> None:
        silver_dir = _produce_silver(tmp_path)

        result = run_analytics(
            silver_dir=str(silver_dir),
            report_dir=str(tmp_path / "gold"),
        )

        counts = {q["name"]: q["row_count"] for q in result["queries"]}
        assert counts["daily_revenue"] == 11
        assert counts["top_products"] == 10
        assert counts["customer_orders"] == 12
        assert counts["orphan_foreign_keys"] == 1
        assert counts["duplicate_detection"] == 0
