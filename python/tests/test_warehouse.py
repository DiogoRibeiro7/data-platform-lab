from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from data_platform_lab.warehouse.loader import (
    load_raw_csv,
    load_raw_events_json,
    run_sql_file,
    run_warehouse_pipeline,
)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = REPO_ROOT / "data" / "sample"
SQL_DIR = REPO_ROOT / "sql"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_staging_table(conn: sqlite3.Connection, table_name: str, columns: list[str]) -> None:
    """Create a simple staging table for unit tests."""
    col_defs = ", ".join(f"{c} TEXT" for c in columns)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})")
    conn.commit()


def _make_events_table(conn: sqlite3.Connection) -> None:
    """Create the events staging table matching the DDL."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id    TEXT,
            type        TEXT,
            user_id     TEXT,
            page        TEXT,
            product_id  TEXT,
            quantity    INTEGER,
            order_id    TEXT,
            timestamp   TEXT
        )
        """
    )
    conn.commit()


# ---------------------------------------------------------------------------
# 1. test_load_raw_csv
# ---------------------------------------------------------------------------


def test_load_raw_csv(tmp_path: Path) -> None:
    csv_file = tmp_path / "items.csv"
    csv_file.write_text("id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n3,Doohickey,4.50\n")

    conn = sqlite3.connect(":memory:")
    _make_staging_table(conn, "items", ["id", "name", "price"])

    count = load_raw_csv(conn, "items", csv_file)
    assert count == 3

    rows = conn.execute("SELECT COUNT(*) FROM items").fetchone()[0]
    assert rows == 3
    conn.close()


# ---------------------------------------------------------------------------
# 2. test_load_raw_csv_handles_empty_values
# ---------------------------------------------------------------------------


def test_load_raw_csv_handles_empty_values(tmp_path: Path) -> None:
    csv_file = tmp_path / "sparse.csv"
    csv_file.write_text("id,name,email\n1,Alice,alice@test.com\n2,Bob,\n3,,charlie@test.com\n")

    conn = sqlite3.connect(":memory:")
    _make_staging_table(conn, "sparse", ["id", "name", "email"])

    load_raw_csv(conn, "sparse", csv_file)

    # Bob's email should be NULL, not empty string
    row = conn.execute("SELECT email FROM sparse WHERE id = '2'").fetchone()
    assert row[0] is None

    # Row 3's name should be NULL
    row = conn.execute("SELECT name FROM sparse WHERE id = '3'").fetchone()
    assert row[0] is None

    conn.close()


# ---------------------------------------------------------------------------
# 3. test_load_raw_events_json
# ---------------------------------------------------------------------------


def test_load_raw_events_json(tmp_path: Path) -> None:
    events_file = tmp_path / "events.json"
    events = [
        {
            "event_id": "e1",
            "type": "page_view",
            "user_id": "u1",
            "page": "/home",
            "product_id": None,
            "quantity": None,
            "order_id": None,
            "timestamp": "2025-01-15T10:00:00",
        },
        {
            "event_id": "e2",
            "type": "add_to_cart",
            "user_id": "u2",
            "page": "/product/1",
            "product_id": "p1",
            "quantity": 2,
            "order_id": None,
            "timestamp": "2025-01-15T11:00:00",
        },
    ]
    events_file.write_text("\n".join(json.dumps(e) for e in events) + "\n")

    conn = sqlite3.connect(":memory:")
    _make_events_table(conn)

    count = load_raw_events_json(conn, events_file)
    assert count == 2

    rows = conn.execute("SELECT * FROM events").fetchall()
    assert len(rows) == 2

    # Verify field values for the first event
    row = conn.execute("SELECT type, user_id, page FROM events WHERE event_id = 'e1'").fetchone()
    assert row == ("page_view", "u1", "/home")

    conn.close()


# ---------------------------------------------------------------------------
# 4. test_load_raw_events_json_handles_nulls
# ---------------------------------------------------------------------------


def test_load_raw_events_json_handles_nulls(tmp_path: Path) -> None:
    events_file = tmp_path / "events.json"
    events = [
        {
            "event_id": "e1",
            "type": "page_view",
            "user_id": None,
            "page": "/home",
            "product_id": None,
            "quantity": None,
            "order_id": None,
            "timestamp": "2025-01-15T10:00:00",
        },
    ]
    events_file.write_text(json.dumps(events[0]) + "\n")

    conn = sqlite3.connect(":memory:")
    _make_events_table(conn)

    count = load_raw_events_json(conn, events_file)
    assert count == 1

    row = conn.execute("SELECT user_id FROM events WHERE event_id = 'e1'").fetchone()
    assert row[0] is None

    conn.close()


# ---------------------------------------------------------------------------
# 5. test_run_sql_file
# ---------------------------------------------------------------------------


def test_run_sql_file(tmp_path: Path) -> None:
    sql_file = tmp_path / "setup.sql"
    sql_file.write_text(
        "CREATE TABLE test_tbl (id INTEGER PRIMARY KEY, val TEXT);\n"
        "INSERT INTO test_tbl VALUES (1, 'hello');\n"
    )

    conn = sqlite3.connect(":memory:")
    run_sql_file(conn, sql_file)

    # Table should exist and contain one row
    row = conn.execute("SELECT val FROM test_tbl WHERE id = 1").fetchone()
    assert row[0] == "hello"

    conn.close()


# ---------------------------------------------------------------------------
# 6. test_run_sql_file_multiple_statements
# ---------------------------------------------------------------------------


def test_run_sql_file_multiple_statements(tmp_path: Path) -> None:
    sql_file = tmp_path / "multi.sql"
    sql_file.write_text(
        "CREATE TABLE a (x INTEGER);\n"
        "CREATE TABLE b (y TEXT);\n"
        "INSERT INTO a VALUES (42);\n"
        "INSERT INTO b VALUES ('foo');\n"
        "INSERT INTO b VALUES ('bar');\n"
    )

    conn = sqlite3.connect(":memory:")
    run_sql_file(conn, sql_file)

    a_count = conn.execute("SELECT COUNT(*) FROM a").fetchone()[0]
    b_count = conn.execute("SELECT COUNT(*) FROM b").fetchone()[0]
    assert a_count == 1
    assert b_count == 2

    conn.close()


# ---------------------------------------------------------------------------
# 7. test_run_warehouse_pipeline_sample_data
# ---------------------------------------------------------------------------


def test_run_warehouse_pipeline_sample_data(tmp_path: Path) -> None:
    db_path = str(tmp_path / "warehouse.db")
    result = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )

    assert result["status"] == "success"

    # Staging row counts
    st = result["staging_tables"]
    assert st["customers"] == 13
    assert st["products"] == 12
    assert st["orders"] == 15
    assert st["order_items"] == 20
    assert st["events"] == 20

    # Warehouse row counts
    wh = result["warehouse_tables"]
    assert wh["dim_customer"] == 12
    assert wh["dim_product"] == 11
    assert wh["dim_date"] == 366
    assert wh["fact_order"] == 14
    assert wh["fact_order_item"] == 19
    assert wh["fact_event"] == 19

    # Analytical queries
    assert len(result["queries"]) == 5
    for q in result["queries"]:
        assert q["row_count"] > 0


# ---------------------------------------------------------------------------
# 8. test_run_warehouse_pipeline_with_report_dir
# ---------------------------------------------------------------------------


def test_run_warehouse_pipeline_with_report_dir(tmp_path: Path) -> None:
    db_path = str(tmp_path / "warehouse.db")
    report_dir = tmp_path / "reports"

    result = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        report_dir=str(report_dir),
        sql_dir=SQL_DIR,
    )
    assert result["status"] == "success"

    # CSV files for each query
    for q in result["queries"]:
        csv_path = report_dir / f"{q['name']}.csv"
        assert csv_path.exists(), f"Missing report CSV: {csv_path.name}"

    # Summary JSON
    summary_path = report_dir / "warehouse_summary.json"
    assert summary_path.exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "success"
    assert "staging_tables" in summary
    assert "warehouse_tables" in summary
    assert len(summary["queries"]) == 5


# ---------------------------------------------------------------------------
# 9. test_run_warehouse_pipeline_missing_data_dir
# ---------------------------------------------------------------------------


def test_run_warehouse_pipeline_missing_data_dir(tmp_path: Path) -> None:
    db_path = str(tmp_path / "warehouse.db")
    missing_dir = tmp_path / "nonexistent"

    result = run_warehouse_pipeline(
        data_dir=missing_dir,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )

    # Should not crash — either succeeds with empty tables or fails gracefully
    assert result["status"] in ("success", "failed")


# ---------------------------------------------------------------------------
# 10. test_run_warehouse_pipeline_idempotent
# ---------------------------------------------------------------------------


def test_run_warehouse_pipeline_idempotent(tmp_path: Path) -> None:
    db_path = str(tmp_path / "warehouse.db")

    result1 = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )
    assert result1["status"] == "success"

    result2 = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )
    assert result2["status"] == "success"

    # Staging tables use INSERT OR REPLACE with primary keys, so counts stay stable
    assert result1["staging_tables"] == result2["staging_tables"]

    # Dimension tables use INSERT OR REPLACE with UNIQUE constraints, so counts stay stable
    for dim in ("dim_customer", "dim_product", "dim_date"):
        assert result1["warehouse_tables"][dim] == result2["warehouse_tables"][dim]


# ---------------------------------------------------------------------------
# 11. test_warehouse_dim_customer_deduplication
# ---------------------------------------------------------------------------


def test_warehouse_dim_customer_deduplication(tmp_path: Path) -> None:
    """dim_customer should have 12 rows, not 13, because duplicate C003 is deduplicated."""
    db_path = str(tmp_path / "warehouse.db")
    result = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )
    assert result["status"] == "success"
    assert result["staging_tables"]["customers"] == 13
    assert result["warehouse_tables"]["dim_customer"] == 12


# ---------------------------------------------------------------------------
# 12. test_warehouse_dim_product_filters_negative_price
# ---------------------------------------------------------------------------


def test_warehouse_dim_product_filters_negative_price(tmp_path: Path) -> None:
    """dim_product should have 11 rows, not 12, because P009 with negative price is filtered."""
    db_path = str(tmp_path / "warehouse.db")
    result = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )
    assert result["status"] == "success"
    assert result["staging_tables"]["products"] == 12
    assert result["warehouse_tables"]["dim_product"] == 11

    # Verify P009 specifically is absent
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT COUNT(*) FROM dim_product WHERE product_id = 'P009'").fetchone()
    assert row[0] == 0
    conn.close()


# ---------------------------------------------------------------------------
# 13. test_warehouse_fact_order_skips_orphan_fk
# ---------------------------------------------------------------------------


def test_warehouse_fact_order_skips_orphan_fk(tmp_path: Path) -> None:
    """fact_order has 14 rows, not 15: ORD-008 references nonexistent C099."""
    db_path = str(tmp_path / "warehouse.db")
    result = run_warehouse_pipeline(
        data_dir=DATA_DIR,
        db_path=db_path,
        sql_dir=SQL_DIR,
    )
    assert result["status"] == "success"
    assert result["staging_tables"]["orders"] == 15
    assert result["warehouse_tables"]["fact_order"] == 14

    # Verify ORD-008 specifically is absent
    conn = sqlite3.connect(db_path)
    row = conn.execute("SELECT COUNT(*) FROM fact_order WHERE order_id = 'ORD-008'").fetchone()
    assert row[0] == 0
    conn.close()
