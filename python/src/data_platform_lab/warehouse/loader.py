"""Warehouse loader — build a star-schema warehouse in SQLite from raw data.

Loads raw CSV/JSONL files into staging tables, executes SQL-based transforms
to populate dimension and fact tables, then runs analytical queries against
the resulting star schema.

Run from the ``python/`` directory::

    poetry run python -m data_platform_lab.warehouse.cli

Or call ``run_warehouse_pipeline(...)`` directly from Python.
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from data_platform_lab.manifest import write_manifest

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Analytical queries — each is a (name, description, sql) tuple
# ---------------------------------------------------------------------------

WAREHOUSE_QUERIES: list[tuple[str, str, str]] = [
    (
        "warehouse_row_counts",
        "Row counts for all warehouse tables",
        """
        SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count FROM dim_customer
        UNION ALL
        SELECT 'dim_product', COUNT(*) FROM dim_product
        UNION ALL
        SELECT 'dim_date', COUNT(*) FROM dim_date
        UNION ALL
        SELECT 'fact_order', COUNT(*) FROM fact_order
        UNION ALL
        SELECT 'fact_order_item', COUNT(*) FROM fact_order_item
        UNION ALL
        SELECT 'fact_event', COUNT(*) FROM fact_event;
        """,
    ),
    (
        "revenue_by_status",
        "Total revenue grouped by order status",
        """
        SELECT fo.status,
               COUNT(*)            AS order_count,
               ROUND(SUM(fo.total), 2) AS total_revenue
        FROM fact_order fo
        GROUP BY fo.status
        ORDER BY total_revenue DESC;
        """,
    ),
    (
        "top_products_warehouse",
        "Top products by revenue from the warehouse layer",
        """
        SELECT dp.product_id,
               dp.name           AS product_name,
               dp.category,
               SUM(fi.quantity)  AS units_sold,
               ROUND(SUM(fi.line_total), 2) AS total_revenue
        FROM fact_order_item fi
        JOIN dim_product dp ON dp.product_key = fi.product_key
        GROUP BY dp.product_id, dp.name, dp.category
        ORDER BY total_revenue DESC;
        """,
    ),
    (
        "daily_warehouse_revenue",
        "Daily revenue from the warehouse layer",
        """
        SELECT dd.date_key,
               dd.day_of_week,
               dd.month_name,
               COUNT(*)              AS order_count,
               ROUND(SUM(fo.total), 2) AS daily_revenue
        FROM fact_order fo
        JOIN dim_date dd ON dd.date_key = fo.order_date_key
        GROUP BY dd.date_key
        ORDER BY dd.date_key;
        """,
    ),
    (
        "customer_spend_warehouse",
        "Customer spend from the warehouse layer",
        """
        SELECT dc.customer_id,
               dc.first_name || ' ' || dc.last_name AS full_name,
               dc.country,
               COUNT(fo.order_key)          AS order_count,
               ROUND(SUM(fo.total), 2)      AS total_spend
        FROM dim_customer dc
        LEFT JOIN fact_order fo ON fo.customer_key = dc.customer_key
        GROUP BY dc.customer_id, full_name, dc.country
        ORDER BY total_spend DESC;
        """,
    ),
]

# ---------------------------------------------------------------------------
# CSV/JSON staging loaders
# ---------------------------------------------------------------------------

# Column names for the events staging table, matching the DDL in 05_events.sql.
_EVENTS_COLUMNS = (
    "event_id",
    "type",
    "user_id",
    "page",
    "product_id",
    "quantity",
    "order_id",
    "timestamp",
)


def load_raw_csv(conn: sqlite3.Connection, table_name: str, csv_path: Path) -> int:
    """Load a CSV file into a staging table. Return the number of rows loaded.

    Uses ``INSERT OR REPLACE`` so that re-runs and duplicate primary keys in
    the raw data do not cause constraint errors.
    """
    if not csv_path.exists():
        logger.warning("CSV not found: %s — table %s will be empty", csv_path, table_name)
        return 0

    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames is not None
        columns = reader.fieldnames
        placeholders = ", ".join("?" for _ in columns)
        col_list = ", ".join(columns)
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({col_list}) VALUES ({placeholders})"

        rows = 0
        for row in reader:
            values = [row[col] if row[col] != "" else None for col in columns]
            conn.execute(insert_sql, values)
            rows += 1

    conn.commit()
    logger.info("Loaded %d rows into staging.%s", rows, table_name)
    return rows


def load_raw_events_json(conn: sqlite3.Connection, events_path: Path) -> int:
    """Load a JSONL events file into the staging ``events`` table.

    Each line is a JSON object whose keys match the events DDL columns.
    Returns the number of rows loaded.
    """
    if not events_path.exists():
        logger.warning("Events file not found: %s — events table will be empty", events_path)
        return 0

    placeholders = ", ".join("?" for _ in _EVENTS_COLUMNS)
    col_list = ", ".join(_EVENTS_COLUMNS)
    insert_sql = f"INSERT INTO events ({col_list}) VALUES ({placeholders})"

    rows = 0
    with events_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            values = [record.get(col) for col in _EVENTS_COLUMNS]
            conn.execute(insert_sql, values)
            rows += 1

    conn.commit()
    logger.info("Loaded %d rows into staging.events", rows)
    return rows


# ---------------------------------------------------------------------------
# SQL file executor
# ---------------------------------------------------------------------------


def run_sql_file(conn: sqlite3.Connection, sql_path: Path) -> None:
    """Read and execute a SQL file, handling multiple statements split by ``;``."""
    if not sql_path.exists():
        logger.warning("SQL file not found: %s — skipping", sql_path)
        return

    text = sql_path.read_text(encoding="utf-8")
    statements = [s.strip() for s in text.split(";") if s.strip()]
    for stmt in statements:
        conn.execute(stmt)
    conn.commit()
    logger.info("Executed SQL file: %s (%d statement(s))", sql_path.name, len(statements))


# ---------------------------------------------------------------------------
# Query runner helpers
# ---------------------------------------------------------------------------


def _run_query(conn: sqlite3.Connection, sql: str) -> list[dict[str, Any]]:
    """Execute a SQL query and return results as a list of dicts."""
    cursor = conn.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _write_report_csv(rows: list[dict[str, Any]], path: Path) -> None:
    """Write query results to a CSV file."""
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Main warehouse pipeline
# ---------------------------------------------------------------------------

# Mapping of raw CSV filenames to staging table names.
_CSV_TO_TABLE: list[tuple[str, str]] = [
    ("customers.csv", "customers"),
    ("products.csv", "products"),
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
]


def run_warehouse_pipeline(
    data_dir: str | Path,
    db_path: str = ":memory:",
    report_dir: str | Path | None = None,
    sql_dir: str | Path | None = None,
) -> dict[str, Any]:
    """Build the warehouse star schema end-to-end and return a summary.

    Parameters
    ----------
    data_dir:
        Directory containing ``customers.csv``, ``products.csv``, ``orders.csv``,
        ``order_items.csv``, and ``events.json``.
    db_path:
        SQLite database path.  Defaults to ``:memory:``.
    report_dir:
        If provided, query results are written here as CSV and a summary JSON.
    sql_dir:
        Root of the SQL asset tree (containing ``ddl/``, ``dml/``, ``warehouse/``).
        Defaults to ``../sql`` relative to the Python package root.
    """
    data_dir = Path(data_dir)

    # Resolve sql_dir — default to <repo>/sql relative to the package.
    sql_root = Path(sql_dir) if sql_dir is not None else Path(__file__).resolve().parents[4] / "sql"

    ddl_dir = sql_root / "ddl"
    dml_dir = sql_root / "dml"
    warehouse_dir = sql_root / "warehouse"

    status = "success"
    staging_tables: dict[str, int] = {}
    warehouse_tables: dict[str, int] = {}
    query_results: list[dict[str, Any]] = []

    try:
        # 1. Connect
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # 2. DDL — create staging and warehouse tables
        ddl_files = sorted(ddl_dir.glob("*.sql"))
        for ddl_file in ddl_files:
            run_sql_file(conn, ddl_file)

        # 3. Load raw CSVs into staging tables
        for csv_name, table_name in _CSV_TO_TABLE:
            csv_path = data_dir / csv_name
            count = load_raw_csv(conn, table_name, csv_path)
            staging_tables[table_name] = count

        # 4. Load events.json into staging events table
        events_path = data_dir / "events.json"
        staging_tables["events"] = load_raw_events_json(conn, events_path)

        # 5. Populate dim_date
        dim_date_path = dml_dir / "06_load_dim_date.sql"
        run_sql_file(conn, dim_date_path)

        # 6. Execute warehouse transforms (01-05 only)
        warehouse_files = sorted(warehouse_dir.glob("*.sql"))
        for wh_file in warehouse_files:
            # Only run the stage-to-dim/fact transforms (01-05)
            if wh_file.name[:2].isdigit() and int(wh_file.name[:2]) <= 5:
                run_sql_file(conn, wh_file)

        # 7. Gather warehouse table row counts
        for tbl in (
            "dim_customer",
            "dim_product",
            "dim_date",
            "fact_order",
            "fact_order_item",
            "fact_event",
        ):
            cursor = conn.execute(f"SELECT COUNT(*) FROM {tbl}")
            warehouse_tables[tbl] = cursor.fetchone()[0]

        # 8. Run analytical queries
        for name, description, sql in WAREHOUSE_QUERIES:
            rows = _run_query(conn, sql)
            query_results.append(
                {
                    "name": name,
                    "description": description,
                    "row_count": len(rows),
                    "rows": rows,
                }
            )
            logger.info("Query '%s': %d rows", name, len(rows))

        # 9. Write reports if requested
        if report_dir is not None:
            report_path = Path(report_dir)
            report_path.mkdir(parents=True, exist_ok=True)

            for qr in query_results:
                _write_report_csv(qr["rows"], report_path / f"{qr['name']}.csv")

            summary_out = {
                "db_path": db_path,
                "staging_tables": staging_tables,
                "warehouse_tables": warehouse_tables,
                "queries": [
                    {
                        "name": q["name"],
                        "description": q["description"],
                        "row_count": q["row_count"],
                    }
                    for q in query_results
                ],
                "status": status,
            }
            summary_json_path = report_path / "warehouse_summary.json"
            summary_json_path.write_text(json.dumps(summary_out, indent=2), encoding="utf-8")
            logger.info("Reports written to %s", report_path)

        conn.close()

    except Exception:
        logger.exception("Warehouse pipeline failed")
        status = "failed"

    manifest_path = write_manifest(
        pipeline_name="warehouse",
        run_id=datetime.now(UTC).strftime("%Y%m%d_%H%M%S"),
        source=str(data_dir),
        output=str(report_dir) if report_dir else db_path,
        row_count=sum(warehouse_tables.values()),
        status=status,
        extras={
            "staging_tables": staging_tables,
            "warehouse_tables": warehouse_tables,
            "db_path": db_path,
        },
    )

    return {
        "db_path": db_path,
        "staging_tables": staging_tables,
        "warehouse_tables": warehouse_tables,
        "queries": query_results,
        "status": status,
        "manifest_path": str(manifest_path),
    }
