"""SQLite analytics layer for the e-commerce demo.

Loads curated CSV outputs into an in-memory (or file-backed) SQLite database
and runs a set of analytical queries.  Each query returns its results as a
list of dicts and is also written to a report directory as CSV.

Designed to run after the main demo pipeline has produced cleaned CSVs in
``data/silver/demo/``.

Run from the ``python/`` directory::

    poetry run python -m data_platform_lab.analytics

Or with custom paths::

    poetry run python -m data_platform_lab.analytics \\
        --silver-dir ../data/silver/demo \\
        --report-dir ../data/gold/reports
"""

from __future__ import annotations

import csv
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Analytical queries — each is a (name, description, sql) tuple
# ---------------------------------------------------------------------------

QUERIES: list[tuple[str, str, str]] = [
    (
        "daily_revenue",
        "Revenue by date for completed orders",
        """\
SELECT
    order_date,
    COUNT(*)              AS order_count,
    ROUND(SUM(total), 2)  AS daily_revenue,
    ROUND(AVG(total), 2)  AS avg_order_value
FROM orders
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date;
""",
    ),
    (
        "top_products",
        "Products ranked by total revenue",
        """\
SELECT
    p.product_id,
    p.name             AS product_name,
    p.category,
    SUM(oi.quantity)   AS units_sold,
    ROUND(SUM(oi.line_total), 2) AS total_revenue,
    COUNT(DISTINCT oi.order_id)  AS order_count
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name, p.category
ORDER BY total_revenue DESC;
""",
    ),
    (
        "customer_orders",
        "Order count and total spend per customer",
        """\
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.country,
    COUNT(o.order_id)           AS order_count,
    COALESCE(ROUND(SUM(o.total), 2), 0) AS total_spend
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, full_name, c.country
ORDER BY total_spend DESC;
""",
    ),
    (
        "orphan_foreign_keys",
        "Orders referencing non-existent customers",
        """\
SELECT
    o.order_id,
    o.customer_id AS missing_customer_id,
    o.order_date,
    o.total
FROM orders o
LEFT JOIN customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL;
""",
    ),
    (
        "duplicate_detection",
        "Duplicate rows across tables",
        """\
SELECT 'customers' AS table_name,
       customer_id AS duplicate_key,
       COUNT(*)    AS occurrences
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'order_items' AS table_name,
       order_id || '|' || product_id || '|' || quantity AS duplicate_key,
       COUNT(*) AS occurrences
FROM order_items
GROUP BY order_id, product_id, quantity
HAVING COUNT(*) > 1;
""",
    ),
]

# ---------------------------------------------------------------------------
# CSV -> SQLite loader
# ---------------------------------------------------------------------------

TABLE_SCHEMAS: dict[str, str] = {
    "customers": """\
CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    first_name  TEXT,
    last_name   TEXT,
    email       TEXT,
    city        TEXT,
    country     TEXT,
    created_at  TEXT
);""",
    "products": """\
CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    name       TEXT,
    category   TEXT,
    price      REAL,
    currency   TEXT,
    stock      INTEGER,
    active     TEXT
);""",
    "orders": """\
CREATE TABLE orders (
    order_id         TEXT PRIMARY KEY,
    customer_id      TEXT,
    order_date       TEXT,
    status           TEXT,
    total            REAL,
    shipping_country TEXT
);""",
    "order_items": """\
CREATE TABLE order_items (
    order_id   TEXT,
    product_id TEXT,
    quantity   INTEGER,
    unit_price REAL,
    line_total REAL
);""",
}


def load_csv_into_table(conn: sqlite3.Connection, table_name: str, csv_path: Path) -> int:
    """Load a CSV file into an existing SQLite table. Returns row count."""
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames is not None
        columns = reader.fieldnames
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        rows = 0
        for row in reader:
            values = [row[col] if row[col] != "" else None for col in columns]
            conn.execute(insert_sql, values)
            rows += 1

    conn.commit()
    logger.info("Loaded %d rows into %s", rows, table_name)
    return rows


def create_database(silver_dir: Path, db_path: str = ":memory:") -> sqlite3.Connection:
    """Create a SQLite database and load all curated CSVs into it.

    Parameters
    ----------
    silver_dir : Path
        Directory containing the curated CSV files (customers.csv, etc.).
    db_path : str
        SQLite database path. Defaults to ``:memory:`` for an in-memory DB.

    Returns
    -------
    sqlite3.Connection
        Open connection with all tables populated.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    for table_name, ddl in TABLE_SCHEMAS.items():
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(ddl)

    load_summary: dict[str, int] = {}
    for table_name in TABLE_SCHEMAS:
        csv_path = silver_dir / f"{table_name}.csv"
        if csv_path.exists():
            load_summary[table_name] = load_csv_into_table(conn, table_name, csv_path)
        else:
            logger.warning("CSV not found: %s — table %s will be empty", csv_path, table_name)
            load_summary[table_name] = 0

    return conn


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------


def run_query(conn: sqlite3.Connection, name: str, sql: str) -> list[dict[str, Any]]:
    """Execute a SQL query and return results as a list of dicts."""
    cursor = conn.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def write_report_csv(rows: list[dict[str, Any]], path: Path) -> None:
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
# Main analytics pipeline
# ---------------------------------------------------------------------------


def run_analytics(
    silver_dir: str | Path = "data/silver/demo",
    report_dir: str | Path = "data/gold/reports",
    db_path: str = ":memory:",
) -> dict[str, Any]:
    """Load curated data into SQLite and run all analytical queries.

    Returns a dict with ``db_path``, ``tables_loaded``, and ``queries``
    containing each query's name, description, row_count, and rows.
    """
    silver_dir = Path(silver_dir)
    report_dir = Path(report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)

    conn = create_database(silver_dir, db_path)

    query_results: list[dict[str, Any]] = []

    for name, description, sql in QUERIES:
        rows = run_query(conn, name, sql)
        write_report_csv(rows, report_dir / f"{name}.csv")
        query_results.append(
            {
                "name": name,
                "description": description,
                "row_count": len(rows),
                "rows": rows,
            }
        )
        logger.info("Query '%s': %d rows", name, len(rows))

    # Write a summary JSON
    summary = {
        "db_path": db_path,
        "tables_loaded": list(TABLE_SCHEMAS.keys()),
        "queries": [
            {"name": q["name"], "description": q["description"], "row_count": q["row_count"]}
            for q in query_results
        ],
    }
    summary_path = report_dir / "analytics_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    conn.close()

    return {
        "summary_path": str(summary_path),
        "report_dir": str(report_dir),
        "queries": query_results,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Load curated CSVs into SQLite and run analytical queries.",
    )
    parser.add_argument(
        "--silver-dir",
        default="../data/silver/demo",
        help="Directory containing curated CSVs from the demo pipeline.",
    )
    parser.add_argument(
        "--report-dir",
        default="../data/gold/reports",
        help="Directory for analytical report CSVs.",
    )
    parser.add_argument(
        "--db-path",
        default=":memory:",
        help="SQLite database path (default: in-memory).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    result = run_analytics(args.silver_dir, args.report_dir, args.db_path)

    print()
    print("=== Analytics Report ===")
    print()
    for q in result["queries"]:
        print(f"  {q['name']}: {q['row_count']} rows")
        if q["rows"]:
            # Print first 3 rows as preview
            for row in q["rows"][:3]:
                cols = ", ".join(f"{k}={v}" for k, v in row.items())
                print(f"    {cols}")
            if len(q["rows"]) > 3:
                print(f"    ... ({len(q['rows']) - 3} more)")
        print()
    print(f"Reports written to: {result['report_dir']}")
    print(f"Summary: {result['summary_path']}")


if __name__ == "__main__":
    main()
