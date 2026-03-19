# SQLite Analytics Demo

Loads the curated CSVs from the e-commerce demo into SQLite and runs five
analytical queries. Report CSVs and a JSON summary are written to
`data/gold/reports/`.

This is the final stage of the platform pipeline:

```
data/sample/ ──> demo pipeline ──> data/silver/demo/ ──> analytics ──> data/gold/reports/
   (raw)          (ingest+clean)      (curated)          (SQLite)       (reports)
```

---

## Prerequisites

Run the e-commerce demo first to produce the silver layer:

```bash
# Python
cd python && poetry run python -m data_platform_lab.demo

# JavaScript
cd javascript && node src/demo.js
```

---

## Run the analytics

### Python

```bash
cd python
poetry run python -m data_platform_lab.analytics
```

### JavaScript

Requires Node.js 22+ (uses the experimental `node:sqlite` module).

```bash
cd javascript
node src/analytics.js
```

### Custom paths

```bash
# Python
poetry run python -m data_platform_lab.analytics \
  --silver-dir ../data/silver/demo \
  --report-dir ../data/gold/reports

# JavaScript
node src/analytics.js \
  --silver-dir ../data/silver/demo \
  --report-dir ../data/gold/reports
```

---

## Queries

| # | Query | Description | Expected rows |
|---|-------|-------------|---------------|
| 1 | `daily_revenue` | Revenue by date for completed orders | 11 |
| 2 | `top_products` | Products ranked by total revenue | 10 |
| 3 | `customer_orders` | Order count and total spend per customer | 12 |
| 4 | `orphan_foreign_keys` | Orders referencing non-existent customers | 1 |
| 5 | `duplicate_detection` | Duplicate rows across tables | 0 |

The duplicate detection query returns 0 rows because the demo pipeline already
removed duplicates during the cleaning stage. The orphan FK query finds ORD-008
which references customer C099 (not in the customers table).

---

## Output

### Console

```
=== Analytics Report ===

  daily_revenue: 11 rows
    order_date=2024-06-01, order_count=1, daily_revenue=59.98, avg_order_value=59.98
    order_date=2024-06-03, order_count=1, daily_revenue=89.5, avg_order_value=89.5
    order_date=2024-06-05, order_count=1, daily_revenue=133.49, avg_order_value=133.49
    ... (8 more)

  top_products: 10 rows
    product_id=P002, product_name=Mechanical Keyboard, category=Electronics, units_sold=4, total_revenue=358.0, order_count=4
    product_id=P004, product_name=Standing Desk, category=Furniture, units_sold=1, total_revenue=349.99, order_count=1
    ...

  customer_orders: 12 rows
  orphan_foreign_keys: 1 rows
  duplicate_detection: 0 rows

Reports written to: data/gold/reports
Summary: data/gold/reports/analytics_summary.json
```

### Report files

```
data/gold/reports/
  daily_revenue.csv
  top_products.csv
  customer_orders.csv
  orphan_foreign_keys.csv
  duplicate_detection.csv
  analytics_summary.json
```

### Summary JSON

```json
{
  "db_path": ":memory:",
  "tables_loaded": ["customers", "products", "orders", "order_items"],
  "queries": [
    { "name": "daily_revenue", "description": "Revenue by date for completed orders", "row_count": 11 },
    { "name": "top_products", "description": "Products ranked by total revenue", "row_count": 10 },
    { "name": "customer_orders", "description": "Order count and total spend per customer", "row_count": 12 },
    { "name": "orphan_foreign_keys", "description": "Orders referencing non-existent customers", "row_count": 1 },
    { "name": "duplicate_detection", "description": "Duplicate rows across tables", "row_count": 0 }
  ]
}
```

---

## How it works

1. **Create tables** — DDL for customers, products, orders, order_items
2. **Load CSVs** — read each silver CSV and INSERT rows into SQLite
3. **Run queries** — execute each analytical SQL statement
4. **Write reports** — each query's results saved as a CSV file
5. **Write summary** — JSON manifest of what was produced

The database is in-memory by default. Pass `--db-path lab.db` to persist it
to disk for manual inspection:

```bash
poetry run python -m data_platform_lab.analytics --db-path ../lab.db
sqlite3 ../lab.db "SELECT * FROM orders LIMIT 5;"
```

---

## Relationship to sql/ directory

The `sql/analytics/` directory contains standalone SQL files for the same
queries (and more). This module embeds the SQL directly so it can run
programmatically and produce structured output. The standalone files are
useful for learning and manual exploration; this module is useful for
automated pipelines.

---

## Tests

```bash
# Python — 10 tests
cd python && poetry run pytest tests/test_analytics.py -v

# JavaScript — 5 tests
cd javascript && node --test tests/analytics.test.js
```

---

## File locations

| Language | Module | Tests |
|----------|--------|-------|
| Python | `python/src/data_platform_lab/analytics.py` | `python/tests/test_analytics.py` |
| JavaScript | `javascript/src/analytics.js` | `javascript/tests/analytics.test.js` |
