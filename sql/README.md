# SQL Assets

Standalone SQL scripts that complement the Python and JavaScript exercises.
All scripts target **SQLite** and can be executed with the `sqlite3` CLI or any
SQLite-compatible driver.

## Folder structure

```
sql/
├── ddl/          — Table definitions (CREATE TABLE)
├── dml/          — Data loading statements (INSERT)
├── analytics/    — Read-only analytical queries
└── warehouse/    — Star-schema ETL transformations
```

### ddl/ — Schema definitions

| # | File | Description |
|---|------|-------------|
| 01 | `01_customers.sql` | `customers` table — matches `data/sample/customers.csv` |
| 02 | `02_products.sql` | `products` table — matches `data/sample/products.csv` |
| 03 | `03_orders.sql` | `orders` table — matches `data/sample/orders.csv` |
| 04 | `04_order_items.sql` | `order_items` table — matches `data/sample/order_items.csv` |
| 05 | `05_events.sql` | `events` table — matches `data/sample/events.json` |
| 06 | `06_warehouse_dims_facts.sql` | Star schema: `dim_customer`, `dim_product`, `dim_date`, `fact_order`, `fact_order_item`, `fact_event` |

### dml/ — Data loading

| # | File | Description |
|---|------|-------------|
| 01 | `01_load_customers.sql` | All 12 customer rows (includes duplicate C003) |
| 02 | `02_load_products.sql` | All 12 products (includes negative-price P009) |
| 03 | `03_load_orders.sql` | All 15 orders (includes orphan FK C099) |
| 04 | `04_load_order_items.sql` | All 20 line items (includes duplicate line) |
| 05 | `05_load_events.sql` | All 20 events (includes duplicate evt-005) |
| 06 | `06_load_dim_date.sql` | Recursive CTE generating every date in 2024 |

### analytics/ — Analytical queries

| # | File | Description |
|---|------|-------------|
| 01 | `01_daily_revenue.sql` | Revenue by date for completed orders |
| 02 | `02_top_products.sql` | Products ranked by total revenue |
| 03 | `03_customer_order_counts.sql` | Customer activity summary (LEFT JOIN) |
| 04 | `04_duplicate_detection.sql` | Find duplicates in customers, order items, events |
| 05 | `05_missing_foreign_keys.sql` | Orphan FK detection across tables |
| 06 | `06_revenue_by_category.sql` | Category revenue with percentage of total |
| 07 | `07_customer_cohorts.sql` | Signup-month cohorts with conversion rate |
| 08 | `08_data_quality_checks.sql` | 7 checks: negative prices, NULLs, casing, date format, arithmetic |

### warehouse/ — Star-schema ETL

| # | File | Description |
|---|------|-------------|
| 01 | `01_stage_to_dim_customer.sql` | Deduplicate + standardise country casing |
| 02 | `02_stage_to_dim_product.sql` | Filter negative prices, convert boolean text → int |
| 03 | `03_stage_to_fact_order.sql` | Normalise dates, join dim, skip orphan FKs |
| 04 | `04_stage_to_fact_order_item.sql` | Deduplicate line items, recalculate totals |
| 05 | `05_stage_to_fact_event.sql` | Deduplicate by event_id, extract date + hour |
| 06 | `06_deduplication.sql` | 3 reusable deduplication patterns |
| 07 | `07_snapshot_merge.sql` | CDC merge pattern (delete → upsert) |

## Database compatibility

These scripts are written for **SQLite 3.35+** (for `DROP TABLE IF EXISTS`,
recursive CTEs, and `INSERT OR REPLACE`). They intentionally avoid
Postgres/MySQL-specific syntax so they can run anywhere without setup.

Key SQLite idioms used throughout:

- `INSERT OR IGNORE` / `INSERT OR REPLACE` instead of `ON CONFLICT`
- `rowid` for deduplication (no `ROW_NUMBER()` in DELETE)
- `substr()` / `replace()` for string manipulation
- Recursive CTEs for date generation

## Relationship to the rest of the repo

The sample data in `data/sample/` is the single source of truth. The DML
scripts load the exact same rows — including intentional data-quality issues
(duplicates, NULLs, bad FKs, negative prices) — so you can practise cleaning
them with the analytics and warehouse queries.

The warehouse ETL scripts mirror what the Python and JavaScript pipelines do
programmatically:

| SQL script | Python / JS equivalent |
|---|---|
| `warehouse/01–05` | Exercise 03 — validation + transform |
| `warehouse/06` | Exercise 06 — deduplication patterns |
| `warehouse/07` | Exercise 05 — CDC snapshot diff |

## Quick start

```bash
sqlite3 lab.db < sql/ddl/01_customers.sql
sqlite3 lab.db < sql/dml/01_load_customers.sql
sqlite3 lab.db < sql/analytics/01_daily_revenue.sql
```

Or load everything at once:

```bash
for f in sql/ddl/*.sql sql/dml/*.sql; do sqlite3 lab.db < "$f"; done
```
