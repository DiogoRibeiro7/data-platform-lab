# Warehouse CLI

Load raw sample data into SQLite, execute star-schema warehouse transforms,
and run analytical queries — all from a single command.

---

## What it does

The warehouse CLI implements a complete ELT (Extract-Load-Transform) pipeline:

```text
Raw CSVs + JSONL events
       │
       ▼
  Create staging tables (sql/ddl/01-05)
       │
       ▼
  Load raw data into staging tables
       │
       ▼
  Create warehouse dims & facts (sql/ddl/06)
       │
       ▼
  Populate dim_date (sql/dml/06)
       │
       ▼
  Execute warehouse transforms (sql/warehouse/01-05)
  — deduplicate customers
  — filter negative-price products
  — normalise date formats
  — resolve surrogate keys
  — skip orphan foreign keys
       │
       ▼
  Run analytical queries against the star schema
       │
       ▼
  Write CSV reports + summary JSON
```

## Running the CLI

### Python

```bash
cd python
poetry run python -m data_platform_lab.warehouse.cli \
  --data-dir ../data/sample \
  --sql-dir ../sql \
  --report-dir ../data/gold/warehouse
```

### JavaScript

```bash
node javascript/src/warehouse/cli.js \
  --data-dir data/sample \
  --sql-dir sql \
  --report-dir data/gold/warehouse
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | `../data/sample` (Python), `../data/sample` (JS) | Directory with raw CSVs and events.json |
| `--sql-dir` | `../sql` (Python), `../sql` (JS) | Root of the SQL assets directory |
| `--db-path` | `:memory:` | SQLite database path (in-memory by default) |
| `--report-dir` | `../data/gold/warehouse` | Output directory for CSV reports and summary |

---

## Data flow

### Staging tables (loaded from raw files)

| Table | Source file | Rows |
|-------|-----------|------|
| customers | customers.csv | 13 |
| products | products.csv | 12 |
| orders | orders.csv | 15 |
| order_items | order_items.csv | 20 |
| events | events.json | 20 |

### Warehouse tables (after transforms)

| Table | Rows | Transform applied |
|-------|------|-------------------|
| dim_customer | 12 | Deduplicate C003, standardise country casing |
| dim_product | 11 | Filter P009 (negative price), convert boolean text to int |
| dim_date | 366 | Recursive CTE generating all 2024 dates |
| fact_order | 14 | Skip ORD-008 (orphan FK to C099), normalise date format |
| fact_order_item | 19 | Deduplicate identical line items, recalculate line_total |
| fact_event | 19 | Deduplicate by event_id, extract date and hour |

### Analytical queries

| Query | Description |
|-------|-------------|
| warehouse_row_counts | Row counts for all six warehouse tables |
| revenue_by_status | Revenue grouped by order status (completed, shipped, etc.) |
| top_products_warehouse | Products ranked by total revenue from the fact layer |
| daily_warehouse_revenue | Daily revenue with day-of-week and month context |
| customer_spend_warehouse | Customer spend from the warehouse layer with surrogate keys |

---

## SQL assets used

The CLI reads SQL files from disk at runtime:

| Category | Files | Purpose |
|----------|-------|---------|
| `sql/ddl/01-05` | Staging DDL | CREATE TABLE for raw tables |
| `sql/ddl/06` | Warehouse DDL | CREATE TABLE for dim/fact tables |
| `sql/dml/06` | dim_date loader | Populate the date dimension |
| `sql/warehouse/01-05` | Transforms | Stage → dim/fact with cleaning |

Analytical queries are defined in the Python/JS source code rather than
SQL files because they return structured results to the caller.

---

## Output files

When `--report-dir` is set, the CLI writes:

```
data/gold/warehouse/
├── warehouse_row_counts.csv
├── revenue_by_status.csv
├── top_products_warehouse.csv
├── daily_warehouse_revenue.csv
├── customer_spend_warehouse.csv
└── warehouse_summary.json
```

The summary JSON includes staging table row counts, warehouse table row
counts, and query result metadata.

---

## Tests

```bash
# Python (13 tests)
cd python && python -m pytest tests/test_warehouse.py -v

# JavaScript (13 tests)
cd javascript && node --test tests/warehouse.test.js
```

Tests cover:
- CSV and JSONL loading
- SQL file execution (single and multi-statement)
- Full pipeline against real sample data with exact row count assertions
- Report file generation
- Missing data directory handling
- Idempotent re-runs
- Warehouse transform correctness: deduplication, negative price filtering, orphan FK skipping

---

## Relationship to the analytics module

The repository has two analytical paths:

| | Analytics module | Warehouse CLI |
|---|---|---|
| **Input** | Curated CSVs from the demo pipeline (`data/silver/demo/`) | Raw sample data (`data/sample/`) |
| **Schema** | Flat staging tables only | Star schema (dims + facts) |
| **Transforms** | None (data already cleaned) | Deduplication, FK resolution, type conversion |
| **Use case** | Quick queries after running the demo | Full ELT pipeline exercise |

The analytics module is simpler — it loads already-cleaned data and runs
queries. The warehouse CLI starts from raw data and demonstrates the full
transformation pipeline including the star schema pattern.

---

## Limitations

- SQLite is single-file, single-process — not a distributed warehouse.
- The dim_date dimension covers only 2024 (matches sample data range).
- Fact tables use plain `INSERT` (not `INSERT OR REPLACE`), so re-running
  against the same file-backed database will duplicate fact rows. Use
  `:memory:` or delete the DB file between runs.
- The JavaScript CSV parser splits on commas and does not handle quoted
  fields containing commas. The Python implementation uses `csv.DictReader`,
  which handles quoting correctly.
- `node:sqlite` (JavaScript) is experimental in Node 22.
