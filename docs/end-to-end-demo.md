# End-to-End Demo: E-Commerce Pipeline

A single command that ingests, validates, cleans, and outputs four related
datasets — demonstrating how the repository's modules work together.

---

## What it processes

| Table | Rows in | Issues found | Rows out |
|-------|---------|--------------|----------|
| customers | 13 | 1 duplicate (C003), inconsistent country casing | 12 |
| products | 12 | 1 negative price (P009), mixed currencies | 11 |
| orders | 15 | 1 orphan FK (C099), date format inconsistency | 15 |
| order_items | 20 | 1 duplicate line item (ORD-006/P006) | 19 |
| **Total** | **60** | **6 warnings** | **57** |

---

## Run it

### Python

```bash
cd python
poetry run python -m data_platform_lab.demo
```

### JavaScript

```bash
cd javascript
node src/demo.js
```

Both produce identical results. Output CSVs go to `data/silver/demo/`, and a
JSON manifest goes to `data/manifests/`.

### Custom paths

```bash
# Python
poetry run python -m data_platform_lab.demo \
  --data-dir ../data/sample \
  --output-dir ../data/silver/demo \
  --manifest-dir ../data/manifests

# JavaScript
node src/demo.js \
  --data-dir ../data/sample \
  --output-dir ../data/silver/demo \
  --manifest-dir ../data/manifests
```

---

## What it demonstrates

| Capability | How it is used |
|------------|----------------|
| **Ingestion** | `read_csv_file`, `standardize_headers`, `trim_fields` from Exercise 01 |
| **Validation** | `check_required_columns`, `check_no_nulls`, `check_unique`, `check_numeric_range`, `check_allowed_values`, `check_date_format` from Exercise 03 |
| **Transformation** | Deduplication, country casing normalisation, date format repair, negative price filtering |
| **Cross-table logic** | Foreign key validation (orders reference valid customer IDs from the cleaned customers table) |
| **Observability** | `RunTracker` from Exercise 07 captures timing, row counts, warnings, and extras |
| **Curated output** | Cleaned CSVs written to `data/silver/demo/` |
| **Run manifest** | JSON file with full run metadata and per-table summaries |

---

## Output

### Console

```
=== Run: ecommerce_demo (20260316_120000) ===
Status: success
Started: 2026-03-16T12:00:00.000000+00:00
Ended:   2026-03-16T12:00:00.015000+00:00
Duration: 0.01s

Rows read:     60
Rows written:  57
Rows rejected: 3
Files processed: 4
Files rejected:  0

Warnings (6):
  - customers: 1 validation check(s) failed
  - customers: removed 1 duplicate row(s)
  - products: 1 validation check(s) failed
  - products: filtered 1 row(s) with invalid price
  - orders: 1 row(s) reference non-existent customer_id
  - order_items: removed 1 duplicate row(s)

Extra:
  tables_processed: 4
  output_dir: data/silver/demo

  customers: 13 read -> 12 out
  products: 12 read -> 11 out
  orders: 15 read -> 15 out
  order_items: 20 read -> 19 out

Manifest: data/manifests/ecommerce_demo_20260316_120000.json
```

### Cleaned files in `data/silver/demo/`

```
data/silver/demo/
  customers.csv      (12 rows — deduplicated, country casing fixed)
  products.csv       (11 rows — negative prices removed)
  orders.csv         (15 rows — date format normalised)
  order_items.csv    (19 rows — duplicate line items removed)
```

### JSON manifest

```json
{
  "run": {
    "pipeline_name": "ecommerce_demo",
    "run_id": "20260316_120000",
    "status": "success",
    "rows_read": 60,
    "rows_written": 57,
    "rows_rejected": 3,
    "files_processed": 4,
    "warnings": ["..."],
    "extra": {
      "tables_processed": 4,
      "output_dir": "data/silver/demo"
    }
  },
  "tables": {
    "customers": { "rows_read": 13, "rows_out": 12, "duplicates_removed": 1 },
    "products": { "rows_read": 12, "rows_out": 11, "rows_filtered": 1 },
    "orders": { "rows_read": 15, "rows_out": 15, "orphan_customer_ids": 1 },
    "order_items": { "rows_read": 20, "rows_out": 19, "duplicates_removed": 1 }
  }
}
```

---

## Pipeline flow

```
data/sample/customers.csv ──> read + validate + deduplicate + fix casing ──> silver/customers.csv
data/sample/products.csv  ──> read + validate + filter bad prices ─────────> silver/products.csv
data/sample/orders.csv    ──> read + validate + fix dates + FK check ──────> silver/orders.csv
data/sample/order_items.csv > read + validate + deduplicate ───────────────> silver/order_items.csv
                                                                     │
                                                            RunTracker collects
                                                            timing, counts, warnings
                                                                     │
                                                            manifests/ecommerce_demo_{run_id}.json
```

---

## Tests

```bash
# Python — 8 tests
cd python && poetry run pytest tests/test_demo.py -v

# JavaScript — 7 tests
cd javascript && node --test tests/demo.test.js
```

Tests verify: row counts, output files, manifest structure, warnings, and
country casing normalisation.

---

## File locations

| Language | Demo module | Tests |
|----------|------------|-------|
| Python | `python/src/data_platform_lab/demo.py` | `python/tests/test_demo.py` |
| JavaScript | `javascript/src/demo.js` | `javascript/tests/demo.test.js` |

---

## Relationship to the orchestrated workflow

The repository also contains a smaller [orchestrated workflow](orchestrated-workflow.md)
that processes only the customers table through the orchestration runner (Exercise 06).

The two serve different purposes:

- **This demo** is the main showcase — it processes all 4 tables using direct
  function calls and `RunTracker` for observability. It is the recommended
  entry point for visitors.
- **The orchestrated workflow** is a focused example of Exercise 06 — it
  demonstrates the `Pipeline` class with real modules (extract, validate,
  clean, load, report). It is the recommended entry point for studying how
  the orchestration runner works.
