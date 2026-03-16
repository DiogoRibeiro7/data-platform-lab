# Orchestrated Workflow: Customer ETL

A concrete end-to-end workflow that wires the orchestration runner (Exercise 06)
to real modules from the repository: CSV ingestion (Exercise 01), validation
framework (Exercise 03), and deduplication.

---

## What it does

The customer ETL pipeline reads `data/sample/customers.csv`, runs data-quality
checks, removes duplicate rows, and writes a cleaned CSV. Every step is executed
through the orchestration runner, which provides structured timing, status
reporting, retry logic, and fail-fast behaviour.

### Steps

| # | Step | Module used | What it does |
|---|------|-------------|--------------|
| 1 | **extract** | `ingestion/csv_pipeline` | Read CSV, standardise headers, trim fields |
| 2 | **validate** | `validation/rules` + `runner` | Check required columns, no nulls, unique IDs, date format |
| 3 | **clean** | `ingestion/csv_pipeline` | Deduplicate rows |
| 4 | **load** | (built-in) | Write cleaned CSV to output path |
| 5 | **report** | `validation/runner` | Format validation report as text |

The validate step is registered with `allow_skip=True`, so validation failures
are recorded but do not stop the pipeline. All other steps use fail-fast: if
extract fails (e.g., missing file), the pipeline stops immediately.

---

## Running it

### Python

```bash
cd python
poetry run python -c "
from data_platform_lab.orchestration.customer_etl import run_customer_etl
from data_platform_lab.orchestration.runner import format_result

result = run_customer_etl('../data/sample/customers.csv', '../data/bronze/customers_cleaned.csv')
print(format_result(result))
"
```

### JavaScript

```bash
cd javascript
node -e "
import { runCustomerEtl, formatResult } from './src/orchestration/customer-etl.js';

const result = await runCustomerEtl('../data/sample/customers.csv', '../data/bronze/customers_cleaned.js.csv');
console.log(formatResult(result));
"
```

### Expected output

```
=== Pipeline: customer_etl ===
Status: success
Duration: 0.01s
Steps: 5 total | 5 passed | 0 failed | 0 skipped

  [PASS] extract (0.00s)
  [PASS] validate (0.00s)
  [PASS] clean (0.00s)
  [PASS] load (0.00s)
  [PASS] report (0.00s)
```

---

## Step results

Each step returns a structured result accessible via `result.steps[i].result`:

**extract:**
```json
{ "rows_read": 13, "columns": 7 }
```

**validate:**
```json
{ "total_checks": 4, "passed": 3, "failed": 1, "status": "failed" }
```

The validation reports 3 passes (required columns, no nulls in ID fields,
date format) and 1 failure (duplicate `customer_id` C003). Despite the
validation failure, the pipeline continues because validate is skippable.

**clean:**
```json
{ "rows_before": 13, "rows_after": 12, "duplicates_removed": 1 }
```

**load:**
```json
{ "output_path": "data/bronze/customers_cleaned.csv", "rows_written": 12 }
```

**report:** Returns the formatted validation report as a string.

---

## Failure behaviour

| Scenario | Behaviour |
|----------|-----------|
| Input file missing | Extract step fails, pipeline stops, status = `"failed"` |
| CSV is empty | Extract step raises `ValueError`, pipeline stops |
| Validation finds issues | Logged and reported, pipeline continues (step is skippable) |
| Output directory missing | Load step creates it automatically |
| Output write fails | Load step fails, pipeline stops |

---

## How the runner is used

The workflow demonstrates these orchestration features:

- **Shared context** — steps communicate through a dict. Extract stores
  `headers` and `rows`; validate adds `records` and `validation_report`;
  clean updates `rows` in-place.
- **Step results** — each step returns a structured summary stored in
  `context["step_results"]["step_name"]`.
- **allow_skip** — the validate step is skippable so quality issues are
  reported without blocking the pipeline.
- **Fail-fast** — if extract or load fails, execution stops immediately.
- **Timing** — every step gets wall-clock timing automatically.
- **format_result** — produces a human-readable summary of the entire run.

---

## Tests

```bash
# Python — 11 tests
cd python && poetry run pytest tests/test_customer_etl.py -v

# JavaScript — 5 tests
cd javascript && node --test tests/customer-etl.test.js
```

Tests cover: individual step behaviour, end-to-end success, missing file
failure, real sample data, and output format.

---

## File locations

| Language | Workflow module | Tests |
|----------|----------------|-------|
| Python | `python/src/data_platform_lab/orchestration/customer_etl.py` | `python/tests/test_customer_etl.py` |
| JavaScript | `javascript/src/orchestration/customer-etl.js` | `javascript/tests/customer-etl.test.js` |
