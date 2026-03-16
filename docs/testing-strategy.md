# Testing Strategy

How tests are organized, what they cover, and how to run them.

---

## Running the tests

```bash
# Python — all tests
cd python && poetry run pytest

# Python — single file
poetry run pytest tests/test_incremental_etl.py -v

# JavaScript — all tests
cd javascript && yarn test

# JavaScript — single file
node --test tests/incremental-etl.test.js
```

---

## Test categories

### 1. Unit tests

Test individual functions in isolation with synthetic inputs.

**Examples:**
- `read_csv_file` with a valid CSV, empty CSV, header-only CSV
- `transform_event` with valid, missing-field, and bad-timestamp events
- `check_no_nulls` with clean and dirty records
- `deduplicate` with known duplicates

**What they verify:** correct return values, edge-case handling, error raising.

### 2. Integration tests

Test the full pipeline from input to output, verifying that modules compose
correctly.

**Examples:**
- `run_incremental_etl` — read JSONL, filter, transform, write output, update checkpoint
- `run_pipeline` — read directory of CSVs, validate, merge, deduplicate, write output
- `run_demo` — process all 4 sample tables end-to-end
- `run_analytics` — load silver CSVs into SQLite, run queries, write reports

**What they verify:** correct row counts, file creation, summary structure.

### 3. Rerun safety tests

Verify that running a pipeline twice on the same input produces identical
results and does not create duplicate outputs.

**Covered scenarios:**
- Incremental ETL: second run on same data creates no new output file
- CSV pipeline: rerun produces byte-identical output
- Demo pipeline: rerun produces identical silver CSVs
- Checkpoint: not updated when no new events exist

### 4. Failure and recovery tests

Test that pipelines handle errors gracefully and recover correctly.

**Covered scenarios:**
- Malformed JSON lines in JSONL files (skipped, not crashed)
- Corrupted checkpoint file (start fresh, log warning)
- Validation rule that throws an exception (recorded as critical failure)
- Checkpoint save failure (checkpoint unchanged, rerun processes everything)
- Empty CSV file (rejected gracefully)
- Missing input file (pipeline fails with clear error)
- Invalid timestamps (transform returns null)
- Non-numeric id/userId in API transform (skipped with warning)

### 5. Golden output tests

Verify that curated outputs contain the exact expected data, not just
correct row counts.

**Covered scenarios:**
- Customers CSV: C003 duplicate removed (appears exactly once)
- Products CSV: P009 (negative price) filtered out (not in output)
- Country casing: all values are title-cased
- Revenue amounts: top product is Mechanical Keyboard, revenue > 300
- Orphan FK: ORD-008 references C099 (exactly 1 orphan found)
- Duplicate detection: 0 duplicates after cleaning

### 6. Shape validation tests

Verify the JSON structure of checkpoints, manifests, and reports.

**Covered scenarios:**
- Checkpoint JSON has exactly: `pipeline_name`, `last_run_at`, `processed_ids`, `total_runs`
- Manifest JSON has exactly: `run` (with metadata fields) and `tables` (with per-table summaries)
- Analytics summary JSON has: `db_path`, `tables_loaded`, `queries`
- Report CSVs have same row count as in-memory query results

### 7. CDC determinism tests

Verify that snapshot diff produces consistent results regardless of
input order.

**Covered scenarios:**
- Running the same diff twice produces identical change lists
- Snapshots with different column order still compare correctly

---

## What is covered

| Area | Unit | Integration | Rerun | Failure | Golden | Shape |
|------|:----:|:-----------:|:-----:|:-------:|:------:|:-----:|
| CSV ingestion | x | x | x | x | | |
| API ingestion | x | x | | x | | |
| Validation rules | x | | | x | | |
| Validation runner | x | x | | x | | |
| Incremental ETL | x | x | x | x | | x |
| Snapshot diff | x | x | | | | |
| CDC determinism | | | | | | x |
| Orchestration runner | x | x | | x | | |
| Customer ETL workflow | x | x | | x | | |
| Observability tracker | x | | | | | |
| Demo pipeline | | x | x | | x | x |
| SQLite analytics | x | x | | | x | x |

---

## What is intentionally not covered

| Area | Reason |
|------|--------|
| Performance / large datasets | This is a learning repo; sample data is intentionally small |
| Concurrent pipeline runs | Single-process design; concurrency is documented as a limitation |
| Network failures in API ingestion | Tests use mocked HTTP; real network tests would be flaky |
| File permission errors | OS-specific and difficult to test portably |
| SQLite file-backed databases | Tests use `:memory:` for speed; file I/O is trivially different |
| CLI argument parsing | Thin wrappers over tested library functions |
| Streaming exercises | Not yet implemented |
| Warehouse / storage exercises | Not yet implemented |

---

## Test data

Tests use two kinds of input data:

1. **Inline fixtures** — small CSV/JSONL strings defined directly in test files.
   Used for unit tests and targeted edge cases.

2. **Sample data** — the committed files in `data/sample/`. Used for
   integration tests and golden output tests. These files contain intentional
   data quality issues (duplicates, null values, bad dates, orphan FKs) that
   the tests verify are detected and handled.

---

## Test file locations

| Python test file | JavaScript test file | What it covers |
|-----------------|---------------------|----------------|
| `test_csv_pipeline.py` | `csv-pipeline.test.js` | CSV ingestion |
| `test_api_pipeline.py` | `api-pipeline.test.js` | API ingestion |
| `test_validation.py` | `validation.test.js` | Validation rules + runner |
| `test_incremental_etl.py` | `incremental-etl.test.js` | Incremental ETL + checkpoints |
| `test_snapshot_diff.py` | `snapshot-diff.test.js` | CDC snapshot comparison |
| `test_runner.py` | `runner.test.js` | Orchestration runner |
| `test_observability.py` | `observability.test.js` | Timer, RunTracker |
| `test_customer_etl.py` | `customer-etl.test.js` | Orchestrated customer workflow |
| `test_demo.py` | `demo.test.js` | End-to-end demo pipeline |
| `test_analytics.py` | `analytics.test.js` | SQLite analytics layer |
