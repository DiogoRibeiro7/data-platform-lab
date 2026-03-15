# Exercise Index

Quick reference for every exercise in the repository. See the
[roadmap](roadmap.md) for recommended order and stage descriptions.

---

## Implemented Exercises

| # | Exercise | Skill area | Difficulty | Python | JavaScript | SQL | Depends on |
|---|----------|------------|------------|--------|------------|-----|------------|
| 01 | [CSV Ingestion](01-csv-ingestion-pipeline.md) | Ingestion | Beginner | Done | Done | `ddl/01–04`, `dml/01–04` | — |
| 02 | [API Ingestion](02-api-ingestion-pipeline.md) | Ingestion | Intermediate | Done | Done | — | — |
| 03 | [Validation Framework](03-validation-framework.md) | Data quality | Beginner | Done | Done | `analytics/08` | 01 |
| 04 | [Incremental ETL](04-incremental-etl-pipeline.md) | Transform | Intermediate | Done | Done | `warehouse/01–05` | 01, 03 |
| 05 | [Snapshot Diff](05-snapshot-diff.md) | Transform | Intermediate | Done | Done | `warehouse/07` | — |
| 06 | [Orchestration Runner](06-orchestration-runner.md) | Orchestration | Advanced | Done | Done | — | 04 |
| 07 | [Observability](07-observability.md) | Observability | Advanced | Done | Done | — | 06 |

### SQL-only exercises

The `sql/` directory contains standalone exercises that do not have Python or
JavaScript implementations. They can be worked independently using SQLite.

| Area | Files | What it covers | Difficulty |
|------|-------|----------------|------------|
| Schema design | `ddl/01–05` | Staging tables matching sample CSVs | Beginner |
| Star schema | `ddl/06` | Dimensions, facts, surrogate keys, `loaded_at` | Intermediate |
| Data loading | `dml/01–05` | INSERT with intentional quality issues | Beginner |
| Date dimension | `dml/06` | Recursive CTE generating a full calendar year | Intermediate |
| Revenue queries | `analytics/01, 02, 06` | Aggregation, ranking, percentage of total | Beginner |
| Join patterns | `analytics/03, 05` | LEFT JOIN, orphan FK detection | Beginner |
| Duplicate detection | `analytics/04` | GROUP BY + HAVING across multiple tables | Beginner |
| Cohort analysis | `analytics/07` | Signup cohorts, conversion rates | Intermediate |
| Data quality checks | `analytics/08` | 7 checks: negatives, NULLs, casing, dates, arithmetic | Intermediate |
| Warehouse ETL | `warehouse/01–05` | Stage → dim/fact with cleaning and joins | Intermediate |
| Deduplication patterns | `warehouse/06` | rowid, temp-table swap, GROUP BY approaches | Intermediate |
| CDC merge | `warehouse/07` | Staging table → delete + upsert pattern | Advanced |

---

## Planned Exercises (not yet implemented)

| Exercise | Skill area | Difficulty | Notes |
|----------|------------|------------|-------|
| ZIP extraction | Ingestion | Beginner | Archive handling, file routing |
| Log parsing | Ingestion | Beginner | `data/sample/logs.log` exists as sample input |
| Event processing | Streaming | Advanced | Windowed aggregation, simulated real-time |
| Warehouse loading CLI | Warehouse | Intermediate | Python/JS wrapper around the SQL assets |
| Ingestion benchmark | Performance | Advanced | Sequential vs parallel vs async throughput |

---

## Status Summary

| Metric | Count |
|--------|-------|
| Implemented exercises (Python + JS) | 7 |
| SQL exercise areas | 12 |
| Python test files | 7 |
| JavaScript test files | 7 |
| SQL scripts | 27 |
| Exercise guides in `docs/` | 7 |
| Planned exercises (not started) | 5 |

---

## File Locations

Quick lookup for where each exercise lives in the repository.

| # | Guide | Python source | Python tests | JS source | JS tests |
|---|-------|---------------|--------------|-----------|----------|
| 01 | `docs/01-csv-ingestion-pipeline.md` | `python/src/.../ingestion/csv_pipeline.py` | `python/tests/test_csv_pipeline.py` | `javascript/src/ingestion/csv-pipeline.js` | `javascript/tests/csv-pipeline.test.js` |
| 02 | `docs/02-api-ingestion-pipeline.md` | `python/src/.../ingestion/api_pipeline.py` | `python/tests/test_api_pipeline.py` | `javascript/src/ingestion/api-pipeline.js` | `javascript/tests/api-pipeline.test.js` |
| 03 | `docs/03-validation-framework.md` | `python/src/.../validation/rules.py` | `python/tests/test_validation.py` | `javascript/src/validation/rules.js` | `javascript/tests/validation.test.js` |
| 04 | `docs/04-incremental-etl-pipeline.md` | `python/src/.../transform/incremental_etl.py` | `python/tests/test_incremental_etl.py` | `javascript/src/transform/incremental-etl.js` | `javascript/tests/incremental-etl.test.js` |
| 05 | `docs/05-snapshot-diff.md` | `python/src/.../transform/snapshot_diff.py` | `python/tests/test_snapshot_diff.py` | `javascript/src/transform/snapshot-diff.js` | `javascript/tests/snapshot-diff.test.js` |
| 06 | `docs/06-orchestration-runner.md` | `python/src/.../orchestration/runner.py` | `python/tests/test_runner.py` | `javascript/src/orchestration/runner.js` | `javascript/tests/runner.test.js` |
| 07 | `docs/07-observability.md` | `python/src/.../observability/tracker.py` | `python/tests/test_observability.py` | `javascript/src/observability/tracker.js` | `javascript/tests/observability.test.js` |
