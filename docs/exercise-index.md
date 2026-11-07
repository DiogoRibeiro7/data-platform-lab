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
| 08 | [Streaming Event Processor](streaming-exercise.md) | Streaming | Advanced | Done | Done | — | 03, 07 |
| 09 | [Benchmark Runner](benchmark-exercise.md) | Performance | Advanced | Done | Done | — | 01, 07 |

### Supporting workflows

These implemented workflows complement the nine numbered exercises:

| Workflow | Guide | What it covers | Python | JavaScript |
| --- | --- | --- | --- | --- |
| Warehouse loading CLI | [Warehouse CLI](warehouse-cli.md) | Load raw CSV/JSONL into SQLite, apply star-schema SQL transforms, and write analytical reports | Done | Done |
| Config-driven execution | [Config-driven workflows](config-driven-workflows.md) | JSON configuration for streaming, warehouse, and benchmark runs; CLI flags override config values | Done | Done |

Example configurations: [streaming](../configs/streaming.json),
[warehouse](../configs/warehouse.json), and [benchmark](../configs/benchmark.json).

### Canonical data platform integration

- [Registry ingestion](registry-handoff-ingestion.md) verifies pinned source
  bytes into a local or S3-compatible object store, with run receipts.
- [ONS pay analytics](ons-pay-analytics.md) consumes those receipts, gates
  source quality, and writes typed DuckDB/Parquet Silver/Gold datasets.

These Python-only integrations are separate from the numbered cross-language
exercises counted below.

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
| Windowed event aggregation | Streaming | Advanced | Time-window aggregation beyond the per-event processing in exercise 08 |

---

## Status Summary

| Metric | Count |
|--------|-------|
| Implemented numbered exercises (Python + JS) | 9 |
| Supporting workflows (Python + JS) | 2 |
| SQL exercise areas | 12 |
| Primary test files for numbered exercises, per language | 9 |
| SQL exercise scripts (`ddl/`, `dml/`, `analytics/`, `warehouse/`) | 27 |
| Numbered exercise guides in `docs/` | 9 |
| Planned exercises (not started) | 3 |

Test-file counts refer to the primary suites listed below, not the repository's
full platform and integration test suites. SQL counts exclude platform metadata
migrations and packaged ONS analytics models.

---

## File Locations

Quick lookup for where each exercise lives in the repository.

| # | Guide | Python source | Python tests | JS source | JS tests |
|---|-------|---------------|--------------|-----------|----------|
| 01 | `docs/01-csv-ingestion-pipeline.md` | `python/src/data_platform_lab/ingestion/csv_pipeline.py` | `python/tests/test_csv_pipeline.py` | `javascript/src/ingestion/csv-pipeline.js` | `javascript/tests/csv-pipeline.test.js` |
| 02 | `docs/02-api-ingestion-pipeline.md` | `python/src/data_platform_lab/ingestion/api_pipeline.py` | `python/tests/test_api_pipeline.py` | `javascript/src/ingestion/api-pipeline.js` | `javascript/tests/api-pipeline.test.js` |
| 03 | `docs/03-validation-framework.md` | `python/src/data_platform_lab/validation/rules.py` | `python/tests/test_validation.py` | `javascript/src/validation/rules.js` | `javascript/tests/validation.test.js` |
| 04 | `docs/04-incremental-etl-pipeline.md` | `python/src/data_platform_lab/transform/incremental_etl.py` | `python/tests/test_incremental_etl.py` | `javascript/src/transform/incremental-etl.js` | `javascript/tests/incremental-etl.test.js` |
| 05 | `docs/05-snapshot-diff.md` | `python/src/data_platform_lab/transform/snapshot_diff.py` | `python/tests/test_snapshot_diff.py` | `javascript/src/transform/snapshot-diff.js` | `javascript/tests/snapshot-diff.test.js` |
| 06 | `docs/06-orchestration-runner.md` | `python/src/data_platform_lab/orchestration/runner.py` | `python/tests/test_runner.py` | `javascript/src/orchestration/runner.js` | `javascript/tests/runner.test.js` |
| 07 | `docs/07-observability.md` | `python/src/data_platform_lab/observability/tracker.py` | `python/tests/test_observability.py` | `javascript/src/observability/tracker.js` | `javascript/tests/observability.test.js` |
| 08 | `docs/streaming-exercise.md` | `python/src/data_platform_lab/streaming/processor.py` | `python/tests/test_streaming.py` | `javascript/src/streaming/processor.js` | `javascript/tests/streaming.test.js` |
| 09 | `docs/benchmark-exercise.md` | `python/src/data_platform_lab/benchmark/runner.py` | `python/tests/test_benchmark.py` | `javascript/src/benchmark/runner.js` | `javascript/tests/benchmark.test.js` |

Supporting workflow locations:

| Workflow | Python source | Python tests | JS source | JS tests |
| --- | --- | --- | --- | --- |
| Warehouse CLI | `python/src/data_platform_lab/warehouse/cli.py`, `python/src/data_platform_lab/warehouse/loader.py` | `python/tests/test_warehouse.py` | `javascript/src/warehouse/cli.js`, `javascript/src/warehouse/loader.js` | `javascript/tests/warehouse.test.js` |
| Config-driven execution | `python/src/data_platform_lab/config.py` | `python/tests/test_config.py` | `javascript/src/config.js` | `javascript/tests/config.test.js` |
