# Learning Roadmap

A structured path through the exercises in this repository, organised by
difficulty. Work through each stage in order. Within a stage, follow the
numbered sequence — later exercises build on patterns introduced earlier.

---

## Stage 1 — Beginner

Learn to read data from files, clean it, and enforce quality rules.

| Order | Exercise | Skills trained |
|-------|----------|----------------|
| 1 | [01 — CSV Ingestion](01-csv-ingestion-pipeline.md) | File I/O, encoding handling, row-level cleaning, deduplication, writing structured output |
| 2 | [03 — Validation Framework](03-validation-framework.md) | Composable rules, severity levels, pass/fail gating, separating valid from invalid records |

**What you will be able to do after this stage:**
- Read messy CSV files and produce clean, deduplicated output
- Define reusable validation rules and apply them to any dataset
- Route rejected records to a dead-letter file for inspection

**SQL to pair with this stage:**
- `sql/ddl/` — read the table definitions to understand target schemas
- `sql/dml/` — see how the same sample data is loaded in SQL
- `sql/analytics/04_duplicate_detection.sql` — compare your deduplication logic to the SQL approach
- `sql/analytics/08_data_quality_checks.sql` — see quality checks expressed as queries

---

## Stage 2 — Intermediate

Move beyond flat files. Fetch data from APIs, process only what has changed,
and detect differences between snapshots.

| Order | Exercise | Skills trained |
|-------|----------|----------------|
| 3 | [02 — API Ingestion](02-api-ingestion-pipeline.md) | HTTP pagination, retry with backoff, raw vs processed storage, run metadata |
| 4 | [04 — Incremental ETL](04-incremental-etl-pipeline.md) | Checkpoint persistence, idempotent reruns, processing only new records |
| 5 | [05 — Snapshot Diff](05-snapshot-diff.md) | Change data capture, detecting inserts/updates/deletes between two snapshots |

**What you will be able to do after this stage:**
- Ingest data from paginated HTTP endpoints with automatic retry
- Run a pipeline repeatedly and process only new data each time
- Compare two versions of a dataset and produce a structured change report

**SQL to pair with this stage:**
- `sql/warehouse/01–05` — star-schema ETL transformations that mirror what the code pipelines do
- `sql/warehouse/07_snapshot_merge.sql` — the SQL equivalent of snapshot diff (CDC merge pattern)
- `sql/analytics/05_missing_foreign_keys.sql` — orphan detection that incremental loads must handle

---

## Stage 3 — Advanced

Compose pipelines from discrete steps, add timing and health tracking, and
think about warehouse-layer patterns.

| Order | Exercise | Skills trained |
|-------|----------|----------------|
| 6 | [06 — Orchestration Runner](06-orchestration-runner.md) | Sequential step execution, retry logic, fail-fast vs skip, shared context between steps |
| 7 | [07 — Observability](07-observability.md) | Execution timing, run metadata, counters, warnings, structured reporting |

**What you will be able to do after this stage:**
- Wire multiple pipeline steps into a single run with retry and skip policies
- Instrument any pipeline with timing, counters, and structured run metadata
- Produce machine-readable run reports for monitoring

**SQL to pair with this stage:**
- `sql/warehouse/06_deduplication.sql` — reusable deduplication patterns (rowid, temp table, GROUP BY)
- `sql/analytics/06_revenue_by_category.sql` and `07_customer_cohorts.sql` — analytical queries that consume warehouse output

---

## Skill Map

Which exercises train which skills. Use this to jump directly to a topic.

| Skill | Exercises |
|-------|-----------|
| File I/O and parsing | 01, 02 |
| Data cleaning | 01, 03 |
| Deduplication | 01, 04, 05, SQL `warehouse/06` |
| Schema validation | 03 |
| HTTP and APIs | 02 |
| Retry and error handling | 02, 06 |
| Checkpointing and idempotency | 04 |
| Change data capture | 05, SQL `warehouse/07` |
| Pipeline composition | 06 |
| Observability and metrics | 07 |
| Star-schema modelling | SQL `ddl/06`, SQL `warehouse/01–05` |
| Analytical SQL | SQL `analytics/01–08` |
| Data quality (SQL) | SQL `analytics/04, 05, 08` |

---

## Suggested Pacing

Each exercise is designed to be completed in a single session. A reasonable
pace is one exercise per sitting, which puts the full implemented set at
roughly seven sessions.

Work through each exercise in this order:

1. Read the guide in `docs/`
2. Study the tests — they define expected behaviour
3. Read the implementation
4. Try re-implementing from scratch using only the tests as a spec
5. Run the matching SQL queries to see the same patterns in a different paradigm

---

## Future Exercises

The following are planned but not yet implemented. They appear in the main
README roadmap and have placeholder module directories.

| Exercise | Area | Concepts |
|----------|------|----------|
| ZIP extraction | Ingestion | Archive handling, file routing, content inventory |
| Log parsing | Ingestion | Semi-structured text, regex extraction, structured output |
| Event processing simulation | Streaming | Windowed aggregation, event ordering, simulated real-time |
| Warehouse loading CLI | Warehouse | SQLite integration, Python/JS wrapper around SQL assets |
| Ingestion benchmark | Performance | Sequential vs parallel vs async, throughput measurement |
