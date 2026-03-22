# Milestone 2 — Phase 3 Build-out

Completed: 2026-03-21

---

## Purpose

Milestone 1 delivered a working exercise lab: seven exercises, one demo
pipeline, and an analytics layer. Everything was batch, single-step, and
invoked via module-specific entry points.

Milestone 2 extends the repository into territory that a first batch lab
cannot reach: event-stream processing, warehouse-style ELT, performance
benchmarking, config-driven execution, and cross-module metadata
infrastructure. The exercises still use only standard library modules — no
pandas, no Spark — but the patterns they demonstrate are now closer to what
production data platforms require.

---

## What was added in Phase 3

### 1. Streaming / event processing

**Exercise 08 — Streaming processor** (`streaming/processor.py`, `streaming/processor.js`)

Reads a JSONL file of sensor events one event at a time and applies:

- Per-event validation against six required fields (`sensor_id`, `type`,
  `value`, `unit`, `location`, `timestamp`)
- Deduplication by composite key (`sensor_id::timestamp`; first occurrence wins)
- Dead-letter routing — rejected and duplicate events written to
  `dead_letter.jsonl` with status and reason
- Grouped aggregation over accepted events (`by_sensor`, `by_type`,
  `by_location`) with min/max/avg/count per sensor
- Event-time watermark tracking — watermark advances to the latest observed
  event timestamp
- Configurable lateness threshold — events behind the watermark by more than
  `lateness_threshold_seconds` are classified as late and written to
  `late_events.jsonl`
- Machine-readable run summary (`summary.json`) with full breakdown:
  `events_seen`, `events_accepted`, `events_rejected`, `events_duplicate`,
  `events_late`, `max_lateness_seconds`, `watermark`, `rejection_reasons`

The CLI (`streaming/cli.py`, `streaming/cli.js`) accepts `--config` to load
settings from `configs/streaming.json`.

**Sensor pipeline demo** (`sensor_demo.py`, `sensor-demo.js`)

A five-step orchestrated pipeline built on the Exercise 06 runner:

1. Ingest — read 16 JSONL events from `data/sample/sensor_events.json`
2. Validate — reject events missing required fields or with null values
3. Deduplicate — remove duplicate `sensor_id::timestamp` pairs
4. Aggregate — hourly aggregation by sensor with min/max/avg/count
5. Output — write silver-layer CSVs, dead-letter file, and summary

Uses `RunTracker` for observability and generates a manifest on completion.

### 2. Warehouse execution workflows

**Warehouse CLI** (`warehouse/loader.py`, `warehouse/loader.js`)

Full ELT pipeline from raw files to star-schema analytics:

1. Load raw CSVs (`customers.csv`, `products.csv`, `orders.csv`,
   `order_items.csv`) and JSONL (`events.json`) into SQLite staging tables
2. Execute SQL transforms from `sql/warehouse/` to populate dimension tables
   (`dim_customer`, `dim_product`, `dim_date`) and fact tables (`fact_order`,
   `fact_order_item`, `fact_event`)
3. Run five analytical queries over the resulting star schema (row counts,
   revenue by status, top products, daily revenue, customer spend)
4. Write query results as CSVs to `data/gold/warehouse/`

The SQL assets in `sql/warehouse/` include staging-to-dimension transforms,
deduplication patterns (rowid, temp-table swap, GROUP BY), and a CDC snapshot
merge. The warehouse CLI uses `configs/warehouse.json` for paths.

### 3. Benchmark exercise

**Exercise 09 — Benchmark runner** (`benchmark/runner.py`, `benchmark/runner.js`)

Generates synthetic CSV files (configurable count and row size), then processes
them using three strategies:

| Strategy | Python | JavaScript |
| --- | --- | --- |
| Sequential | `for` loop | `for` loop |
| Concurrent | `ThreadPoolExecutor` | `Promise.all` with worker pool |
| Async | `asyncio.gather` | `Promise.all` (file I/O) |

Outputs a report with per-strategy elapsed time, rows processed, and files
handled. Results are machine-dependent and cannot be asserted in tests —
tests verify structure and correctness, not timing.

The CLI accepts `--config` to load settings from `configs/benchmark.json`
(defaults: 100 files, 200 rows each, 4 workers).

### 4. Config-driven execution

**Config loader** (`config.py`, `config.js`)

Implements a three-level precedence model:

```text
defaults < config file (JSON) < CLI flags
```

- `load_config()` — parse and validate JSON
- `validate_config()` — check required keys, warn on unknown keys
- `merge_config()` — merge defaults, file config, and CLI overrides

Three CLIs use it today: streaming, warehouse, and benchmark. Example config
files are committed in `configs/`.

### 5. Manifest infrastructure

**Shared manifest utility** (`manifest.py`, `manifest.js`)

- `write_manifest()` — writes a JSON manifest with required fields
  (`pipeline_name`, `run_id`, `created_at`, `source`, `output`, `row_count`,
  `status`) plus optional `schema_hint`, `warnings`, and pipeline-specific
  `extras`
- `read_manifest()` — parse a manifest file
- `validate_manifest()` — check for missing required keys

Six pipeline entry points now generate manifests (best-effort — failures are
logged and do not stop the pipeline):

| Pipeline | `pipeline_name` |
| --- | --- |
| CSV ingestion | `csv_ingestion` |
| API ingestion | `api_ingestion` |
| Incremental ETL | `events_etl` |
| Streaming processor | `sensor_stream` |
| Warehouse loader | `warehouse` |
| E-commerce demo | `ecommerce_demo` |

### 6. Shared test infrastructure

**`conftest.py`** (Python) and **`helpers.js`** (JavaScript):

- Path constants for sample data files
- `tmp_dir` fixture / temp directory factory
- CSV and JSONL writer helpers for generating test data
- Eliminates duplicated helper code across 17 Python and 16 JavaScript test files

### 7. CI improvements

- Poetry virtualenv caching via `actions/cache` with dependency hash key
- Yarn cache via `actions/cache`
- `pipx install poetry` (replaces slower pip installation)
- `sql/**` added to path triggers for both workflows
- Sensor demo added as a second smoke test in both CI workflows

---

## What is now demonstrated beyond Milestone 1

| Capability | M1 | M2 |
| --- | :--: | :--: |
| Batch CSV/API ingestion | Yes | Yes |
| Composable validation rules | Yes | Yes |
| Incremental ETL with checkpoints | Yes | Yes |
| CDC snapshot comparison | Yes | Yes |
| Orchestration runner | Unit tests + customer_etl | + sensor demo (real workflow) |
| Observability (Timer, RunTracker) | Yes | Yes |
| Event-stream processing | — | Streaming processor |
| Event-time watermarks | — | Watermark tracking + lateness classification |
| Dead-letter routing | — | Streaming + sensor demo |
| Star-schema ELT pipeline | SQL scripts only | Executable warehouse CLI |
| Performance benchmarking | — | 3 strategies with report output |
| Cross-module manifests | Demo only | 6 pipeline entry points |
| Config-driven execution | — | 3 CLIs with `--config` |
| Shared test helpers | — | `conftest.py` + `helpers.js` |
| CI dependency caching | — | Poetry + Yarn caching |

---

## Current test coverage

| Language | Tests | Failures |
| --- | ---: | :---: |
| Python | 251 | 0 |
| JavaScript | 235 | 0 |
| **Total** | **486** | **0** |

All linting (Ruff, mypy strict, ESLint) passes with zero errors.

| Test file | Py | JS | Covers |
| --- | ---: | ---: | --- |
| test_csv_pipeline | 17 | 17 | CSV ingestion |
| test_api_pipeline | 13 | 13 | API ingestion |
| test_validation | 21 | 21 | Validation rules + runner |
| test_incremental_etl | 16 | 16 | Incremental ETL + checkpoints |
| test_snapshot_diff | 25 | 25 | CDC snapshot comparison |
| test_runner | 14 | 14 | Orchestration runner |
| test_observability | 18 | 18 | Timer, RunTracker |
| test_customer_etl | 11 | 5 | Orchestrated customer workflow |
| test_streaming | 33 | 33 | Streaming processor + lateness |
| test_warehouse | 13 | 13 | Warehouse loader + transforms |
| test_benchmark | 13 | 14 | Benchmark runner |
| test_demo | 8 | 7 | E-commerce demo |
| test_sensor_demo | 8 | 8 | Sensor pipeline demo |
| test_analytics | 10 | 5 | SQLite analytics |
| test_manifest | 13 | 11 | Manifest utility |
| test_config | 12 | 12 | Config loader |

---

## Current strongest workflows

1. **E-commerce demo** — the clearest entry point. Four tables through
   ingestion, validation, cleaning, and analytical queries. Produces identical
   results in both languages (60 rows in, 57 out, 3 rejected, 6 warnings).

2. **Sensor pipeline demo** — demonstrates the orchestration runner in a real
   workflow with dead-letter routing, hourly aggregation, and manifest
   generation. Good contrast with the e-commerce demo's direct-call approach.

3. **Warehouse CLI** — the only workflow that exercises the full ELT path:
   raw files → SQLite staging → star-schema dims/facts → analytical queries.
   Uses the SQL assets in `sql/warehouse/` programmatically.

4. **Streaming processor** — the most educational single module. Covers
   per-event validation, deduplication, event-time watermarks, lateness
   classification, and dead-letter routing in ~350 lines.

---

## Current architectural limits

**Placeholder modules remain.** `storage/`, `utils/`, and `cli/` exist as
empty directories with docstring-only files in both languages. They appear in
the project structure but contribute nothing. They should either be
implemented or removed — the current state is the repo's most visible
structural weakness.

**Two demo styles can confuse.** The e-commerce demo uses direct function
calls; the sensor demo uses the orchestration runner. The difference is
intentional (direct vs. orchestrated) and documented, but someone browsing
the code without reading docs may not understand the relationship.

**No unified CLI.** Each module has its own entry point. There is no single
`data-platform-lab run <workflow>` command. Users must know the module path
(e.g., `python -m data_platform_lab.warehouse.cli`).

**Benchmark results are machine-dependent.** The benchmark exercise measures
wall-clock time. Results vary across machines and cannot be asserted in
tests. Tests verify output structure and row counts, not timing.

**Manifests are best-effort.** Pipelines wrap manifest writes in try/except so
tests running in temp directories don't fail. This means manifest generation is
not verified by most tests — a manifest could silently break without test
failure.

**`node:sqlite` is experimental.** The JavaScript analytics and warehouse
modules depend on Node.js's `node:sqlite`, which is experimental in Node 22.
This could change in future releases.

**No streaming windows.** The streaming processor does per-run aggregation —
it processes the full input file and computes aggregates across all accepted
events. It does not implement tumbling, sliding, or session windows. The
watermark infrastructure exists but is not used for windowed computation.

**Single-process design.** All processing is sequential within a pipeline run.
The benchmark exercise demonstrates concurrency for file I/O but the core
exercises process events one at a time. This is intentional for a learning
lab but limits the patterns that can be demonstrated.

---

## What remains future work

| Area | Status | Notes |
| --- | --- | --- |
| Log parsing exercise | Not started | `data/sample/logs.log` (26 lines, structured key-value) exists; no implementation |
| Placeholder modules | Empty shells | `storage/`, `utils/`, `cli/` have docstring-only files in both languages |
| Streaming windowed aggregation | Not implemented | Watermark exists; tumbling/sliding windows do not |
| Unified CLI | Not started | Each module has its own `cli.py`; no dispatcher |
| Lock file reproducibility | Lock files gitignored | Acceptable trade-off for a learning repo, but not explicit |
| DuckDB integration | Not started | Could complement SQLite for analytical queries with native CSV/Parquet support |
| Real database connections | Not applicable | All pipelines use file I/O or in-memory SQLite |
| Distributed processing | Not applicable | Single-process by design |

---

## Recommended Milestone 3 targets

1. **Resolve placeholder modules.** Either implement `storage/`, `utils/`,
   `cli/` with real functionality or delete them. The current empty-shell
   state undermines the repo's otherwise clean structure. This is the
   single highest-priority cleanup.

2. **Log parsing exercise.** Build a structured extraction pipeline over
   `data/sample/logs.log`. The sample data has 26 lines with structured
   key-value pairs. This would add a new data format (semi-structured text)
   and a well-scoped exercise.

3. **Unified CLI dispatcher.** Create a single entry point that routes to
   existing module CLIs: `data-platform-lab run demo`,
   `data-platform-lab run warehouse`, etc. This makes the repo more
   accessible without changing any module internals.

4. **Streaming windowed aggregation.** Add tumbling or sliding time windows
   to the streaming processor. The watermark infrastructure already exists;
   windows are the natural next step. This would let the repo demonstrate
   a core streaming concept it currently only gestures toward.

5. **DuckDB analytical layer.** Add an alternative analytical path using
   DuckDB alongside the existing SQLite layer. DuckDB handles CSV and
   Parquet natively and would demonstrate a different analytical pattern
   (columnar, analytical-first) vs. SQLite (row-oriented, general-purpose).

6. **Commit lock files.** Either commit `poetry.lock` and `yarn.lock` for
   build reproducibility, or document the trade-off explicitly in the
   developer guide. The current silent `.gitignore` entry is the least
   helpful option.

7. **Manifest testing.** Add dedicated tests that verify manifest files are
   actually written with correct content, rather than relying on best-effort
   writes that are silently skipped in test environments.

---

## Repository scale at M2

| Category | Count |
| --- | ---: |
| Python source files | 35 |
| JavaScript source files | 33 |
| SQL scripts | 27 |
| Python tests | 251 (17 files) |
| JavaScript tests | 235 (16 files) |
| Documentation files | 23 |
| Config files | 3 |
| Sample data files | 11 |

---

## How to explore the repository at M2

**Run the demos:**

```bash
cd python && poetry install
poetry run python -m data_platform_lab.demo
poetry run python -m data_platform_lab.sensor_demo
```

**Run the warehouse pipeline:**

```bash
poetry run python -m data_platform_lab.warehouse.cli
```

**Run benchmarks:**

```bash
poetry run python -m data_platform_lab.benchmark.cli --num-files 50
```

**Use a config file:**

```bash
poetry run python -m data_platform_lab.streaming.cli --config ../configs/streaming.json
```

**Study the exercises:** Follow the [roadmap](roadmap.md) from Exercise 01
through 09. Start with [milestone-m1.md](milestone-m1.md) for history,
[platform-conventions.md](platform-conventions.md) for cross-module patterns.
