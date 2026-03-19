# Milestone 1

Completed: 2026-03-19

---

## Purpose

Milestone 1 establishes the repository as a working data engineering lab with
a core set of exercises, a polished end-to-end demo, and reliable tooling.
The goal is a coherent, runnable project that demonstrates real data
engineering patterns — not a broad framework.

---

## What is implemented

### Seven exercises (Python + JavaScript)

| # | Exercise | Pattern |
|---|----------|---------|
| 01 | CSV ingestion | Read, validate, standardize, deduplicate, write |
| 02 | API ingestion | Paginated fetch, retry with backoff, raw + processed storage |
| 03 | Validation framework | Composable rules, severity levels, pass/fail gating |
| 04 | Incremental ETL | Checkpoint persistence, idempotent reruns, deduplication |
| 05 | Snapshot diff (CDC) | Compare snapshots to detect inserts, updates, deletes |
| 06 | Orchestration runner | Sequential steps, retry, skip, shared context, timing |
| 07 | Observability | Timer, RunTracker, counters, warnings, structured metadata |

Each exercise has a documentation guide in `docs/`, implementations in both
languages, and tests covering happy paths, edge cases, malformed input,
failure recovery, and rerun safety.

### End-to-end demo

Processes 4 e-commerce tables (customers, products, orders, order_items)
through ingestion, validation, cleaning, and curated output. Both languages
produce identical results: 60 rows in, 57 out, 3 rejected, 6 warnings.

### SQLite analytics

Loads curated CSVs into SQLite and runs 5 analytical queries: daily revenue,
top products, customer order counts, orphan FK detection, duplicate
detection. Results written as report CSVs and a JSON summary.

### Orchestrated workflow

The orchestration runner (Exercise 06) wired to real modules — processes
customers through extract, validate, clean, load, and report steps.

### SQL assets

27 standalone SQL scripts: 6 DDL, 6 DML, 8 analytics, 7 warehouse ETL.
All SQLite-compatible.

### Test suite

159 Python tests, 144 JavaScript tests. Zero failures. Coverage includes:
unit tests, integration tests, rerun safety, failure recovery, golden output
verification, JSON shape validation, and CDC determinism.

### Tooling

- Ruff (lint + format) — zero errors
- mypy strict — zero errors
- ESLint — zero errors
- GitHub Actions CI — lint and test as separate jobs, demo smoke test

---

## What is stable

- All 7 exercise implementations and their tests
- The end-to-end demo pipeline
- The SQLite analytics layer
- The sample data in `data/sample/` (intentional quality issues documented)
- The platform conventions (snake_case result objects, status strings, checkpoint format)
- The CI pipeline

---

## What is intentionally not part of Milestone 1

| Area | Status |
|------|--------|
| Streaming exercises | Placeholder module exists; no implementation |
| Warehouse loading CLI | Planned; SQL assets exist but no programmatic wrapper |
| Log parsing exercise | Planned; `logs.log` sample data exists |
| Benchmark exercise | Planned; no implementation |
| `storage/`, `utils/`, `cli/` modules | Placeholder directories; no implementation |
| Manifest generation in individual exercises | Only the demo generates manifests |
| Shared test fixtures (`conftest.py`) | Each test file is self-contained |
| CI dependency caching | Not configured; builds from scratch each run |

These are deferred to Milestone 2, not forgotten.

---

## Known limitations

- **Standard library only.** No pandas, no Polars, no external data
  processing libraries. This is intentional for the learning exercises but
  limits what the pipelines can demonstrate.
- **File-based only.** All pipelines read and write local files. No database
  connections, no cloud storage, no message queues.
- **No parallelism.** All processing is sequential. The orchestration runner
  executes steps one at a time.
- **Lock files are gitignored.** Builds are not fully reproducible across
  machines. Acceptable for a learning repo.
- **Node.js `node:sqlite` is experimental.** The JS analytics module depends
  on a feature marked experimental in Node 22.
- **Small scale.** Sample data is 60 rows total. The patterns are
  production-grade but the data volume is not.

---

## Recommended Milestone 2 targets

1. **Log parsing exercise** — use `data/sample/logs.log` to build a
   structured extraction pipeline. The sample data already exists.
2. **Event processing exercise** — use `data/sample/sensor_events.json` for
   windowed aggregation and anomaly detection.
3. **Remove empty placeholder modules** or implement them. The current
   state (empty directories in the structure diagram) weakens trust.
4. **Shared test fixtures** — create `conftest.py` and JS test helpers to
   reduce duplication across test files.
5. **CI dependency caching** — add Poetry and Yarn cache steps to speed up
   builds.

---

## How to explore this repository

**If you want to see it work:** Run the demo (`python -m data_platform_lab.demo`
then `python -m data_platform_lab.analytics`). See
[docs/end-to-end-demo.md](end-to-end-demo.md).

**If you want to learn the exercises:** Start with the roadmap
([docs/roadmap.md](roadmap.md)) and work through exercises 01-07 in order.

**If you want to study the code:** Each exercise lives in a module under
`src/`. The tests define expected behavior. The exercise guides in `docs/`
explain design decisions and limitations.

**If you want to understand the conventions:** See
[docs/platform-conventions.md](platform-conventions.md) for naming, status
strings, and result shapes.

**If you want an honest assessment:** See
[docs/current-state-review.md](current-state-review.md) for strengths,
weaknesses, and prioritized next steps.
