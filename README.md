# Data Platform Lab

A hands-on data engineering laboratory implemented in both Python and JavaScript. Seven exercises cover batch ingestion, schema validation, data quality, incremental ETL, change data capture, orchestration, and observability. An end-to-end demo processes four e-commerce datasets through the full pipeline — from raw CSV to cleaned output to SQLite analytics.

All implementations use only standard library modules. No pandas, no Spark, no external frameworks — just the core patterns, built from scratch.

## Run the demo

The fastest way to see the repository in action. Processes customers, products, orders, and order items through ingestion, validation, cleaning, and analytical queries.

```bash
# Python
cd python
poetry install
poetry run python -m data_platform_lab.demo

# JavaScript
cd javascript
corepack enable && yarn install
node src/demo.js
```

Output: 60 rows in, 57 rows out, 3 rejected, 6 data-quality warnings. Cleaned CSVs in `data/silver/demo/`, JSON manifest in `data/manifests/`.

Then load into SQLite and run analytical queries:

```bash
# Python
poetry run python -m data_platform_lab.analytics

# JavaScript
node src/analytics.js
```

See [docs/end-to-end-demo.md](docs/end-to-end-demo.md) and [docs/sqlite-analytics-demo.md](docs/sqlite-analytics-demo.md) for details.

### Sensor pipeline demo

A second showcase that processes event data through the orchestration runner (Exercise 06):

```bash
# Python
cd python && poetry run python -m data_platform_lab.sensor_demo

# JavaScript
cd javascript && node src/sensor-demo.js
```

Output: 16 events in, 14 accepted, 1 rejected, 1 duplicate. Hourly aggregates, dead-letter file, and JSON manifest. See [docs/sensor-pipeline-demo.md](docs/sensor-pipeline-demo.md).

## What is implemented

Seven exercises, each in both Python and JavaScript with full test coverage:

| # | Exercise | Key concepts | Guide |
| --- | --- | --- | --- |
| 01 | CSV ingestion | Read flat files, standardize headers, deduplicate, write to bronze | [guide](docs/01-csv-ingestion-pipeline.md) |
| 02 | API ingestion | Fetch paginated JSON, retry with backoff, store raw + processed | [guide](docs/02-api-ingestion-pipeline.md) |
| 03 | Validation framework | Composable rules, severity levels, pass/fail gating | [guide](docs/03-validation-framework.md) |
| 04 | Incremental ETL | Checkpoint persistence, idempotent reruns, process only new records | [guide](docs/04-incremental-etl-pipeline.md) |
| 05 | Snapshot diff (CDC) | Compare snapshots to detect inserts, updates, and deletes | [guide](docs/05-snapshot-diff.md) |
| 06 | Orchestration runner | Sequential step execution, retry logic, shared context | [guide](docs/06-orchestration-runner.md) |
| 07 | Observability | Execution timing, run metadata, counters, structured reporting | [guide](docs/07-observability.md) |

Plus two showcase demos and supporting assets:
- **E-commerce demo** — processes all 4 tables through direct function calls with `RunTracker` ([guide](docs/end-to-end-demo.md))
- **Sensor pipeline demo** — processes event data through the orchestration runner with dead-letter routing ([guide](docs/sensor-pipeline-demo.md))
- **SQLite analytics** — loads curated output into SQLite and runs 5 analytical queries ([guide](docs/sqlite-analytics-demo.md))
- **27 SQL scripts** — DDL, DML, analytical queries, and warehouse ETL patterns in `sql/`

See [docs/exercise-index.md](docs/exercise-index.md) for file locations and dependencies. See [docs/roadmap.md](docs/roadmap.md) for learning order.

## Quickstart

### Prerequisites

- **Python 3.11+** and [Poetry](https://python-poetry.org/)
- **Node.js 22+** (Yarn is enabled via [Corepack](https://nodejs.org/api/corepack.html); the analytics module uses `node:sqlite`)
- **SQLite 3.35+** (for SQL exercises, optional)

### Set up

```bash
git clone <repo-url> && cd data-platform-lab

# Python
cd python
poetry install
poetry run pytest
cd ..

# JavaScript
cd javascript
corepack enable
yarn install
yarn test
cd ..
```

See [docs/local-development.md](docs/local-development.md) for detailed setup, common commands, and known pitfalls.

## Why this repository exists

Most data engineering learning resources stop at "read a CSV and load it into a database." Real-world pipelines involve checkpointing, schema evolution, idempotent loads, layered storage, quality gates, and observable execution.

This repository closes that gap through deliberate practice — building small, focused pipelines that exercise production-grade patterns without requiring production-scale infrastructure. Every exercise is self-contained, runnable locally, and focused on one or two core concepts.

### Dual implementation

Each exercise is implemented in both Python and JavaScript. The goal is not identical code, but solving the same data engineering problems using each language's idiomatic tools. Both languages use only built-in modules — no external data processing libraries.

## Data architecture

Data follows a layered storage model (medallion architecture):

| Layer | Directory | Purpose |
| --- | --- | --- |
| Raw | `data/raw/` | Landing zone — untouched source files |
| Bronze | `data/bronze/` | Lightly parsed, append-only |
| Silver | `data/silver/` | Cleaned, deduplicated, conformed |
| Gold | `data/gold/` | Business aggregates and analytics outputs |
| Checkpoints | `data/checkpoints/` | Pipeline state for incremental loads |
| Manifests | `data/manifests/` | Run metadata and lineage |
| Sample | `data/sample/` | Committed sample datasets for tests |

Generated data is git-ignored. Only `data/sample/` is committed.

## Repository structure

```text
data-platform-lab/
├── python/                        # Python implementations
│   ├── pyproject.toml
│   ├── src/data_platform_lab/
│   │   ├── ingestion/             #   exercises 01–02
│   │   ├── transform/             #   exercises 04–05
│   │   ├── validation/            #   exercise 03
│   │   ├── orchestration/         #   exercise 06 + customer ETL workflow
│   │   ├── observability/         #   exercise 07
│   │   ├── demo.py                #   end-to-end demo
│   │   └── analytics.py           #   SQLite analytics
│   └── tests/
├── javascript/                    # JavaScript implementations (same structure)
│   ├── package.json
│   ├── src/
│   └── tests/
├── sql/                           # 27 standalone SQL scripts
│   ├── ddl/                       #   table definitions
│   ├── dml/                       #   data loading
│   ├── analytics/                 #   analytical queries
│   └── warehouse/                 #   star-schema ETL patterns
├── data/
│   └── sample/                    # committed sample datasets
├── docs/                          # exercise guides, conventions, review docs
└── .github/workflows/             # CI for both Python and JavaScript
```

## Technology stack

| Area | Tools |
| --- | --- |
| Python | Poetry, pytest, Ruff, mypy (strict) |
| JavaScript | Yarn, Node.js built-in test runner, ESLint |
| SQL | SQLite |
| CI | GitHub Actions |

## Planned (not yet implemented)

| Exercise | Concepts |
| --- | --- |
| Log parsing | Semi-structured text to structured records |

## Documentation

| Document | Purpose |
| --- | --- |
| [Milestone M1](docs/milestone-m1.md) | What is implemented and what comes next |
| [Exercise index](docs/exercise-index.md) | Quick reference for all exercises |
| [Roadmap](docs/roadmap.md) | Recommended learning order |
| [End-to-end demo](docs/end-to-end-demo.md) | E-commerce showcase (direct function calls) |
| [Sensor pipeline demo](docs/sensor-pipeline-demo.md) | Event showcase (orchestration runner) |
| [SQLite analytics](docs/sqlite-analytics-demo.md) | Analytical queries over curated output |
| [Orchestration in the repo](docs/orchestrated-workflow.md) | How the runner is used across demos |
| [Platform conventions](docs/platform-conventions.md) | Naming, status strings, result shapes |
| [Testing strategy](docs/testing-strategy.md) | Test categories and coverage |
| [Local development](docs/local-development.md) | Setup, commands, pitfalls |
| [Current state review](docs/current-state-review.md) | Honest quality assessment |

## License

[MIT](LICENSE)
