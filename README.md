# Data Platform Lab

A hands-on data engineering laboratory implemented in both Python and JavaScript. The repository contains structured exercises that cover batch pipelines, streaming-style processing, schema validation, data quality, incremental ETL, storage layouts, observability, orchestration, SQL, and analytics-ready outputs.

## Why This Repository Exists

Most data engineering learning resources stop at "read a CSV and load it into a database." Real-world pipelines involve checkpointing, schema evolution, idempotent loads, layered storage, quality gates, and observable execution. This repository exists to close that gap through deliberate practice — building small, focused pipelines that exercise production-grade patterns without requiring production-scale infrastructure.

Every exercise is designed to be self-contained, runnable locally, and focused on one or two core concepts at a time.

## Core Skills Trained

- **Batch ingestion** — reading from flat files, APIs, compressed archives, and structured logs.
- **Streaming-style processing** — event-driven patterns, windowed aggregation, and simulated real-time pipelines.
- **Schema validation** — defining contracts, enforcing them at pipeline boundaries, and handling violations.
- **Data quality** — building reusable checks, tracking quality metrics, and gating downstream processing on pass/fail results.
- **Incremental ETL** — checkpointed loads, append-only ingestion, and change data capture comparisons.
- **Storage layouts** — medallion architecture (raw → bronze → silver → gold), partitioning strategies, and file format trade-offs.
- **Observability** — structured logging, execution metrics, data lineage, and pipeline health monitoring.
- **Orchestration** — task scheduling, dependency resolution, retry logic, and DAG-based execution.
- **SQL and warehouse thinking** — DDL, DML, CTEs, window functions, and analytical query patterns.
- **Analytics-ready outputs** — producing clean, documented, consumption-layer datasets.

## Repository Philosophy

Each exercise is developed with three layers in mind:

- **Code layer** — the implementation itself: functions, modules, and CLI entry points that do the actual work of reading, transforming, validating, and writing data.
- **Pipeline layer** — the execution logic that ties code together: task ordering, checkpoint management, retry behavior, configuration, and scheduling.
- **Data layer** — the artifacts that flow through the pipeline: raw inputs, intermediate representations, validated outputs, and the metadata that tracks their lineage.

An exercise is not complete until all three layers are addressed. Writing a transformation function without thinking about how it fits into a pipeline or where its output lands is only a third of the work.

## Dual Implementation

Most exercises are implemented twice — once in Python and once in JavaScript / Node.js. The goal is not to produce identical code in both languages, but to solve the same data engineering problems using each language's idiomatic tools and patterns.

Python is the dominant language in data engineering. JavaScript is included to strengthen general-purpose engineering skills, to explore how data problems map onto a different runtime and ecosystem, and to build fluency in a language that shows up across web services, serverless functions, and tooling scripts.

| Concern | Python | JavaScript |
| --- | --- | --- |
| DataFrames | pandas, Polars | Arquero, Danfo.js |
| Validation | Pandera, Great Expectations | Zod, Joi |
| Orchestration | Prefect, Dagster | custom runners, BullMQ |
| Streaming | Faust, kafka-python | kafkajs, Node streams |
| CLI | Click, Typer | Commander, yargs |

## Data Architecture

Data produced and consumed by the exercises follows a layered storage model:

| Layer | Directory | Purpose |
| --- | --- | --- |
| Raw | `data/raw/` | Landing zone. Untouched source files exactly as received. |
| Bronze | `data/bronze/` | Lightly parsed, append-only. Preserves original structure with added metadata. |
| Silver | `data/silver/` | Cleaned, deduplicated, conformed. Ready for joins and aggregation. |
| Gold | `data/gold/` | Business-level aggregates and analytics-ready datasets. |
| Staging | `data/staging/` | Intermediate scratch area for in-progress transformations. |
| Checkpoints | `data/checkpoints/` | Pipeline state for incremental and resumable loads. |
| Manifests | `data/manifests/` | File and run manifests for lineage tracking. |
| Sample | `data/sample/` | Small, committed datasets for tests and demonstrations. |

Generated data in `raw/`, `bronze/`, `silver/`, `gold/`, `staging/`, `checkpoints/`, and `manifests/` is git-ignored. Only `sample/` contains committed files.

## Repository Structure

```text
data-platform-lab/
├── README.md
├── LICENSE
├── .gitignore
├── docs/                          # guides, diagrams, architecture decisions
├── data/                          # layered data storage (see Data Architecture)
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── staging/
│   ├── checkpoints/
│   ├── manifests/
│   └── sample/
├── python/                        # Python implementations
│   ├── pyproject.toml             #   Poetry project definition
│   ├── README.md
│   ├── src/data_platform_lab/     #   source package
│   │   ├── ingestion/
│   │   ├── transform/
│   │   ├── validation/
│   │   ├── storage/
│   │   ├── orchestration/
│   │   ├── observability/
│   │   ├── streaming/
│   │   ├── warehouse/
│   │   ├── utils/
│   │   └── cli/
│   └── tests/
├── javascript/                    # JavaScript implementations
│   ├── package.json               #   Yarn project definition
│   ├── README.md
│   ├── src/
│   │   ├── ingestion/
│   │   ├── transform/
│   │   ├── validation/
│   │   ├── storage/
│   │   ├── orchestration/
│   │   ├── observability/
│   │   ├── streaming/
│   │   ├── warehouse/
│   │   ├── utils/
│   │   └── cli/
│   └── tests/
├── sql/                           # SQL exercises
│   ├── ddl/                       #   schema definitions
│   ├── dml/                       #   inserts, updates, merges
│   ├── analytics/                 #   analytical queries
│   └── warehouse/                 #   warehouse-specific patterns
├── configs/                       # shared configuration files
├── scripts/                       # helper and automation scripts
└── .github/workflows/             # CI/CD pipelines
```

## Technology Stack

| Area | Tools |
| --- | --- |
| Python | Poetry, pytest, Ruff |
| JavaScript | Yarn, Node.js built-in test runner |
| SQL | SQLite, DuckDB, PostgreSQL (any compatible engine) |
| Data formats | CSV, JSON, Parquet |
| CI | GitHub Actions |

## Planned Exercises

| # | Exercise | Key concepts |
| --- | --- | --- |
| 1 | CSV ingestion | Read flat files, handle encodings, parse malformed rows, write to bronze |
| 2 | API ingestion | Fetch paginated JSON from HTTP endpoints, handle rate limits and retries |
| 3 | ZIP extraction | Decompress archives, inventory contents, route files to appropriate layers |
| 4 | Log parsing | Parse semi-structured log files into structured records |
| 5 | Checkpointed incremental ETL | Track high-water marks, resume from last checkpoint, guarantee idempotency |
| 6 | CDC snapshot comparison | Compare successive snapshots to detect inserts, updates, and deletes |
| 7 | Schema validation framework | Define schemas, validate at pipeline boundaries, route failures to dead-letter storage |
| 8 | Data quality framework | Build reusable quality checks with pass/fail gating and metric collection |
| 9 | Warehouse loading with SQLite | Load silver/gold data into SQLite, write analytical queries against it |
| 10 | Event processing simulation | Simulate a stream of events, apply windowed aggregation, emit results |
| 11 | Orchestration runner | Build a minimal DAG runner with dependency resolution and retry logic |
| 12 | Ingestion benchmark | Compare sequential, parallel, and async ingestion strategies on throughput and resource usage |

## How to Use This Repository

### Python

```bash
cd python
poetry install
poetry run pytest
```

### JavaScript

```bash
cd javascript
yarn install
yarn test
```

### SQL

SQL files in `sql/` are standalone scripts. Run them against any compatible database engine:

```bash
sqlite3 < sql/ddl/create_tables.sql
```

### Working through exercises

Each exercise lives in a module directory under `src/` (e.g., `python/src/data_platform_lab/ingestion/`). Exercises are designed to be worked in order but can be tackled independently. Accompanying tests in `tests/` verify expected behavior.

## Development Principles

- **Small, focused exercises.** Each lab targets one or two concepts. Complexity comes from composing simple parts, not from monolithic implementations.
- **Three-layer thinking.** Every exercise should address code, pipeline, and data concerns. A transformation without a pipeline to run it or a storage layout to land it is incomplete.
- **Idempotency by default.** Pipelines should be safe to re-run. Design for append-only ingestion, checkpoint-based resumption, and deterministic outputs.
- **Observability from the start.** Log what matters, measure what moves, and track what changes. Do not treat observability as an afterthought.
- **Tests as specifications.** Tests define what an exercise should accomplish. Write them before or alongside the implementation.
- **No unnecessary abstractions.** Solve the problem at hand. Introduce shared utilities only when duplication becomes a maintenance burden.

## Roadmap

- [ ] Complete initial exercise implementations (ingestion, validation, transforms)
- [ ] Add sample datasets to `data/sample/` for each exercise
- [ ] Build orchestration runner with checkpoint support
- [ ] Add streaming simulation exercises
- [ ] Write SQL exercises for warehouse patterns (CTEs, window functions, slowly changing dimensions)
- [x] Set up GitHub Actions CI for both Python and JavaScript
- [ ] Add architecture diagrams and exercise guides to `docs/`
- [ ] Implement the end-to-end benchmark lab

## Development Notes

This is a personal learning and practice repository. It is not a framework or a library — it is a collection of exercises designed to build fluency in data engineering patterns.

Contributions, suggestions, and forks are welcome. If you use this structure for your own learning, consider adapting the exercises to datasets and problems relevant to your domain.

## License

[MIT](LICENSE)
