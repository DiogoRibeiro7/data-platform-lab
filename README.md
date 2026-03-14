# Data Platform Lab

A hands-on data engineering laboratory for building practical skills across the modern data stack. Every module is implemented in both **Python** and **JavaScript** so you can compare idioms, tooling, and trade-offs side by side.

## Purpose

This repository is a structured learning environment for data engineering fundamentals. It provides exercises and mini-projects that cover the full lifecycle of data — from ingestion through transformation, validation, storage, orchestration, and observability.

## Learning Goals

- **Ingestion** — read from files, APIs, databases, and message queues.
- **Transformation** — clean, reshape, enrich, and aggregate datasets.
- **Validation** — enforce schemas, detect anomalies, and build data quality checks.
- **Storage Layout** — understand raw/bronze/silver/gold medallion architecture and file formats (CSV, JSON, Parquet, etc.).
- **Orchestration** — schedule and coordinate multi-step pipelines.
- **Observability** — instrument pipelines with logging, metrics, and lineage tracking.
- **Streaming** — process data in near-real-time with event-driven patterns.
- **SQL & Analytics** — write DDL, DML, and analytical queries for warehouse-style workloads.

## Dual-Language Approach

Each module lives under both `python/` and `javascript/`. The goal is **not** to produce identical code, but to solve the same data engineering problems using each language's strengths:

| Concern | Python | JavaScript |
|---------|--------|------------|
| DataFrames | pandas / Polars | Arquero / Danfo.js |
| Validation | Great Expectations / Pandera | Zod / Joi |
| Orchestration | Prefect / Dagster | custom / BullMQ |
| Streaming | Faust / Kafka-python | kafkajs / Node streams |
| CLI | Click / Typer | Commander / yargs |

## Repository Structure

```text
data-platform-lab/
├── README.md              # this file
├── LICENSE                 # MIT license
├── .gitignore
├── docs/                   # guides, diagrams, ADRs
├── data/                   # medallion-architecture data layers
│   ├── raw/                #   landing zone — untouched source data
│   ├── bronze/             #   lightly parsed, append-only
│   ├── silver/             #   cleaned, deduplicated, conformed
│   ├── gold/               #   business-level aggregates
│   ├── staging/            #   intermediate / scratch area
│   ├── checkpoints/        #   pipeline state & recovery
│   ├── manifests/          #   file/run manifests for lineage
│   └── sample/             #   small datasets for tests & demos
├── python/                 # Python implementation
│   ├── pyproject.toml
│   ├── README.md
│   ├── src/data_platform_lab/
│   └── tests/
├── javascript/             # JavaScript implementation
│   ├── package.json
│   ├── README.md
│   ├── src/
│   └── tests/
├── sql/                    # SQL exercises
│   ├── ddl/                #   schema definitions
│   ├── dml/                #   inserts, updates, merges
│   ├── analytics/          #   analytical queries
│   └── warehouse/          #   warehouse-specific patterns
├── configs/                # shared configuration files
├── scripts/                # helper & automation scripts
└── .github/workflows/      # CI/CD pipelines
```

## Planned Modules

| # | Module | Description |
|---|--------|-------------|
| 1 | Flat-file ingestion | Read CSV/JSON, handle encodings and malformed rows |
| 2 | Schema validation | Define and enforce contracts on incoming data |
| 3 | Medallion transforms | Implement bronze → silver → gold promotion |
| 4 | Idempotent loads | Write-audit-publish pattern with checkpoints |
| 5 | SQL foundations | DDL, DML, CTEs, window functions |
| 6 | Pipeline orchestration | DAG-based scheduling with retries |
| 7 | Streaming basics | Produce and consume events in real time |
| 8 | Observability | Structured logging, metrics, and data lineage |
| 9 | CLI tooling | Build developer-friendly pipeline commands |
| 10 | End-to-end project | Tie all modules into a complete mini-platform |

## Getting Started

### Python

```bash
cd python
poetry install
```

### JavaScript

```bash
cd javascript
yarn install
```

### SQL

SQL files are standalone and can be run against any compatible database (SQLite, PostgreSQL, DuckDB, etc.).

## Contributing

This is a personal learning lab. Feel free to fork and adapt it to your own needs.

## License

[MIT](LICENSE)
