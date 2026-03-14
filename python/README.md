# Data Platform Lab — Python

Python implementation of the data platform lab. This package provides structured modules for practicing data engineering patterns: ingestion, transformation, validation, storage layout, orchestration, observability, streaming, warehouse loading, and CLI tooling.

Type hints are expected throughout. Each module is designed to work independently while following consistent conventions for configuration, error handling, and data flow.

## Installation

```bash
cd python
poetry install
```

This installs the package in editable mode along with all development dependencies.

## Development Setup

The project uses three development tools:

- **pytest** — test runner
- **ruff** — linter and formatter
- **mypy** — static type checker

All three are included in the `dev` dependency group and installed automatically by `poetry install`.

## Commands

Run tests:

```bash
poetry run pytest
```

Run linter:

```bash
poetry run ruff check src/ tests/
```

Run formatter:

```bash
poetry run ruff format src/ tests/
```

Run type checker:

```bash
poetry run mypy src/
```

## Package Layout

```text
src/data_platform_lab/
├── __init__.py
├── ingestion/          → read from files, APIs, archives, external sources
├── transform/          → clean, reshape, enrich, aggregate datasets
├── validation/         → enforce schemas and data quality checks
├── storage/            → write to layered storage with format and partition control
├── orchestration/      → schedule and coordinate multi-step pipelines
├── observability/      → structured logging, metrics, lineage tracking
├── streaming/          → event-driven and near-real-time processing patterns
├── warehouse/          → load into analytical stores, run warehouse-style queries
├── utils/              → shared helpers: paths, I/O, config, type definitions
└── cli/                → command-line entry points for pipelines and utilities
```

## Module Responsibilities

| Module | Responsibility |
| --- | --- |
| `ingestion` | Read data from flat files (CSV, JSON), HTTP APIs with pagination and retries, compressed archives, and structured log files. |
| `transform` | Apply column mapping, type casting, deduplication, filtering, derived fields, and medallion-layer promotion logic. |
| `validation` | Define schemas, enforce contracts at pipeline boundaries, detect anomalies, and route invalid records to dead-letter storage. |
| `storage` | Write to raw/bronze/silver/gold layers, select file formats (CSV, JSON, Parquet), apply partitioning, and track manifests. |
| `orchestration` | Resolve task dependencies, execute DAG-based pipelines, manage retries and checkpoints, and load pipeline definitions from configuration. |
| `observability` | Emit structured logs, record execution timing and row counts, track data lineage, and monitor pipeline health. |
| `streaming` | Simulate event streams, apply windowed aggregation, bridge streaming to batch, and implement producer/consumer patterns locally. |
| `warehouse` | Load silver/gold data into SQLite or DuckDB, write analytical queries (CTEs, window functions, SCDs), and produce consumption-layer datasets. |
| `utils` | Resolve paths, handle file I/O, format timestamps, load configuration, and define shared types. |
| `cli` | Expose entry points for running pipelines, inspecting data, and executing developer utilities from the command line. |

## Conventions

- **Python 3.11+** is required.
- **Type annotations** on all public functions and methods.
- **Docstrings** on all modules and public interfaces.
- **Tests** live in `tests/` and mirror the `src/` structure.
- **Ruff** enforces style and import ordering. The rule set includes `E`, `F`, `I`, `UP`, `B`, `SIM`, and `RUF`.
- **mypy** runs in strict mode.
