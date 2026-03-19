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

## CLI Entry Points

Run the CSV ingestion pipeline against sample data:

```bash
poetry run python -m data_platform_lab.ingestion \
  --input-dir ../data/sample \
  --output ../data/bronze/merged.csv \
  --required-columns customer_id,email
```

Run the API ingestion pipeline (fetches from JSONPlaceholder by default):

```bash
poetry run python -m data_platform_lab.ingestion.api_cli --max-pages 2
```

Both commands accept `--help` for full option details.

## Package Layout

```text
src/data_platform_lab/
├── __init__.py
├── ingestion/          → CSV and API ingestion (exercises 01–02)
├── transform/          → incremental ETL and CDC snapshot diff (exercises 04–05)
├── validation/         → composable data quality rules and runner (exercise 03)
├── orchestration/      → sequential pipeline runner and customer ETL workflow (exercise 06)
├── observability/      → timing, run metadata, and structured reporting (exercise 07)
├── demo.py             → end-to-end e-commerce demo
└── analytics.py        → SQLite analytical queries over curated output
```

Additional module directories (`cli/`, `storage/`, `streaming/`, `warehouse/`, `utils/`) exist as placeholders for future exercises. They contain only docstrings and no implementation.

## Implemented Modules

| Module | What it contains |
| --- | --- |
| `ingestion` | CSV pipeline (read, validate, standardize, deduplicate, write) and API pipeline (paginated fetch, retry, transform, save raw + processed). |
| `transform` | Incremental ETL with checkpoint-based deduplication and CDC snapshot comparison (inserts, updates, deletes). |
| `validation` | Six composable check functions (required columns, no nulls, unique, numeric range, allowed values, date format) and a runner that aggregates results. |
| `orchestration` | Sequential pipeline runner with retry, skip, shared context, and timing. Plus a customer ETL workflow wired to real modules. |
| `observability` | Timer, RunTracker (context manager with counters, warnings, extras), run metadata formatting. |

## Conventions

- **Python 3.11+** is required.
- **Type annotations** on all public functions and methods.
- **Docstrings** on all modules and public interfaces.
- **Tests** live in `tests/` and mirror the `src/` structure.
- **Ruff** enforces style and import ordering. The rule set includes `E`, `F`, `I`, `UP`, `B`, `SIM`, and `RUF`.
- **mypy** runs in strict mode.
