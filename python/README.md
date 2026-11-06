# Data Platform Lab — Python

Python implementation of the data platform lab. This package provides structured modules for practicing data engineering patterns: ingestion, transformation, validation, storage layout, orchestration, observability, streaming, warehouse loading, and CLI tooling.

Type hints are expected throughout. Each module is designed to work independently while following consistent conventions for configuration, error handling, and data flow.

## Installation

```bash
cd python
poetry check --lock
poetry sync --extras serving
```

This installs the package in editable mode, development dependencies, and the
optional analytics/serving dependencies required by the complete offline suite.
Add the `mongodb` extra for live PyMongo tests. Core-only users can omit extras.

Use Poetry **2.5.1**, as used by CI. The committed `poetry.lock` covers Python
**3.11–3.14**, all optional extras, and development dependencies. Normal setup
uses its recorded versions and hashes. `sync` also removes unselected packages
from the project virtualenv. See the [lockfile workflow](../docs/local-development.md#dependency-lockfiles)
for deliberate updates and reproducibility limits.

## Development Setup

The project uses three development tools:

- **pytest** — test runner
- **ruff** — linter and formatter
- **mypy** — static type checker

All three are included in the `dev` dependency group and installed automatically by `poetry sync` or `poetry install`.

## Commands

Run tests:

```bash
poetry run pytest
```

Run linter:

```bash
poetry run ruff check --config pyproject.toml . ../scripts
```

Run formatter:

```bash
poetry run ruff format --config pyproject.toml . ../scripts
```

Run type checker:

```bash
poetry run mypy src/ ../scripts
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

The package also implements platform CLI, storage, streaming, warehouse,
metadata and Iceberg modules. ONS processing lives in `curation/`; verified
queries, sink publishing, evidence and manual release controls live in `serving/`.
See the [publishing readiness checklist](../docs/ons-pay-publishing-readiness.md)
and [complete local walkthrough](../docs/ons-pay-publishing-demo.md).

## Implemented Modules

| Module | What it contains |
| --- | --- |
| `ingestion` | CSV pipeline (read, validate, standardize, deduplicate, write) and API pipeline (paginated fetch, retry, transform, save raw + processed). |
| `transform` | Incremental ETL with checkpoint-based deduplication and CDC snapshot comparison (inserts, updates, deletes). |
| `validation` | Six composable check functions (required columns, no nulls, unique, numeric range, allowed values, date format) and a runner that aggregates results. |
| `orchestration` | Sequential pipeline runner with retry, skip, shared context, and timing. Plus a customer ETL workflow wired to real modules. |
| `observability` | Timer, RunTracker (context manager with counters, warnings, extras), run metadata formatting. |
| `curation` | ONS CSV quality gates, Silver/Gold SQL models and immutable verified Parquet outputs. |
| `serving` | Pinned ONS queries, SQL/document plans, publishing, reconciliation, acceptance, durable outcomes and separate manual release controls. |

## Conventions

- **Python 3.11–3.14** is required.
- **Type annotations** on all public functions and methods.
- **Docstrings** on all modules and public interfaces.
- **Tests** live in `tests/` and mirror the `src/` structure.
- **Ruff** enforces style and import ordering. The rule set includes `E`, `F`, `I`, `UP`, `B`, `SIM`, and `RUF`.
- **mypy** runs in strict mode.
