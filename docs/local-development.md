# Local Development

Everything you need to clone, set up, and work with this repository locally.

---

## Prerequisites

| Tool | Version | Check command | Notes |
|------|---------|---------------|-------|
| Python | 3.11+ | `python --version` | Required for the Python exercises |
| Poetry | 1.8+ | `poetry --version` | Python dependency manager ([install guide](https://python-poetry.org/docs/#installation)) |
| Node.js | 22+ | `node --version` | Required for the JavaScript exercises (22+ needed for `node:sqlite`) |
| Corepack | (bundled) | `corepack --version` | Ships with Node.js; used to enable Yarn |
| SQLite | 3.35+ | `sqlite3 --version` | Optional; only needed for SQL exercises |

You do not need all three runtimes to work on the repository. If you only want to do Python exercises, skip the Node.js setup and vice versa.

---

## Setup order

### 1. Clone

```bash
git clone <repo-url>
cd data-platform-lab
```

### 2. Python

```bash
cd python
poetry install
```

This installs the `data_platform_lab` package in editable mode plus three dev tools: **pytest**, **ruff**, and **mypy**.

Verify:

```bash
poetry run pytest
```

All tests should pass with zero failures.

### 3. JavaScript

```bash
cd javascript
corepack enable
yarn install
```

`corepack enable` activates the Yarn version declared in `package.json` (`yarn@4.6.0`). You only need to run it once per Node.js installation.

Verify:

```bash
yarn test
```

All tests should pass with zero failures.

### 4. SQL (optional)

No installation required. Run any script against SQLite:

```bash
sqlite3 lab.db < sql/ddl/01_customers.sql
sqlite3 lab.db < sql/dml/01_load_customers.sql
sqlite3 lab.db "SELECT * FROM customers LIMIT 5;"
```

Or load all schemas and data at once:

```bash
for f in sql/ddl/*.sql sql/dml/*.sql; do sqlite3 lab.db < "$f"; done
```

---

## Common commands

### Python

All commands run from the `python/` directory.

| Task | Command |
|------|---------|
| Run all tests | `poetry run pytest` |
| Run one test file | `poetry run pytest tests/test_validation.py -v` |
| Lint | `poetry run ruff check .` |
| Format | `poetry run ruff format .` |
| Format check (no changes) | `poetry run ruff format --check .` |
| Type check | `poetry run mypy src/` |
| CSV ingestion CLI | `poetry run python -m data_platform_lab.ingestion --input-dir ../data/sample --output ../data/bronze/merged.csv --required-columns customer_id,email` |
| API ingestion CLI | `poetry run python -m data_platform_lab.ingestion.api_cli --max-pages 2` |

### JavaScript

All commands run from the `javascript/` directory.

| Task | Command |
|------|---------|
| Run all tests | `yarn test` |
| Run one test file | `node --test tests/validation.test.js` |
| Lint | `yarn lint` |
| Lint and fix | `yarn lint:fix` |
| CSV ingestion CLI | `node src/ingestion/cli.js --input-dir ../data/sample --output ../data/bronze/merged.csv --required-columns customer_id,email` |
| API ingestion CLI | `node src/ingestion/api-cli.js --max-pages 2` |

### SQL

All commands run from the repository root.

| Task | Command |
|------|---------|
| Create tables | `for f in sql/ddl/*.sql; do sqlite3 lab.db < "$f"; done` |
| Load data | `for f in sql/dml/*.sql; do sqlite3 lab.db < "$f"; done` |
| Run a query | `sqlite3 lab.db < sql/analytics/01_daily_revenue.sql` |
| Reset database | `rm lab.db` |

---

## Project structure at a glance

```
data-platform-lab/
├── python/          → Poetry project (pyproject.toml)
├── javascript/      → Yarn project (package.json)
├── sql/             → Standalone SQL scripts
├── data/sample/     → Committed sample datasets
├── data/{raw,bronze,silver,gold,...}/  → Git-ignored output directories
├── docs/            → Exercise guides, roadmap, audit
└── .github/workflows/  → CI for both projects
```

Python source is in `python/src/data_platform_lab/`. JavaScript source is in `javascript/src/`. Tests live in `python/tests/` and `javascript/tests/` respectively.

---

## Data directories

The `data/` tree uses a medallion layout. Only `data/sample/` is committed to git. All other directories (`raw/`, `bronze/`, `silver/`, `gold/`, `staging/`, `checkpoints/`, `manifests/`) are git-ignored but preserved via `.gitkeep` files so the directory structure exists after cloning.

Pipeline outputs land in these directories. They can be safely deleted at any time:

```bash
# clean all generated data
rm -rf data/raw/* data/bronze/* data/silver/* data/gold/* data/staging/* data/checkpoints/* data/manifests/*
```

The `.gitkeep` files will remain.

---

## Known pitfalls

### Poetry `ModuleNotFoundError: No module named 'poetry.console'`

On some systems (particularly Windows with Anaconda), `poetry run` fails with this error. Workaround: activate the Poetry virtualenv directly, or run commands through the venv Python:

```bash
# find the venv path
poetry env info --path

# use it directly
/path/to/venv/bin/python -m pytest
```

Alternatively, if Poetry is installed via `pip install poetry` inside a conda environment, it can conflict with the system Python. Reinstalling Poetry via the [official installer](https://python-poetry.org/docs/#installation) resolves this.

### `corepack enable` required before `yarn install`

If `yarn install` fails with "command not found" or a version mismatch, run `corepack enable` first. This is a one-time operation per Node.js installation.

### Lock files are not committed

This repository gitignores `poetry.lock` and `yarn.lock`. This means `poetry install` and `yarn install` resolve the latest compatible versions rather than pinned ones. In rare cases, a new version of a dev dependency may introduce a breaking change. If this happens, pin the version in `pyproject.toml` or `package.json`.

### `*.log` files in data/sample/

The `.gitignore` has a global `*.log` rule. An exception (`!data/sample/*.log`) ensures `data/sample/logs.log` is tracked. If you add other `.log` files to `data/sample/`, they will also be tracked.

### Working directory matters

CLI commands and tests assume you are in the `python/` or `javascript/` directory. Running `poetry run pytest` from the repository root will fail. Always `cd` into the correct project first.

### Output paths are relative

The CLI tools write to paths like `data/raw/api_posts` which are relative to the current working directory. When running from `python/` or `javascript/`, output lands at `python/data/...` or `javascript/data/...` unless you use `../data/...` paths.

---

## CI

GitHub Actions runs on every push and pull request that touches `python/` or `javascript/` files:

- **Python workflow** (`.github/workflows/python.yml`): ruff lint, ruff format check, mypy, pytest
- **JavaScript workflow** (`.github/workflows/javascript.yml`): eslint, node --test

Both workflows use the same commands documented above.
