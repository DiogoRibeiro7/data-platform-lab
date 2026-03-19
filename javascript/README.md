# Data Platform Lab — JavaScript

JavaScript / Node.js implementation of the data platform lab. This project provides structured modules for practicing data engineering patterns: ingestion, transformation, validation, storage layout, orchestration, observability, streaming, warehouse loading, and CLI tooling.

The project uses ESM modules, the Node.js built-in test runner, and minimal external dependencies. It targets Node.js 22+ to take advantage of native capabilities like `node:test`, `node:fs/promises`, and stable `ReadableStream` support.

## Installation

```bash
cd javascript
yarn install
```

## Available Scripts

| Script | Command | Description |
| --- | --- | --- |
| `test` | `yarn test` | Run all tests using the Node.js built-in test runner |
| `lint` | `yarn lint` | Lint source and test files with ESLint |
| `lint:fix` | `yarn lint:fix` | Lint and auto-fix where possible |
| `check` | `yarn check` | Syntax-check all source files with `node --check` |

## Testing

Tests use the [Node.js built-in test runner](https://nodejs.org/api/test.html) (`node:test`). No external test framework is required.

Test files live in `tests/` and follow the naming convention `*.test.js`. Each test file imports from `node:test` and `node:assert`:

```javascript
import { describe, it } from "node:test";
import assert from "node:assert/strict";

describe("example", () => {
  it("should pass", () => {
    assert.strictEqual(1 + 1, 2);
  });
});
```

Run tests:

```bash
yarn test
```

## CLI Entry Points

Run the CSV ingestion pipeline against sample data:

```bash
node src/ingestion/cli.js \
  --input-dir ../data/sample \
  --output ../data/bronze/merged.csv \
  --required-columns customer_id,email
```

Run the API ingestion pipeline (fetches from JSONPlaceholder by default):

```bash
node src/ingestion/api-cli.js --max-pages 2
```

Both commands accept `--help` for full option details.

## Module Layout

```text
src/
├── ingestion/          → CSV and API ingestion (exercises 01–02)
├── transform/          → incremental ETL and CDC snapshot diff (exercises 04–05)
├── validation/         → composable data quality rules and runner (exercise 03)
├── orchestration/      → sequential pipeline runner and customer ETL workflow (exercise 06)
├── observability/      → timing, run metadata, and structured reporting (exercise 07)
├── demo.js             → end-to-end e-commerce demo
└── analytics.js        → SQLite analytical queries over curated output
```

Each implemented module directory contains an `index.js` entry point that re-exports its public API. Additional module directories (`cli/`, `storage/`, `streaming/`, `warehouse/`, `utils/`) exist as placeholders for future exercises.

## Implemented Modules

| Module | What it contains |
| --- | --- |
| `ingestion` | CSV pipeline (read, validate, standardize, deduplicate, write) and API pipeline (paginated fetch, retry, transform, save raw + processed). |
| `transform` | Incremental ETL with checkpoint-based deduplication and CDC snapshot comparison (inserts, updates, deletes). |
| `validation` | Six composable check functions and a runner that aggregates results into a report with status logic. |
| `orchestration` | Sequential pipeline runner with retry, skip, shared context, and timing. Plus a customer ETL workflow wired to real modules. |
| `observability` | Timer, RunTracker (with counters, warnings, extras), run metadata formatting. |

## Conventions

- **Node.js 22+** is required.
- **ESM** (`"type": "module"`) is used throughout.
- **Built-in modules** (`node:fs`, `node:path`, `node:stream`, `node:test`) are preferred over external packages where they suffice.
- **ESLint** enforces code quality with the flat config format (`eslint.config.js`).
- **Tests** live in `tests/` and use `node:test` + `node:assert/strict`.
