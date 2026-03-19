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
├── ingestion/          → read from files, APIs, archives, external sources
├── transform/          → clean, reshape, enrich, aggregate datasets
├── validation/         → enforce schemas and data quality checks
├── storage/            → write to layered storage with format and partition control
├── orchestration/      → schedule and coordinate multi-step pipelines
├── observability/      → structured logging, metrics, lineage tracking
├── streaming/          → event-driven and near-real-time processing patterns
├── warehouse/          → load into analytical stores, run warehouse-style queries
├── utils/              → shared helpers: paths, I/O, config, constants
└── cli/                → command-line entry points for pipelines and utilities
```

Each module directory contains an `index.js` entry point. Exercises and implementations are added as additional files within each module.

## Module Responsibilities

| Module | Responsibility |
| --- | --- |
| `ingestion` | Read data from flat files (CSV, JSON), HTTP APIs with pagination and retries, compressed archives, and structured log files. |
| `transform` | Apply column mapping, type coercion, deduplication, filtering, derived fields, and medallion-layer promotion logic. |
| `validation` | Define schemas, enforce contracts at pipeline boundaries, detect anomalies, and route invalid records to dead-letter storage. |
| `storage` | Write to raw/bronze/silver/gold layers, select file formats (CSV, JSON), apply partitioning, and track manifests. |
| `orchestration` | Resolve task dependencies, execute DAG-based pipelines, manage retries and checkpoints, and load pipeline definitions from configuration. |
| `observability` | Emit structured logs, record execution timing and row counts, track data lineage, and monitor pipeline health. |
| `streaming` | Simulate event streams, apply windowed aggregation, bridge streaming to batch, and implement producer/consumer patterns using Node.js streams. |
| `warehouse` | Load silver/gold data into SQLite, write analytical queries, and produce consumption-layer datasets. |
| `utils` | Resolve paths, handle file I/O, format timestamps, load configuration, and define shared constants. |
| `cli` | Expose entry points for running pipelines, inspecting data, and executing developer utilities from the command line. |

## Conventions

- **Node.js 22+** is required.
- **ESM** (`"type": "module"`) is used throughout.
- **Built-in modules** (`node:fs`, `node:path`, `node:stream`, `node:test`) are preferred over external packages where they suffice.
- **ESLint** enforces code quality with the flat config format (`eslint.config.js`).
- **Tests** live in `tests/` and use `node:test` + `node:assert/strict`.
