# Data Platform Lab — JavaScript

JavaScript / Node.js implementation of the data platform lab. This project provides structured modules for practicing data engineering patterns: ingestion, transformation, validation, storage layout, orchestration, observability, streaming, warehouse loading, and CLI tooling.

The project uses ESM modules, the Node.js built-in test runner, and minimal external dependencies. It requires Node.js 22.13+ within the 22.x line, or Node.js 24+, for compatibility with ESLint 10. Node.js 23 is unsupported. CI runs lint, tests, and demos on the minimum supported version, 22.13.0.

## Installation

```bash
cd javascript
yarn install --immutable
```

## Available Scripts

| Script | Command | Description |
| --- | --- | --- |
| `test` | `yarn test` | Run all tests using the Node.js built-in test runner |
| `lint` | `yarn lint` | Lint source and test files with ESLint |
| `lint:fix` | `yarn lint:fix` | Lint and auto-fix where possible |
| `check` | `yarn check` | Syntax-check all source files with `node --check` |

## Dependency security checks

The committed `yarn.lock` pins resolved dependencies and checksums. Normal
installs and CI use `yarn install --immutable`, which fails if installation
would rewrite the lockfile. Dependency updates must explicitly regenerate and
commit it before review.
The generated `.yarn/install-state.gz` is ignored and recreated by installation.

```bash
yarn install --immutable
yarn npm audit --all --recursive
yarn lint
yarn test
```

The [recursive audit](https://yarnpkg.com/cli/npm/audit) includes direct and
transitive packages in both production and development dependencies. CI runs
it without ignored advisories, package exclusions, or deprecation suppression,
and fails on security findings or deprecated versions. Results depend on the
registry's data at check time.

The dependency maintenance update replaces these deprecated versions:

| Package | Previous version | Updated version | Reason |
| --- | --- | --- | --- |
| `eslint` | 9.39.4 | 10.14.0 | Maintained major release; paired with `@eslint/js` 10.0.1 |
| `@smithy/core` | 3.33.3 | 3.39.0 | [Custom logger redaction bug](https://github.com/aws/aws-sdk-js-v3/issues/8393) |
| `@smithy/node-http-handler` | 4.12.0 | 4.14.2 | [HTTP/2 session memory leak](https://github.com/smithy-lang/smithy-typescript/issues/2258) |

The Smithy updates stay within the existing AWS SDK dependency ranges.
ESLint 10 keeps the flat configuration and enables its current recommended
rules. Its [Node.js requirements](https://eslint.org/docs/latest/use/migrate-to-10.0.0#nodejs--v2019-v21-v23-are-no-longer-supported)
are reflected in `package.json` and the setup instructions.

ESLint 10.14.0 and `@eslint/config-array` still request minimatch 10, which is
affected by [GHSA-g3hv-75wg-m249](https://github.com/advisories/GHSA-g3hv-75wg-m249).
Two parent-scoped Yarn resolutions pin their minimatch dependency to 11.0.5.
Remove these overrides when the upstream packages require a fixed version.
The overrides require checking ESLint file discovery and ignore behavior when
updating the lint toolchain.

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

- **Node.js 22.13+ (22.x) or 24+** is required.
- **ESM** (`"type": "module"`) is used throughout.
- **Built-in modules** (`node:fs`, `node:path`, `node:stream`, `node:test`) are preferred over external packages where they suffice.
- **ESLint** enforces code quality with the flat config format (`eslint.config.js`).
- **Tests** live in `tests/` and use `node:test` + `node:assert/strict`.
