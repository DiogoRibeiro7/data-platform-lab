# Data Platform Lab — JavaScript

JavaScript implementation of the data platform lab modules.

## Setup

```bash
yarn install
```

## Project Layout

```text
javascript/
├── package.json
├── src/
│   ├── ingestion/       # data source connectors and readers
│   ├── transform/       # cleaning, reshaping, enrichment
│   ├── validation/      # schema enforcement and quality checks
│   ├── storage/         # writers, partitioning, file formats
│   ├── orchestration/   # pipeline scheduling and coordination
│   ├── observability/   # logging, metrics, lineage
│   ├── streaming/       # event-driven / real-time processing
│   ├── warehouse/       # analytical query patterns
│   ├── utils/           # shared helpers
│   └── cli/             # command-line interface
└── tests/
```

## Running Tests

```bash
yarn test
```

## Modules

Each directory under `src/` corresponds to a learning module. Modules are designed to be worked through independently, though later modules build on concepts from earlier ones.
