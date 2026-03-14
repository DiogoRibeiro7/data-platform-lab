# Data Platform Lab — Python

Python implementation of the data platform lab modules.

## Setup

```bash
poetry install
```

## Project Layout

```text
python/
├── pyproject.toml
├── src/
│   └── data_platform_lab/
│       ├── ingestion/       # data source connectors and readers
│       ├── transform/       # cleaning, reshaping, enrichment
│       ├── validation/      # schema enforcement and quality checks
│       ├── storage/         # writers, partitioning, file formats
│       ├── orchestration/   # pipeline scheduling and coordination
│       ├── observability/   # logging, metrics, lineage
│       ├── streaming/       # event-driven / real-time processing
│       ├── warehouse/       # analytical query patterns
│       ├── utils/           # shared helpers
│       └── cli/             # command-line interface
└── tests/
```

## Running Tests

```bash
pytest
```

## Modules

Each sub-package under `src/data_platform_lab/` corresponds to a learning module. Modules are designed to be worked through independently, though later modules build on concepts from earlier ones.
