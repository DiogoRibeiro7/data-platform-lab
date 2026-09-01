# PostgreSQL run metadata

Milestone 3 Phase 3 makes pipeline run history durable without changing the existing `RunMetadata` model.

## Data model

```mermaid
erDiagram
    PIPELINE_RUNS {
        text pipeline_name PK
        text run_id PK
        text status
        timestamptz started_at
        timestamptz ended_at
        double duration_seconds
        bigint rows_read
        bigint rows_written
        bigint rows_rejected
        bigint files_processed
        bigint files_rejected
        jsonb warnings
        jsonb errors
        jsonb extra
        timestamptz recorded_at
        timestamptz updated_at
    }
```

The compound key `(pipeline_name, run_id)` makes snapshots idempotent: a running record can be updated when the same run finishes.

## Platform topology

```mermaid
flowchart LR
    PY[Python workflows] --> RM[RunMetadata]
    JS[JavaScript workflows] --> RM
    RM --> RS[RunStore contract]
    RS --> PG[PostgresRunStore]
    PG --> DB[(PostgreSQL 18.6)]
    DB --> History[Durable run history]
    Workflows[Existing manifests] -. complementary .-> RM
```

Python provides the live PostgreSQL factory through psycopg and the wire-level CI smoke test. JavaScript implements the same SQL/run-store semantics against an injected `query()` client; a locked JavaScript PostgreSQL driver factory is deliberately deferred rather than weakening Yarn reproducibility.

## Local use

```bash
docker compose up -d --wait postgres
cd python
poetry install
poetry run data-platform-lab metadata
```

Default local DSN:

```text
postgresql://data_platform_lab:data_platform_lab@localhost:5432/data_platform_lab
```

The credentials are for the disposable local Compose stack only.
