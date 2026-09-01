# Milestone 3 — Local data platform

Status: in progress

## Architectural rule

```text
workflow logic -> platform contracts -> local or infrastructure adapters
```

## Current platform architecture

```mermaid
flowchart LR
    PY[Python workflows] --> B[BlobStore]
    JS[JavaScript workflows] --> B
    PY --> R[RunStore]
    JS --> R
    B --> Local[LocalBlobStore]
    B --> S3[S3BlobStore]
    S3 --> Garage[(Garage S3)]
    R --> PG[PostgresRunStore]
    PG --> DB[(PostgreSQL 18.6)]
```

## Sequence

1. storage and unified-command boundaries — complete;
2. S3-compatible storage and Garage integration — complete;
3. PostgreSQL-backed run/metadata store — in progress;
4. Iceberg analytical tables over object storage;
5. event-broker adapter for streaming;
6. end-to-end platform failure/recovery demo.

Phase 3 reuses the existing observability `RunMetadata` shape. The initial database contains one deliberately narrow `pipeline_runs` table, keyed by `(pipeline_name, run_id)` and updated idempotently as a run moves from running to success/failed.

See [PostgreSQL run metadata](postgres-run-metadata.md) for the ER diagram, persistence contract, and local validation flow.
