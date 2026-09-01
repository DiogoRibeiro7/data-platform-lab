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
    PY --> I[IcebergTableStore]
    PY --> E[EventBroker]
    B --> Local[LocalBlobStore]
    B --> S3[S3BlobStore]
    S3 --> Garage[(Garage S3)]
    R --> PG[PostgresRunStore]
    PG --> DB[(PostgreSQL 18.6)]
    I --> PI[PyIceberg]
    PI --> DB
    PI --> Garage
    E --> K[KafkaEventBroker]
    K --> Kafka[(Apache Kafka 4.3.1)]
```

## Sequence

1. storage and unified-command boundaries — complete;
2. S3-compatible storage and Garage integration — complete;
3. PostgreSQL-backed run/metadata store — complete;
4. Iceberg analytical tables over object storage — complete;
5. event-broker adapter for streaming — in progress;
6. end-to-end platform failure/recovery demo.

Phase 3 reuses the existing observability `RunMetadata` shape. Phase 4 adds a PyIceberg SQL catalog in PostgreSQL while keeping Iceberg table data and metadata files in the S3-compatible Garage warehouse. Phase 5 adds a byte-oriented `EventBroker` contract with a real Apache Kafka adapter; event decoding and event-time semantics remain above the transport boundary.

See [PostgreSQL run metadata](postgres-run-metadata.md) for the operational metadata layer and [Iceberg analytical tables](iceberg-analytical-tables.md) for the analytical-table architecture and local validation flow.
