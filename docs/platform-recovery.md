# Platform Failure and Recovery

Milestone 3 ends with one deliberate failure/recovery path across the local platform:

```text
Kafka
  -> recoverable ingestion coordinator
     -> PostgreSQL run metadata
     -> Garage raw object
     -> Iceberg analytical table
  -> Kafka acknowledgement
```

## Recovery identity

Every consumed broker position has a deterministic ingestion identity:

```text
{topic}:{partition}:{offset}
```

The same identity is reused as the PostgreSQL `run_id` and is written into the Iceberg row as `ingestion_id`. The raw payload uses the deterministic object key:

```text
ingestion/{topic}/{partition}/{offset}.json
```

This gives replay a stable cross-system referent without introducing a distributed transaction.

## Commit order

The coordinator processes one message in this order:

1. persist a `running` run snapshot in PostgreSQL;
2. write the original message bytes to Garage under the deterministic object key;
3. validate and transform the sensor event;
4. ensure the Iceberg analytical table exists;
5. scan the Iceberg `ingestion_id` column and append only if this broker position is absent;
6. persist a `success` run snapshot in PostgreSQL;
7. synchronously commit Kafka offset `offset + 1`.

Kafka acknowledgement is intentionally last. A crash before acknowledgement leaves the message eligible for replay.

## Post-Iceberg crash window

The acceptance test deliberately fails after the Iceberg append but before final PostgreSQL success metadata and Kafka acknowledgement.

On replay:

- Kafka returns the same uncommitted position;
- Garage receives the same raw-object key, so the raw payload is replaced idempotently;
- PostgreSQL reuses the same `(pipeline_name, run_id)` row;
- Iceberg is scanned for the deterministic `ingestion_id` and therefore does not receive a duplicate append;
- the run is then marked successful;
- only then is the Kafka position committed.

The expected analytical row count after failure plus replay is exactly one.

## Semantics

This is an **at-least-once broker delivery model with deterministic reconciliation**. It is not a distributed exactly-once transaction across Kafka, PostgreSQL, S3 and Iceberg.

The reconciliation scan is deliberately simple and suitable for this local lab. A production platform with large tables would replace the full-column scan with a more scalable ingestion ledger, merge/upsert strategy, or engine-native transactional design.

## Validation

The `Platform Recovery` GitHub Actions workflow boots all four services, publishes one sensor event, injects the post-Iceberg failure, replays the message, and checks:

- the same Kafka position is replayed;
- no second Iceberg row is appended;
- the raw Garage object exists;
- PostgreSQL ends with `status = success`;
- Kafka is acknowledged only after recovery completes.
