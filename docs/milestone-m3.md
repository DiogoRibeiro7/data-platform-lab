# Milestone 3 — Platform boundaries

Status: in progress

## Purpose

Milestone 2 proved the core data-engineering patterns with dependency-light,
local implementations. Milestone 3 turns those exercises into a small local
platform without discarding the educational implementations that make the
repository useful.

The architectural rule for M3 is:

```text
workflow logic -> platform contracts -> local or infrastructure adapters
```

Vendor clients must not become the application architecture.

## Phase 1 — storage and command boundaries

The first M3 slice introduces two boundaries that were previously placeholders.

### Unified command surface

Python and JavaScript now expose one top-level command:

```text
data-platform-lab benchmark ...
data-platform-lab stream ...
data-platform-lab warehouse ...
```

The root command delegates to the existing workflow CLIs. It does not copy
their argument declarations, so configuration parsing remains owned by each
workflow until a later configuration-contract refactor is justified.

### Object-storage contract

Both runtimes now implement the same minimal local object-store semantics:

- put bytes by portable relative key;
- get bytes by key;
- test object existence;
- list objects by prefix in deterministic order;
- reject absolute paths and parent traversal;
- replace complete objects atomically on the local filesystem.

Python formalises the boundary as a `BlobStore` protocol and provides
`LocalBlobStore`. JavaScript provides the equivalent `LocalBlobStore` adapter.
The local implementation is the development and test adapter, not the final
production storage technology.

## Why infrastructure is not added in this phase

Adding PostgreSQL, an S3-compatible object store, Iceberg, and an event broker
before defining application boundaries would make pipeline modules depend
straight on vendor SDKs. That would create a collection of integrations rather
than a platform.

M3 therefore proceeds in this order:

1. stable storage and command boundaries;
2. S3-compatible object-store adapter and Docker Compose service;
3. relational run/metadata store backed by PostgreSQL;
4. Iceberg analytical tables over object storage;
5. event-broker adapter for the streaming exercise;
6. end-to-end local platform demo and failure/recovery tests.

## Non-goals for Phase 1

- replacing the existing medallion filesystem examples;
- changing the statistical or business semantics of any workflow;
- introducing distributed execution;
- treating local filesystem storage as production object storage;
- duplicating child CLI configuration at the root command.

## Acceptance criteria

Phase 1 is complete when both runtimes have:

- a tested root CLI with benchmark, stream, and warehouse dispatch;
- a tested local blob-store adapter;
- traversal-safe object keys;
- deterministic prefix listing;
- complete-object replacement semantics;
- package-level command registration.
