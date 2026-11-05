# Portable SQL/document publishing for ONS pay Gold data

The optional `ons-pay-publish-plan` command models a verified, exact ONS pay
Gold vintage in two target representations: flat relational rows and nested
documents. It is a **read-only publishing preview**, not a database writer.
Neither representation requires choosing Supabase, Neon, MongoDB, DynamoDB or
another hosted provider.

The `ons-pay-publish` operator command uses these plans for explicit PostgreSQL
or MongoDB-compatible publication and independent verification. Both representations
come from the **same curated Parquet artifact**, with source pins, stable identities,
schemas and null semantics preserved, and no automatic `latest` selection.

## Prepare a pinned vintage

First [ingest](registry-handoff-ingestion.md) and
[curate](ons-pay-analytics.md) an ONS pay vintage. Use the complete 40-character
registry commit and 64-character source SHA-256 from its successful curation
receipt. Then, from `python/`:

```bash
poetry install --extras serving

# Flat, typed JSON rows for a future relational SQL table.
poetry run data-platform-lab ons-pay-publish-plan \
  --data-root ../data \
  --registry-commit COMMIT_SHA40 \
  --source-sha256 SOURCE_SHA256 \
  --table geography_year --shape relational

# Nested, stable-key documents for a future document database.
poetry run data-platform-lab ons-pay-publish-plan \
  --data-root ../data \
  --registry-commit COMMIT_SHA40 \
  --source-sha256 SOURCE_SHA256 \
  --table local_authority_yoy --shape document
```

The two supported Gold tables are:

| Table | Natural business grain | Notes |
| --- | --- | --- |
| `geography_year` | `(geography, pay_year)` | Unweighted distribution across areas in a geography type/year; **not** a population-weighted national estimate |
| `local_authority_yoy` | `(area_code, reference_date)` | Local authority annual pay and only strictly adjacent year-on-year comparisons; gaps remain null |

Every projected record has a stable SHA-256 identifier computed from schema
version, dataset, table, **full vintage** and natural business grain. The
same source observation has the **same record ID** in both representations;
two distinct vintages get distinct IDs, even if their business keys match.
Duplicate or null business keys are rejected.

## Operator publishing CLI

`data-platform-lab ons-pay-publish` provides a summary-only operator workflow
over the same planner, publishing adapters, reconciliation reports and acceptance
gate. All invocations require explicit `--model-version 1`, `--registry-commit`,
`--source-sha256`, `--table` and `--sink` (`postgresql` or `mongodb`). The two
operation flags `--apply` and `--verify` are mutually exclusive; omitting both
previews the plan. Abbreviated flags are rejected.

From `python/`, prepare your source pins and choose one operation:

```bash
poetry install --extras serving

# Summary preview: no sink connection, credentials, driver import or file writes.
poetry run data-platform-lab ons-pay-publish \
  --data-root ../data --model-version 1 \
  --registry-commit "$ONS_REGISTRY_COMMIT" --source-sha256 "$ONS_SOURCE_SHA256" \
  --table local_authority_yoy --sink postgresql

# Supply ONS_PAY_POSTGRES_DSN through your environment before these commands.
# Publish, commit, read the full vintage back, and require the acceptance gate.
poetry run data-platform-lab ons-pay-publish \
  --data-root ../data --model-version 1 \
  --registry-commit "$ONS_REGISTRY_COMMIT" --source-sha256 "$ONS_SOURCE_SHA256" \
  --table local_authority_yoy --sink postgresql --apply

# Independent read-only check: never publishes, creates tables or repairs drift.
poetry run data-platform-lab ons-pay-publish \
  --data-root ../data --model-version 1 \
  --registry-commit "$ONS_REGISTRY_COMMIT" --source-sha256 "$ONS_SOURCE_SHA256" \
  --table local_authority_yoy --sink postgresql --verify
```

Preview uses the existing in-memory DuckDB/Parquet validation; it opens no
PostgreSQL or MongoDB connection and does not access connection settings.
`--max-rows` retains the planner's 1–100000 source-row budget. For a
record-by-record source preview, the existing `ons-pay-publish-plan` command
is unchanged.

### Connection settings and ownership

| Sink | Required environment | Fixed destination |
| --- | --- | --- |
| `postgresql` | `ONS_PAY_POSTGRES_DSN` | `ons_pay_geography_year` or `ons_pay_local_authority_yoy` |
| `mongodb` | `ONS_PAY_MONGODB_URI`, `ONS_PAY_MONGODB_DATABASE` | Same fixed collection names, in the explicitly configured database |

There are no default credentials or connection-string CLI flags. Database
clients are loaded only for an apply/verify operation. Psycopg is supplied by
the existing project dependencies. PyMongo remains optional; install it in
your environment before selecting MongoDB. For example, in a disposable
local environment:

```bash
poetry run python -m pip install 'pymongo>=4.10,<5'

# Set ONS_PAY_MONGODB_URI and ONS_PAY_MONGODB_DATABASE in your environment.
poetry run data-platform-lab ons-pay-publish \
  --data-root ../data --model-version 1 \
  --registry-commit "$ONS_REGISTRY_COMMIT" --source-sha256 "$ONS_SOURCE_SHA256" \
  --table geography_year --sink mongodb --apply

# Use the same pins, table and sink with --verify for a separate read-only check.
```

The CLI closes the connections it creates on success or failure. PostgreSQL
uses `autocommit=False`, and verification sets the owned connection to read-only.
Psycopg's connect timeout is 10 seconds; MongoDB selection/connect timeouts
are 10 seconds and its socket timeout is 30 seconds. These are not whole-workflow
deadlines. No indexes, background retry loops or automatic repairs are added.
MongoDB read concern and PostgreSQL query/lock timeouts can be configured
through the connection settings as appropriate for the deployment.

Python callers and offline tests can call `publish_cli.main(argv, environ=...)`
with an injected settings mapping, or pass `postgres_connection=...` or
`mongo_collection=...`. Supplied handles bypass credential loading and remain
caller-owned, including transaction cleanup. Apply requires a handle supporting
both existing writer and reader protocols; verify only requires the reader.

### Revalidation, JSON and exit codes

After connecting and immediately before sink I/O, the CLI rebuilds the plan
from the local curated receipt and all verified Silver/Gold artifacts. It
rejects any difference from the initial plan. The adapter then revalidates and
snapshots the in-memory records before writing. Local files are assumed to
have a trusted single writer; this is not a filesystem lock or authentication.

Except for `--help`, the command prints one JSON object to stdout on both
success and expected failure. Errors do not echo raw arguments, driver
exceptions, source/sink values, connection settings or record identifiers.

| JSON field | Meaning |
| --- | --- |
| `schema_version` | Operator result schema, currently `1` |
| `operation`, `sink`, `table`, `vintage` | Selected operation and validated source pins; unavailable information is `null` |
| `status` | `previewed`, `accepted`, `verified`, `rejected`, or `failed` |
| `counts` | Source, successfully applied and observed row counts; unknown/not applicable is `null` |
| `fingerprints` | Source artifact, source plan, expected sink and observed sink SHA-256; unobserved hashes are `null` |
| `reconciliation` | Pass/fail and counts of missing/unexpected records, changed records and changed fields; no record-level detail |
| `acceptance` | Existing gate's evidence only for a successful apply; otherwise `null` |
| `error` | Stable error `code` and sanitized message; `null` on success |

| Exit | Meaning | Error codes |
| --- | --- | --- |
| `0` | Preview completed, publication accepted, or verification passed | None |
| `2` | Invalid syntax, source pins/files/limits, missing settings or dependency | `invalid_arguments`, `source_validation_failed`, `configuration_error` |
| `3` | Drift, unsafe/malformed sink data, or rejected acceptance evidence | `reconciliation_failed`, `acceptance_rejected` |
| `4` | Connection, database write/read, or owned-client cleanup failed | `database_error` |

A successful `--verify` has status `verified` and `acceptance: null`: it has
no write receipt and cannot fabricate publication acceptance. The command
does not persist evidence or activate a release. To keep its JSON, explicitly
redirect stdout; durable evidence is a separate workflow.

An error does not mean the database is unchanged. PostgreSQL commits before
read-back; MongoDB replacements are individually atomic but the batch is not.
If a write fails partway, `counts.applied` remains `null` because no complete
receipt exists. A read or close failure may occur after all writes completed.
Inspect the target and source first, then explicitly repeat `--apply` with the
same exact pins to replay idempotently. Unexpected extra records remain a
reconciliation failure: replay does not delete them. Concurrent sink writers
can invalidate a checked snapshot; configure isolation/read concern and access
control separately.

Run the offline CLI tests and the existing opt-in integration entry point:

```bash
poetry run pytest -q tests/test_ons_pay_publish_cli.py
# Start a disposable local PostgreSQL service and set ONS_PAY_POSTGRES_DSN.
ONS_PAY_LIVE_POSTGRES=1 poetry run pytest -q tests/test_ons_pay_publish_postgres_live.py
```

The PostgreSQL CI workflow exercises both Gold tables through real CLI-owned
connections, including replay, read-only verification, controlled drift and
explicit recovery. MongoDB CLI coverage currently uses injected collections;
live MongoDB recovery testing is a separate follow-up.

## Output contract

Each invocation prints one JSON object with these fields:

| Field | Meaning |
| --- | --- |
| `schema_version` | Version of this publishing preview (`1`) |
| `dataset_id` / `table` / `shape` | Explicit source model and target representation |
| `vintage` | Exact `model_version`, `registry_commit`, `source_sha256` |
| `source_artifact_sha256` | Verified SHA-256 of the chosen Gold Parquet artifact |
| `source_rows` | Receipt row count, checked against the full projected batch |
| `records_sha256` | SHA-256 of canonical, ordered projected records; differs between shapes |
| `records` | Deterministically ordered flat rows or nested documents |

The `relational` shape adds `record_id` and `model_version` to the original
Gold fields, while preserving source commit/hash columns. It models the row
and natural keys suitable for a future SQL upsert, without issuing SQL.

The `document` shape uses `_id`, `schema_version`, `dataset_id`, `table`, an
explicit `vintage` object and the original Gold fields under `data`. It
models the versioned document record consumed by the MongoDB-compatible adapter.
Other document stores require separately implemented and tested adapters.

Both shapes preserve integers and finite floats as JSON numbers, missing
metrics and suppressed pay as **JSON null**, and dates as ISO `YYYY-MM-DD`
strings. In particular, missing pay is never replaced with zero; neither is
a missing year-on-year percentage interpolated. Record order is stable by
each table's exact business grain, not the physical order in Parquet.

## Integrity and limits

- `CuratedCatalog` first discovers an exact successful receipt and verifies
  **all three Silver/Gold artifacts** against their hashes, column schema,
  row counts, lineage pins and Silver missing-pay totals.
- The selected Gold table is read using a parameterized Parquet path and a
  fixed, internal ordering expression; CLI strings never become SQL names.
  Every row is rechecked against the v1 field types, non-nullable keys and
  expected provenance; geography-year reported/missing area counts must
  reconcile with their total.
- Each projected ID must be unique. The selected artifact is rehashed after
  reading to detect a concurrent change; all planned row counts must match
  the signed-off local curation receipt. Local receipts are integrity records,
  **not cryptographic signatures or proof of trusted external provenance**.
- `--max-rows` bounds the in-memory preview (1–100000, default 100000), and
  an over-budget artifact fails before the data batch is fetched. This is
  deliberately not a streaming bulk-load tool.
- Output goes to stdout. No external connections, credentials, schema DDL,
  database writes, notifications, scheduling, release-pointer changes or
  object-store mutations are performed. Operators can redirect JSON to a
  file explicitly, but doing so is outside the command's side effects.

Exit status is `0` for a verified, bounded plan, or `2` for invalid pins,
table/shape, row limits, corrupted receipts/artifacts, duplicate keys,
unexpected scalar types or an uncurated vintage. There is no fallback to
an unpinned or active version when a pin is invalid.

## Tests

```bash
cd python
poetry run pytest tests/test_ons_pay_publish_plan.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

## Read-after-write sink reconciliation

The provider-independent module
`data_platform_lab.serving.publish_reconcile` adds **read-only checks**
that verify an entire exact-vintage population after optional publication.
It does not rely on the publisher's success receipt as evidence of stored
content. Each function accepts an existing, caller-owned database handle;
no connection, credential, table, index or external write is created.

```python
from data_platform_lab.serving.publish_reconcile import (
    reconcile_mongodb,
    reconcile_postgres,
)

# After explicitly publishing a relational plan using publish_postgres:
sql_report = reconcile_postgres(relational_plan, open_postgres_connection)
if not sql_report.passed:
    raise RuntimeError(sql_report.to_dict())

# Or, after explicitly publishing a document plan using publish_mongodb:
mongo_report = reconcile_mongodb(document_plan, open_mongo_collection)
if not mongo_report.passed:
    raise RuntimeError(mongo_report.to_dict())
```

These names represent **already-created** plans and open connections; the
examples intentionally do not initiate a second publication or contain
credentials. `reconcile_postgres` reads a fixed, parameterized SELECT on
`model_version`, `registry_commit` and `source_sha256`, against one
whitelisted table. `reconcile_mongodb` filters the three corresponding
`vintage.*` fields and uses a bounded cursor. Neither checks just the
expected IDs: both inspect the complete selected vintage, allowing an extra
row or document to be detected.

The `ReconciliationReport` fields are:

| Field | Interpretation |
| --- | --- |
| `sink`, `table`, `vintage` | Target type and full source pins |
| `passed` | True only if counts, identities, fields and sink fingerprints all agree |
| `expected_count` / `observed_count` | Exact row/document counts for the selected vintage |
| `source_plan_sha256` | Original, source-grain-ordered `PublishPlan.records_sha256` |
| `expected_sink_sha256` / `observed_sink_sha256` | Canonical record-ID-ordered hashes using sink-normalized types |
| `missing_ids` / `unexpected_ids` | Stable record IDs present on only one side |
| `changed_fields` | Per-ID field paths that differ, without source or sink values |

For SQL, PostgreSQL `DATE` values normalize to ISO dates; permitted
`DOUBLE PRECISION` storage normalizes safely representable integer-valued
Gold measures to floats. Other type differences are reported as drift.
MongoDB documents retain the strict source JSON scalar types and nesting.
The sink hashes are calculated in **record-ID order**, independent of
physical row order, and may differ from the source-plan hash even when
the data match. Comparisons are exact; suppressed or missing measurements
remain `null`, never zero.

The contract fails closed on invalid source plans, unreadable sources,
unsafe database values, invalid/duplicate stored IDs, malformed SQL rows
and over-budget vintages (more than 100000 observed rows). A read error
raises `ReconciliationError`; ordinary missing, extra or changed records
return a structured `passed=False` report.

### Consistency and operational boundaries

- A mismatched lineage field causes the record to disappear from the exact
  vintage filter and therefore appear as a **missing ID**. Other vintages
  are deliberately outside this comparison.
- PostgreSQL executes a single bounded SELECT; the caller should use a
  consistent snapshot if concurrent writers may change the table.
  MongoDB uses a bounded cursor; strict collection-wide snapshot
  consistency likewise requires an externally configured read concern or
  session. The adapter issues no transaction-control commands; an SQL driver
  may implicitly open a read transaction, whose lifecycle the caller owns.
- For large sinks, indexes on the full vintage pins would avoid scans.
  Index creation and schema permissions are intentionally operator-owned.
- Results are a validation report, **not** a durable audit ledger, a
  digital signature, or evidence that an untrusted external data publisher
  is authentic. They do not authorize a follow-up write or automated repair.

```bash
cd python
poetry run pytest tests/test_ons_pay_publish_reconcile.py -q
poetry run ruff check src/ tests/
poetry run mypy src/
```

The real PostgreSQL integration workflow additionally reconciles both
Gold tables after publication, intentionally modifies one stored value
in an uncommitted test transaction, verifies that drift is detected, then
rolls it back and verifies recovery. MongoDB behavior is tested against
an injected fake collection; no hosted document-store account is required.

### Explicit publication acceptance gate

`data_platform_lab.serving.publish_gate` provides an **opt-in, fail-closed
acceptance boundary** after a publisher returns a `PublishReceipt` and a
read-back produces a `ReconciliationReport`. A successful write receipt alone,
or even an unrelated passing reconciliation, is not sufficient. Acceptance
requires all of these to agree:

1. The full source plan is still valid for the receipt's sink shape.
2. Sink, table, model version, registry commit and source SHA-256 match across
   the plan, receipt and read-back report.
3. The applied, expected and observed counts are all the complete plan count;
   the receipt's canonical plan hash and the report's source hash match it.
4. The read-back passed with no missing, unexpected or changed fields. Both
   sink hashes must match an **independently recomputed expected sink hash**
   using PostgreSQL's normalized SQL values or MongoDB's document semantics.

For explicit, separate steps:

```python
from data_platform_lab.serving.publish_gate import accept_publication
from data_platform_lab.serving.publish_reconcile import reconcile_postgres
from data_platform_lab.serving.publish_sinks import publish_postgres

# plan is the existing verified relational PublishPlan.
# connection is caller-owned and already connected (autocommit=False).
receipt = publish_postgres(plan, connection)
report = reconcile_postgres(plan, connection)
accepted = accept_publication(plan, receipt, report)
print(accepted.to_dict())
```

Or use a convenience wrapper that writes, reads back and returns **only** an
`AcceptedPublication` on success:

```python
from data_platform_lab.serving.publish_gate import (
    publish_and_accept_mongodb,
    publish_and_accept_postgres,
)

# Both handles are injected; a single psycopg connection can serve both roles.
accepted_sql = publish_and_accept_postgres(
    relational_plan, writer=postgres_connection, reader=postgres_connection
)

# One PyMongo collection can also serve as both writer and reader.
accepted_docs = publish_and_accept_mongodb(
    document_plan, writer=mongo_collection, reader=mongo_collection
)
```

The acceptance evidence has status `accepted`, sink/table, complete vintage,
accepted count, source-plan fingerprint and verified sink snapshot fingerprint.
No `rejected` record is emitted by the gate: invalid evidence raises
`PublicationAcceptanceError`; a read failure raises `ReconciliationError`.
Neither path silently repairs drift or switches to an unpinned vintage.

**Important:** PostgreSQL publishing commits *before* the read-back; MongoDB
replacements can already be partially visible. A failed gate does **not**
undo those writes or imply atomic visibility. No active-release pointer,
audit ledger, external notification or downstream publication is advanced.
The caller controls snapshot isolation and trusted database handles. Evidence
is not a digital signature and only establishes agreement at the checked
snapshot; concurrent changes can invalidate it afterward.

```bash
cd python
poetry run pytest tests/test_ons_pay_publish_gate.py -q
poetry run pytest tests/test_ons_pay_publish_postgres_live.py -q
```

Offline fixtures build **real ONS Gold Parquet outputs**, compare both
representations and their stable IDs, check null/suppression preservation,
verify deterministic record fingerprints, and reject corrupted artifacts,
schema drift, duplicate grains, invalid inputs and attempts to exceed the
row budget.

## Explicit opt-in publishing adapters

The Python module
`data_platform_lab.serving.publish_sinks` can **apply** an existing plan
to either PostgreSQL (including providers based on PostgreSQL) or a
MongoDB-compatible collection. The operator CLI above uses these adapters
only with `--apply`. Importing the module opens no connections; writes occur only when
the caller invokes `publish_postgres` or `publish_mongodb` with an
already-open, caller-owned connection/collection.

Both adapters revalidate the complete in-memory plan **before any writes**:
schema version and dataset identity, exact source and registry pins, shape
and Gold column contracts, typed nullable metrics, ISO dates, IDs based on
full vintage and natural grain, duplicate IDs, row counts, and the canonical
record fingerprint. They deep-copy the verified records before I/O, so a
caller mutating the preview during a write does not change the applied batch.
The plan fingerprint detects local alteration; it is **not a digital
signature**. The read-only planning step must still verify the actual local
curation receipt and Parquet inputs.

### PostgreSQL

`postgres_schema_sql(table)` emits the fixed v1 DDL for
`ons_pay_geography_year` and `ons_pay_local_authority_yoy`. Each table has
a primary key on the deterministic `record_id`, explicit SQL types, and
nullability matching the Gold contract. The geography table additionally
checks that area counts reconcile. `postgres_upsert_sql(table)` emits
parameterized `%s` placeholders and an `ON CONFLICT (record_id)` upsert.
No caller-controlled SQL identifier is interpolated.

```python
import os
from pathlib import Path

import psycopg

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing
from data_platform_lab.serving.publish_sinks import publish_postgres

vintage = VintageKey(
    model_version=1,
    registry_commit=os.environ["ONS_REGISTRY_COMMIT"],
    source_sha256=os.environ["ONS_SOURCE_SHA256"],
)
plan = plan_ons_pay_publishing(
    data_root=Path("../data"),
    vintage=vintage,
    table="local_authority_yoy",
    shape="relational",
)
# The operator explicitly opens the connection; the adapter performs the write.
with psycopg.connect(os.environ["DATABASE_URL"], autocommit=False) as connection:
    receipt = publish_postgres(plan, connection, batch_size=500)
```

The connection must have `autocommit=False`. The adapter creates the fixed
table if absent, upserts in bounded batches, and commits once. On a statement or commit error
it attempts rollback and raises `PublishSinkError`, never a success receipt.
It does not close a caller-owned connection. Reapplying the exact same plan
is idempotent; distinct pinned vintages receive distinct IDs. A live local
PostgreSQL smoke test is opt-in and runs in the PostgreSQL integration CI job.

### MongoDB-compatible documents

```python
import os
from pathlib import Path

from pymongo import MongoClient

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing
from data_platform_lab.serving.publish_sinks import publish_mongodb

plan = plan_ons_pay_publishing(
    data_root=Path("../data"),
    vintage=VintageKey(
        model_version=1,
        registry_commit=os.environ["ONS_REGISTRY_COMMIT"],
        source_sha256=os.environ["ONS_SOURCE_SHA256"],
    ),
    table="local_authority_yoy",
    shape="document",
)
# PyMongo is an optional driver installed by the calling application.
with MongoClient(os.environ["MONGODB_URI"]) as client:
    receipt = publish_mongodb(plan, client["data_platform_lab"]["ons_pay_local_authority_yoy"])
```

The adapter calls `replace_one({"_id": stable_id}, document, upsert=True)`
and requires an acknowledged result for every record. It never performs a
delete or changes unrelated vintages. **The batch is not atomic** without
an externally managed MongoDB transaction: if a later replacement fails,
earlier records can already be visible. The error explicitly instructs
retrying the exact same pinned plan. Stable IDs and whole-document
replacement make a replay idempotent, including after partial completion.
Atomic batch visibility, run-ledger tracking, and tenant authorization
remain future production-hardening work; this is a controlled DE exercise.

PyMongo is **not a dependency** of the core project: install it in the
calling application's environment if choosing that backend. No AWS,
Supabase, Neon, Atlas or other hosted accounts are needed for offline
tests. A MongoDB-compatible live integration is not yet part of CI.

### Safety and verification

- The operation is restricted to the two fixed ONS Gold table contracts;
  arbitrary SQL/table/collection names are not accepted.
- PostgreSQL uses one transaction and parameterized upserts; document
  replacement uses acknowledged, stable-key operations with replay semantics.
- Neither adapter selects `latest`, trusts a mismatched shape, truncates a
  table/collection, deletes older vintages, creates external connections,
  or silently switches to another destination.
- A successful `PublishReceipt` reports the requested sink, table, exact
  vintage, attempted record count and content hash. It is not a substitute
  for read-after-write verification or an audit ledger.

```bash
cd python
poetry run pytest tests/test_ons_pay_publish_sinks.py -q
poetry run ruff check src/ tests/
poetry run mypy src/
```
