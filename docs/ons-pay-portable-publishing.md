# Portable SQL/document publishing plans for ONS pay Gold data

The optional `ons-pay-publish-plan` command models a verified, exact ONS pay
Gold vintage in two target representations: flat relational rows and nested
documents. It is a **read-only publishing preview**, not a database writer.
Neither representation requires choosing Supabase, Neon, MongoDB, DynamoDB or
another hosted provider.

The goal is to establish source pins, stable identities, schemas and null
semantics before adding actual adapters. Both representations come from the
**same curated Parquet artifact**, with no automatic `latest` selection.

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
models a versioned document record, without claiming MongoDB or DynamoDB
compatibility until an actual adapter has been tested.

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

Offline fixtures build **real ONS Gold Parquet outputs**, compare both
representations and their stable IDs, check null/suppression preservation,
verify deterministic record fingerprints, and reject corrupted artifacts,
schema drift, duplicate grains, invalid inputs and attempts to exceed the
row budget. Hosted relational and document backends remain future adapters.
