# ONS pay refresh, historical backfill and freshness reporting

This workflow connects the existing [SHA-pinned canonical ingestion](registry-handoff-ingestion.md),
[DuckDB Silver/Gold curation](ons-pay-analytics.md), and
[read-only analytical API](ons-pay-serving.md) behind one repeatable command.
No Supabase, cloud bucket, database supplier, or network fetch is required.

The refresh process runs **explicitly pinned handoffs**, not a floating
`main` branch or an implicit latest ONS download. Each handoff is paired with
a checkout of [`DiogoRibeiro7/data`](https://github.com/DiogoRibeiro7/data)
at the *exact full commit* recorded in the handoff. Prepare the handoff using
the [registry guide](registry-handoff-ingestion.md) first.

## One snapshot: validate, ingest, curate and audit

```bash
cd python
poetry install --extras serving

# Read-only manifest and freshness preflight (no directories or files created).
poetry run data-platform-lab ons-pay-refresh \
  --source ../data/manifests/ons-handoff.json /path/to/pinned/data-checkout \
  --max-source-age-days 365 \
  --dry-run

# Execute the same full, verified pipeline.
poetry run data-platform-lab ons-pay-refresh \
  --source ../data/manifests/ons-handoff.json /path/to/pinned/data-checkout \
  --object-root ../data/object-store \
  --output-root ../data \
  --max-source-age-days 365
```

The CLI prints a JSON summary of published/reused objects and Parquet files,
and the absolute `receipt_path`. Every completed execution creates a distinct
atomic audit under `data/manifests/ons_pay_refresh_<run-id>.json`. The underlying
ingestion and curation each keep their own independent execution receipts.
The data remains under the existing immutable, model-version/commit/SHA paths.

Run the command a second time with the same handoff. It verifies all source,
object, and curated output hashes again; matching object and Parquet bytes are
**reused without changing their timestamps**. A second audit records the
replay. The serving catalog still presents only one logical vintage.

## Historical backfill

Provide one `--source HANDOFF_JSON PINNED_CHECKOUT` pair per historical
snapshot, in the order you want the pipeline to process them:

```bash
poetry run data-platform-lab ons-pay-refresh \
  --source /backfill/2024/handoff.json /checkouts/data-2024 \
  --source /backfill/2025/handoff.json /checkouts/data-2025 \
  --object-root ../data/object-store \
  --output-root ../data
```

Those checkouts must already exist at their respective exact commits.
No source file, handoff, or receipt is committed to this repository. The
batch rejects a repeated commit/source-SHA pair, invalid ONS source identity,
or malformed manifests before ingesting anything. The later ingestion step
additionally checks each real Git blob, its checksum, storage key and output.

A failed second source does **not** roll back a successfully published first
source: the objects are immutable, and there is no cross-store transaction.
Instead, the batch writes a `status: failed` receipt with the completed
vintages, metrics, and failing `phase` (`preflight`, `ingestion`, `curation`)
and zero-based `source_index`. If ingestion had succeeded but curation failed,
the failure includes its successful ingestion receipt path. Fix the cause and
rerun the full batch; previously valid objects and Parquet outputs are checked
and reused. Corrupted immutable files are never silently overwritten.

## Freshness SLA and quality semantics

The optional `--max-source-age-days` is a **pre-ingestion quality gate**:

- The clock is the UTC calendar date. Age is calculated from the upstream
  handoff's `source.retrieved_at`, an ISO date or timezone-aware timestamp.
- A future retrieval date, malformed date, or source older than the configured
  threshold fails preflight. No source object or Parquet output is published.
- Omit this parameter for intentionally historical backfills. A 2024 snapshot
  does not become invalid merely because it is old when historical work is
  the purpose of the batch.
- This checks **source retrieval age**, not the year of a pay observation and
  not the timing of a new upstream ONS publication. It cannot discover newer
  upstream data on its own.

The audit records `policy.as_of_utc_date` alongside the threshold, so the
freshness decision can be reproduced and interpreted after execution.

The underlying curation already enforces source schema, area/year grain,
missing-value/suppression consistency, non-negative pay, confidence interval
validity, and exact Silver row conservation before publishing. The audit
records the observed `source_age_days`, `source_rows`, `rows_with_pay`,
`rows_without_pay`, Gold/Silver counts, byte verification and publication vs
reuse counts per vintage. Top-level metrics **sum across vintages**, so a
two-snapshot backfill can count the same area's year twice; these are run
metrics, not an estimate of distinct people or areas.

`--dry-run` checks handoff structure, duplicate identity and the selected
freshness policy only. It deliberately does **not** claim to have validated
Git checkouts, source file bytes, storage integrity, or downstream SQL quality;
those are rechecked during the actual run.

## Scheduling and operating limits

The CLI is deterministic and exits nonzero on failure, so a local scheduler,
CI job, Dagster asset or later managed orchestrator can run it with the
*explicit handoff and checkout paths*. Freshness breaches and partial failures
produce machine-readable audits for monitoring. No unattended schedule or
data-provider polling is configured in this PR, because silently promoting
a floating upstream version would break the explicit pin contract.
Use the [read-only monitoring command](ons-pay-monitoring.md) to check the
published table contracts and refresh audit history, obtain numeric health
metrics, and return alert-ready exit codes for a configured scheduler.

The process is a single-writer local workflow. It does not support
distributed locking, rollback across multiple sources, concurrent publication,
automatic registry-version discovery, or signed attestations against a local
actor who can rewrite both receipts and files. Core orchestration accepts a
`BlobStore` implementation, but the current CLI chooses `LocalBlobStore` for
offline use; an S3-compatible backend can be added without changing the
ingestion/curation contracts or selecting a database supplier.

## Offline verification

```bash
cd python
poetry install --extras serving
poetry run pytest tests/test_ons_pay_refresh.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Tests create actual pinned Git checkouts and handoffs, run ingestion plus
DuckDB curation, exercise the existing serving catalog, and verify freshness,
multi-source backfills, no-write planning, retry/reuse, provenance, failure
audits, source tampering, SQL quality gates, and conflicting Parquet bytes.
They require neither network access nor a hosted database account.

After backfilling at least two vintages, use the
[read-only release comparison gate](ons-pay-release-comparison.md) to examine
additions, deletions, historic revisions and missing-pay transitions between
an explicit baseline and candidate. This does not promote either pin.
