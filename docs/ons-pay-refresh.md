# ONS pay refresh, historical backfill and freshness reporting

This workflow connects the existing [SHA-pinned canonical ingestion](registry-handoff-ingestion.md),
[DuckDB Silver/Gold curation](ons-pay-analytics.md), and
[read-only analytical API](ons-pay-serving.md) behind one repeatable command.
Outputs use local storage; no hosted database or cloud account is required.

The refresh process runs **explicitly pinned handoffs**, not a floating
`main` branch or an implicit latest ONS download. Each handoff is paired with
an exact checkout of [`DiogoRibeiro7/data`](https://github.com/DiogoRibeiro7/data)
or an independently selected full commit for HTTPS extraction. Prepare the
handoff using the [registry guide](registry-handoff-ingestion.md) first.

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

## Extract, load and transform over HTTPS

Use `--fetch-source HANDOFF_JSON REGISTRY_COMMIT` for the complete workflow
without a local registry checkout. This command accepts only the approved
`DiogoRibeiro7/data` repository and `ons-gross-median-weekly-pay` dataset.
Choose the full commit independently of the handoff, following the
[source trust requirements](registry-handoff-ingestion.md#extract-and-load-pinned-ons-data).

```bash
export ONS_REGISTRY_COMMIT="<trusted-full-40-character-commit>"

# Validate every manifest, pin, budget and freshness policy without network or writes.
poetry run data-platform-lab ons-pay-refresh \
  --fetch-source ../data/manifests/ons-handoff.json "$ONS_REGISTRY_COMMIT" \
  --dry-run

# Download verified raw bytes, load them, curate Silver/Gold and record the batch audit.
poetry run data-platform-lab ons-pay-refresh \
  --fetch-source ../data/manifests/ons-handoff.json "$ONS_REGISTRY_COMMIT" \
  --timeout-seconds 30 --max-object-bytes 16777216 --max-source-bytes 67108864 \
  --object-root ../data/object-store --output-root ../data
```

Repeat `--fetch-source` for historical handoffs, each with its own trusted commit;
the batch processes them in argument order. The CLI accepts either checkout
sources or HTTPS sources per invocation. The Python API also accepts an ordered
mixture of `RefreshSource` and `RemoteRefreshSource` values.

All manifests pass preflight before the first download. Each execution binds
ingestion to the SHA-256 of the manifest bytes read by its preflight; changing
even metadata between those stages fails that source. A separate `--dry-run`
prints this digest and the source transport but does not lock a later execution.

The existing [fetch integrity controls](registry-handoff-ingestion.md#fetch-integrity-and-operational-limits)
apply: exact HTTPS URLs, no redirects, bounded reads, checksum and size validation,
and verification of destination bytes. `--timeout-seconds` limits each blocking
socket operation, not total batch duration. `--max-object-bytes` caps each object;
`--max-source-bytes` caps the sum downloaded for **each handoff**, resetting for
the next source. These fetch flags require `--fetch-source`. The default is one
attempt per object; explicit reruns fetch and verify source bytes again before reusing outputs.

For temporary HTTP or connection failures, add `--max-attempts 3` and optionally
`--retry-delay-seconds 1`. The
[bounded extraction retry policy](registry-handoff-ingestion.md#retry-temporary-extraction-failures)
applies to each object, honors server wait hints up to 30 seconds, and counts
partial attempts against the same source byte budget. It never retries storage
writes or curation. Dry-run reports the policy without fetching or sleeping.

Remote refresh uses the existing batch audit, linked ingestion and curation
receipts, and monitoring command. The ingestion receipt records
`source_transport: "https"` plus attempt, retry and downloaded-byte statistics;
source retrieval dates retain their upstream meaning.
For a single raw load, use `registry-ingest --fetch`. For ordered raw-only batches,
use the mode below.
[Database publication](ons-pay-portable-publishing.md) remains a separate command.

## Load raw data without transformation

Add `--load-only` to a direct invocation to stop after verified raw loading:

```bash
poetry run data-platform-lab ons-pay-refresh --load-only \
  --fetch-source ../data/manifests/ons-handoff.json "$ONS_REGISTRY_COMMIT" \
  --object-root ../data/object-store --output-root ../data
```

With `--fetch-source`, this extracts and loads pinned HTTPS bytes. With `--source`,
it loads bytes from the exact local checkout. It supports the same ordered batches,
freshness gates, handoff digests, checksum/size checks, extraction retries and
destination guards as a full refresh. `--dry-run --load-only` remains read-only.
The optional analytics dependency is unnecessary for this mode.

A raw-only batch creates `manifests/ons_pay_load_<run-id>.json` under `output_root`,
with `pipeline_name: "ons_pay_load"` and `schema_version: 1`. The audit retains
source pins, retrieval ages, ingestion receipt paths, verified bytes, object
publication/reuse counts, any job digest, and partial failures. It omits model
version, curation receipt paths, Parquet counts and row-quality metrics because
transformation did not run. CLI summaries and previews include `load_only: true`;
success summaries omit Parquet counts. Existing exit codes apply.

The raw payload is preserved unchanged, including bytes that may later fail CSV
schema or business-quality checks. A successful load is evidence of source and
storage integrity; it does not establish analytical suitability. No Silver/Gold
outputs, serving entries or release changes are produced. Both successful and
failed raw-load batch audits are excluded from `ons-pay-monitor` refresh history,
so a load cannot reset curated-data freshness or hide a failed full refresh.
Use the load command's exit status and load audit to assess ingestion outcomes.

To transform the entire successful load batch offline, use
[`ons-pay-transform`](ons-pay-offline-transform.md) with its load audit:

```bash
poetry run data-platform-lab ons-pay-transform \
  --load-audit /path/to/ons_pay_load_<run-id>.json \
  --object-root ../data/object-store --output-root ../data --dry-run
```

Remove `--dry-run` to execute. It checks all ingestion receipt links first, then
verifies raw bytes and runs curation with separate batch progress evidence.

To transform later without downloading again, pass an `ingestion_receipt_path`
from the load audit to the existing command:

```bash
poetry install --extras analytics
poetry run data-platform-lab ons-pay-curate \
  --receipt /path/from/load-audit/registry_handoff_ingestion_<run-id>.json \
  --object-root ../data/object-store --output-root ../data
```

This verifies stored bytes and runs the normal curation quality checks. It does
not fabricate a full refresh audit. Alternatively, run `ons-pay-refresh` without
`--load-only`; that rechecks the sources, reuses matching raw objects and creates
normal refresh evidence. A [saved job](ons-pay-refresh-jobs.md) selects raw loading
with the boolean `"load_only": true`; CLI overrides of saved settings are rejected.

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
additionally checks each real Git blob, its checksum, storage key and output
for checkout sources. HTTPS backfills verify downloaded bytes as described above.

A failed second source does **not** roll back a successfully published first
source: the objects are immutable, and there is no cross-store transaction.
Instead, the batch writes a `status: failed` receipt with the completed
vintages, metrics, and failing `phase` (`preflight`, `ingestion`, `curation`)
and zero-based `source_index`. If ingestion had succeeded but curation failed,
the failure includes its successful ingestion receipt path. Fix the cause and
rerun the full batch; previously valid objects and Parquet outputs are checked
and reused. Corrupted immutable files are never silently overwritten.
Failure audits retain the exception type and stage with a fixed diagnostic,
so raw transport or transformation exception text is not stored in the audit.
Successful runs exit `0`; invalid arguments or dry-run preflight exit `2`.
Execution failures (including execution preflight) exit `1` with an audit path
when the audit could be written. Destination guard errors exit `1` before a run
starts, without an audit; a busy destination exits `3` as described below.

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

`--dry-run` checks handoff structure, duplicate identity, the selected freshness
policy and, for HTTPS sources, explicit pins and fetch budgets. It does **not** validate
Git checkouts, source file bytes, storage integrity, or downstream SQL quality;
those are rechecked during the actual run.

## Overlapping runs

Actual refreshes automatically acquire nonblocking local guards for the resolved
`output_root` and, for `LocalBlobStore`, its object root. Another refresh sharing
either exact root is rejected before handoff preflight, source loading or fetching.
This applies to direct CLI arguments, saved jobs and the Python API. The CLI exits
`3` with a fixed busy message; the API raises `RefreshBusyError` with no receipt.
There is no waiting, extraction retry or failed batch audit for a rejected run.
A scheduler can retry later; dry-run neither acquires guards nor checks availability.

Guards remain held through success or failure audit publication. Windows uses
an OS byte-range lock and POSIX uses `flock`; process exit or termination releases
ownership automatically. For `/work/objects`, metadata lives in the sibling file
`/work/.objects.ons-pay-refresh.lock`. Destination parent directories must be
writable, and missing parents may be created. Metadata is outside that root's
object namespace, so a separate object store's listings remain unchanged.
The empty lock files persist after release: **do not delete or replace them while
runs may be active**. Their existence does not mean a job is running, and no stale
PID cleanup is needed. Existing metadata is never truncated or rewritten; symlinks,
hard links and nonregular lock files are rejected.

Coordination covers cooperating refresh processes using the same resolved roots
on one host's local filesystem. Keep these directories under trusted control.
Different nested roots are not treated as the same destination. Standalone
`registry-ingest`, `ons-pay-curate` and other writers must still be coordinated
separately. Network filesystems and distributed execution are outside this
guarantee; a nonlocal `BlobStore` is protected only by its local output-root guard,
not by a remote object-store lock.

## Scheduling and operating limits

For repeatable invocations, use a [saved JSON refresh job](ons-pay-refresh-jobs.md).
Add `--save-job /path/to/new-job.json` to direct source arguments to prepare that
configuration without executing it; combine with `--dry-run` for a read-only preview.
Preparation captures handoff digests and refuses to overwrite an existing job.
The job file records source order, exact handoff digests, local destinations and policies
in one configuration, with paths relative to that file. It supports mixed checkout
and HTTPS sources, and adds the job digest to the existing batch audit.

The CLI is deterministic and exits nonzero on failure, so a local scheduler,
CI job, Dagster asset or later managed orchestrator can run it with the
*explicit handoff and checkout paths or HTTPS commits*. Freshness breaches and partial failures
produce machine-readable audits for monitoring. No unattended schedule or
data-provider polling is configured, because silently promoting
a floating upstream version would break the explicit pin contract.
Use the [read-only monitoring command](ons-pay-monitoring.md) to check the
published table contracts and refresh audit history, obtain numeric health
metrics, and return alert-ready exit codes for a configured scheduler.

The process enforces one cooperating refresh writer per local destination. It does not support
distributed locking, rollback across multiple sources, concurrent publication,
automatic registry-version discovery, or signed attestations against a local
actor who can rewrite both receipts and files. Core orchestration accepts a
`BlobStore` implementation, but the current CLI chooses `LocalBlobStore` for
local output; an S3-compatible backend can be added without changing the
ingestion/curation contracts or selecting a database supplier.

## Offline verification

```bash
cd python
poetry install --extras serving
poetry run pytest tests/test_ons_pay_refresh.py tests/test_ons_pay_remote_refresh.py tests/test_ons_pay_refresh_job.py tests/test_ons_pay_load_only.py tests/test_local_run_guard.py tests/test_ons_pay_refresh_guard.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Tests create actual pinned Git checkouts and handoffs, run ingestion plus
DuckDB curation, exercise the existing serving catalog, and verify freshness,
multi-source backfills, no-write planning, retry/reuse, provenance, failure
audits, source tampering, SQL quality gates, and conflicting Parquet bytes.
HTTPS tests use synthetic responses to cover ordered backfills, preflight before
network access, manifest changes, partial failure recovery and audit compatibility.
They require neither network access nor a hosted database account.
Separate-process tests check contention, independent destinations, release after
exceptions or termination, and ownership through audit publication on Windows and POSIX.

After backfilling at least two vintages, use the
[read-only release comparison gate](ons-pay-release-comparison.md) to examine
additions, deletions, historic revisions and missing-pay transitions between
an explicit baseline and candidate. This does not promote either pin.
