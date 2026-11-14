# Saved ONS refresh jobs

Use `ons-pay-refresh --job` to keep an ordered batch, explicit source pins,
destinations, freshness policy and HTTPS limits in one JSON file. It runs the
existing [refresh pipeline](ons-pay-refresh.md): verified raw loading, Silver/Gold
curation and an atomic batch audit. An opt-in raw-only mode stops after loading.
The file can be passed to a local scheduler;
creating it does not install a schedule or discover newer upstream versions.

## Prepare a job

Obtain a trusted handoff using the [registry guide](registry-handoff-ingestion.md).
Choose its full registry commit independently. To generate a job from that selection,
add `--save-job` to direct refresh arguments:

```bash
cd python
export ONS_REGISTRY_COMMIT="<trusted-full-40-character-commit>"

# Preview the exact configuration without creating any files or directories.
poetry run data-platform-lab ons-pay-refresh \
  --fetch-source ../handoffs/ons.json "$ONS_REGISTRY_COMMIT" \
  --object-root ../data/object-store --output-root ../data \
  --max-source-age-days 365 --max-attempts 3 --load-only \
  --save-job ../jobs/ons-load.json --dry-run

# Save configuration only; this does not extract, load or transform data.
poetry run data-platform-lab ons-pay-refresh \
  --fetch-source ../handoffs/ons.json "$ONS_REGISTRY_COMMIT" \
  --object-root ../data/object-store --output-root ../data \
  --max-source-age-days 365 --max-attempts 3 --load-only \
  --save-job ../jobs/ons-load.json

# Execute the saved configuration separately.
poetry run data-platform-lab ons-pay-refresh --job ../jobs/ons-load.json
```

Use `--source HANDOFF_JSON PINNED_CHECKOUT` for checkout jobs, or repeat the selected
source flag for a historical batch. Omit `--load-only` to prepare full refresh.
Preparation reads and validates every handoff using the existing preflight, captures
its exact SHA-256, and records the selected policies and HTTPS limits. Add
`--reuse-verified` to save `"reuse_verified": true` in `fetch`; the default `false`
is omitted from generated jobs so existing job bytes remain stable.
It opens no source transport or object store, runs no curation, and acquires no
destination guards. The source checkout and raw bytes are verified during execution.

Input paths are interpreted from the invocation's working directory. The saved
paths are relative to the resolved job directory where possible; paths on different
Windows drives remain absolute. Moving the whole directory tree preserves relative
relationships; moving the job alone may require path edits. JSON results report
`status` (`planned` or `saved`), `writes`, `job_path`, `job_sha256` and the job document.
The digest identifies the formatted UTF-8 file bytes, including its final newline,
not the JSON envelope printed to the terminal. Unchanged inputs produce identical
job bytes. A preview does not reserve the file or authorize a later execution.

`--save-job` never overwrites an existing file, directory or symbolic link. Saving
creates missing job parent directories and publishes the complete file exclusively
using a temporary file and hard link, as with immutable platform evidence. Use a
local filesystem supporting hard links; a failed save may leave newly created
parent directories. Keep job directories under trusted control. The job and its
destinations must remain outside every selected canonical checkout. `--job` and
`--save-job` cannot be combined.

Preparation or validation errors exit `2` without publishing a job. Publication
failures, including another writer creating the target after preparation, exit `1`;
preview or successful save exits `0`. No refresh/load batch audit is created because
no ingestion ran. Handoffs are revalidated at execution, so a change after preparation
requires an intentional new digest; preparation does not discover or trust new sources.

### Write a job manually

The same v1 schema remains available for hand-written jobs. Calculate the SHA-256 of the
exact handoff bytes. For a handoff saved at `jobs/ons-handoff.json`:

```bash
python -c "import hashlib,pathlib; print(hashlib.sha256(pathlib.Path('jobs/ons-handoff.json').read_bytes()).hexdigest())"
```

Save this UTF-8 JSON as `jobs/ons-refresh.json`, replacing both placeholders:

```json
{
  "schema_version": 1,
  "object_root": "../data/object-store",
  "output_root": "../data",
  "max_source_age_days": 365,
  "fetch": {
    "timeout_seconds": 30,
    "max_object_bytes": 16777216,
    "max_source_bytes": 67108864,
    "max_attempts": 3,
    "retry_delay_seconds": 1
  },
  "sources": [
    {
      "transport": "https",
      "manifest_path": "ons-handoff.json",
      "manifest_sha256": "<64-character-handoff-sha256>",
      "registry_commit": "<trusted-full-40-character-commit>"
    }
  ]
}
```

The repository and dataset remain fixed to `DiogoRibeiro7/data` and
`ons-gross-median-weekly-pay`. The job pins the **handoff file**, while the handoff
pins the canonical data bytes. A whitespace-only handoff edit also changes its
digest and requires an intentional update to the job.

```bash
cd python
poetry install --extras serving
poetry run data-platform-lab ons-pay-refresh --job ../jobs/ons-refresh.json --dry-run
poetry run data-platform-lab ons-pay-refresh --job ../jobs/ons-refresh.json
```

All relative paths are resolved against the resolved job file's directory,
independently of the command's working directory. Absolute paths are supported.
Paths are literal: environment variables and `~` are not expanded. Dry-run prints
the resolved output roots, ordered source pins and `job_sha256`, and creates no
files, connections or retry waits.

## Batch sources and settings

`sources` accepts 1–256 entries, processed in listed order. HTTPS entries require
the four fields shown above. Checkout entries use this shape:

```json
{
  "transport": "checkout",
  "manifest_path": "historical-handoff.json",
  "manifest_sha256": "<64-character-handoff-sha256>",
  "registry_root": "../checkouts/data-historical"
}
```

A job may mix checkout and HTTPS sources. Each checkout must match its handoff's
full registry commit and committed source bytes. Both destinations must remain
outside every selected canonical checkout, including in a mixed batch.

| Job field | Meaning |
| --- | --- |
| `schema_version` | Required integer `1`. |
| `sources` | Required ordered list of pinned handoffs. |
| `object_root`, `output_root` | Required local destinations; no implicit destination defaults in jobs. |
| `max_source_age_days` | Optional non-negative integer; omit or use `null` for historical backfills. |
| `fetch` | Optional settings shared by HTTPS sources; omit when all sources use checkouts. |
| `load_only` | Optional boolean, default `false`; `true` skips transformation and writes a separate raw-load batch audit. |

The optional `fetch` keys use the same defaults and validation as direct HTTPS
ingestion. `max_source_bytes` resets per handoff and counts partial retry bodies.
Omitting `max_attempts` preserves the default of one attempt. See the
[retry policy and limits](registry-handoff-ingestion.md#retry-temporary-extraction-failures).
The socket timeout is not a deadline for the whole batch.

`fetch.reuse_verified` is an optional boolean, default `false`. With `true`, HTTPS
sources first verify existing stored bytes against the handoff's checksum and size,
then download only missing objects. Corrupt or unreadable objects fail without
repair. The policy is shared across HTTPS sources and has no effect on checkout
verification. Cached bytes and partial downloads share the source byte budget;
see [reuse evidence and limits](registry-handoff-ingestion.md#reuse-verified-raw-objects).
Dry-run reports the policy without checking stored objects. Changing this setting
changes the saved job digest and its status history; `--reuse-verified` cannot
override `--job`. Numbers, strings and `null` are rejected for this field.

Job files are bounded to 256 KiB. Unknown or duplicate fields, unsupported schema
versions, malformed pins, invalid policies and non-finite JSON numbers are
rejected. `--job` cannot be combined with source arguments or overrides for
destinations, freshness, limits or retries—even an override equal to a default.
Edit the saved configuration to change those settings. `--dry-run` is supported.

## Load first, transform later

Set `"load_only": true` at the top level of the job to load verified raw objects
without running curation. HTTPS sources extract their pinned bytes or reuse verified
stored bytes when explicitly configured; checkout sources load locally.
Mixed source batches are supported in both modes. Omit the
field or set it to `false` for the original full refresh behavior. Strings such as
`"false"`, numbers and `null` are rejected.

Run the same `ons-pay-refresh --job` command. The preview and success summary
include `load_only: true`; the summary omits Parquet counts. A load audit named
`ons_pay_load_<run-id>.json` records verified bytes, publication/reuse counts and
links to ingestion receipts. It does not claim row-quality checks, create
Silver/Gold outputs or update curated refresh health. Both successful and failed
raw-load batches are excluded from the curated monitor's refresh history. Inspect
their latest completed outcome with [saved-job status](ons-pay-job-status.md):

```bash
poetry run data-platform-lab ons-pay-job-status \
  --job ../jobs/ons-load.json --max-success-age-hours 24
```

This read-only check also supports full refresh jobs. It reports the latest run,
last complete success and failure progress for the exact saved configuration.
Retain invocation exit codes as well: busy rejections and crashes may leave no audit.

Pass a successful load audit to [offline batch transformation](ons-pay-offline-transform.md)
to curate all its sources without downloading again or editing the saved load job.
To check retained raw objects without transformation, use
[`ons-pay-load-verify`](ons-pay-load-verification.md) with that load audit and the
job's local object root. It verifies stored bytes and optional source age without writes.
Change the saved field to `false` for a subsequent full refresh, or use each
ingestion receipt for [separate offline curation](ons-pay-refresh.md#load-raw-data-without-transformation).
The latter needs the analytics extra; raw loading does not. Full refresh rechecks
sources and reuses matching raw objects. Changing the mode changes the job digest.
`--load-only` cannot accompany `--job`, even when it agrees with the saved value.

## Execution, evidence and recovery

The job is read once per invocation. Its raw-byte SHA-256 is returned in the CLI
summary and stored as `job_sha256` in the existing success or failure batch audit.
Editing the job after it has been loaded does not change that execution's settings.
This digest identifies configuration bytes; it is not a signature or an archived
copy of the configuration. Keep the job file for later comparison.

Every handoff must match the job's saved digest during whole-batch preflight,
before the first source download or load. Ingestion rechecks that same handoff
snapshot again before reading source bytes. A later invocation revalidates the
job and handoffs; dry-run does not reserve or authorize a later execution.

The usual refresh rules apply: successful earlier vintages survive a later failure,
and an explicit rerun verifies and reuses matching raw and Parquet outputs without
changing their timestamps. Linked ingestion receipts retain manifest hashes and
transfer statistics. Existing monitoring continues to read the batch audit.

Invalid job syntax, schema, settings or CLI combinations exit `2` without execution.
Source preflight failures exit `2` during dry-run; during execution they exit `1`
with a failure audit. Success exits `0`. The existing source and transformation
quality gates are unchanged.

Overlapping refreshes sharing either resolved local destination exit `3` before
source preflight, with no batch audit. Guard setup failures exit `1` without an
audit. The default [local run guards](ons-pay-refresh.md#overlapping-runs) apply to
saved jobs and direct invocations alike, and release automatically on process exit.
Dry-run does not reserve destinations or check whether they are busy.

Treat a job as trusted execution configuration: it selects local inputs and write
destinations. No shell commands, credential fields, variable interpolation, database
publication or release activation are supported. Full refresh curates ONS data;
`load_only` preserves raw bytes, and `registry-ingest` remains available for single
raw loads and other approved datasets. Coordinate standalone ingestion,
curation and other external writers separately from refresh jobs.

The Python API `load_refresh_job` in
`data_platform_lab.orchestration.ons_pay_refresh_job` returns an immutable
configuration with resolved paths and existing refresh source objects. It reads
only the job; call `plan_ons_pay_refresh` to validate its handoffs or
`refresh_ons_pay` with its sources, destinations, freshness policy and
`job_sha256=job.sha256, load_only=job.load_only` to execute and retain configuration
lineage. The default returns the existing `RefreshResult`; `load_only=True` returns
`LoadResult` containing `LoadedSource` entries, with ingestion fields only.

For programmatic preparation, `prepare_refresh_job` in
`data_platform_lab.orchestration.ons_pay_job_preparation` accepts the same source
objects, destinations, freshness policy and load mode plus the intended job `path`.
It returns immutable `PreparedRefreshJob` bytes without writing. Pass that result
to `save_refresh_job` to publish it exclusively. Mixed checkout/HTTPS lists preserve
source order; HTTPS sources must share one fetch policy, as required by the v1 schema.
