# Saved ONS refresh jobs

Use `ons-pay-refresh --job` to keep an ordered batch, explicit source pins,
destinations, freshness policy and HTTPS limits in one JSON file. It runs the
existing [refresh pipeline](ons-pay-refresh.md): verified raw loading, Silver/Gold
curation and an atomic batch audit. The file can be passed to a local scheduler;
creating it does not install a schedule or discover newer upstream versions.

## Prepare a job

Obtain a trusted handoff using the [registry guide](registry-handoff-ingestion.md).
Choose its full registry commit independently, then calculate the SHA-256 of the
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

The optional `fetch` keys use the same defaults and validation as direct HTTPS
ingestion. `max_source_bytes` resets per handoff and counts partial retry bodies.
Omitting `max_attempts` preserves the default of one attempt. See the
[retry policy and limits](registry-handoff-ingestion.md#retry-temporary-extraction-failures).
The socket timeout is not a deadline for the whole batch.

Job files are bounded to 256 KiB. Unknown or duplicate fields, unsupported schema
versions, malformed pins, invalid policies and non-finite JSON numbers are
rejected. `--job` cannot be combined with source arguments or overrides for
destinations, freshness, limits or retries—even an override equal to a default.
Edit the saved configuration to change those settings. `--dry-run` is supported.

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

Treat a job as trusted execution configuration: it selects local inputs and write
destinations. No shell commands, credential fields, variable interpolation, database
publication or release activation are supported. This workflow always curates ONS
data; use `registry-ingest` for raw loading alone. Continue to use one controlled
writer per destination; a job file does not add concurrent-writer coordination.

The Python API `load_refresh_job` in
`data_platform_lab.orchestration.ons_pay_refresh_job` returns an immutable
configuration with resolved paths and existing refresh source objects. It reads
only the job; call `plan_ons_pay_refresh` to validate its handoffs or
`refresh_ons_pay` with its sources, destinations, freshness policy and
`job_sha256=job.sha256` to execute and retain configuration lineage.
