# Transform a loaded ONS batch offline

Use `ons-pay-transform` after [loading raw data](ons-pay-refresh.md#load-raw-data-without-transformation)
to curate every source in a successful load batch. It reads the stored raw objects,
checks their checksums and sizes, and runs the existing ONS quality gates and
Silver/Gold models. It does not download sources or require the original handoffs
or Git checkouts. PostgreSQL, MongoDB and release activation remain separate steps.

```bash
cd python
poetry install --extras analytics

# Use the receipt_path returned by a successful --load-only run.
poetry run data-platform-lab ons-pay-transform \
  --load-audit ../data/manifests/ons_pay_load_<run-id>.json \
  --object-root ../data/object-store --output-root ../data --dry-run

# Optionally bind execution to the exact load_audit_sha256 printed by the preview.
poetry run data-platform-lab ons-pay-transform \
  --load-audit ../data/manifests/ons_pay_load_<run-id>.json \
  --load-sha256 <64-character-load-audit-sha256> \
  --object-root ../data/object-store --output-root ../data
```

The three paths are required and explicit; relative command-line paths use the
working directory. `--object-root` must contain the previously loaded canonical
objects. `--output-root` may be the load destination or another local directory.
Both HTTPS and checkout sources, including mixed batches, use the same offline path.

## Preview and input evidence

Preview validates a successful `ons_pay_load` v1 audit and **all** its ingestion
receipt links before any curation. It preserves source order, rejects duplicate
vintages, checks the source pins and publication/byte counts, and captures each
receipt's exact SHA-256. JSON reports `status: planned`, `writes: false`, model
version, output roots, load identity and the ordered ingestion receipt identities.

Preview reads metadata only. It opens no object store, does not check stored bytes,
CSV quality, existing Parquet or destination writability, and creates no files,
directories or locks. It does not need optional analytics dependencies. Execution
repeats metadata preflight; `--load-sha256` rejects a changed load audit. Within that
execution, each ingestion receipt must still match its captured digest when curation
reads it. Every raw object is independently verified before its model runs.

Only a **successful whole raw-load batch** is accepted. A failed or incomplete batch,
full-refresh audit, malformed JSON, duplicate JSON keys, non-finite values,
inconsistent evidence or symlinked receipt is rejected. Limits are 256 sources,
4 MiB for the load audit and 1 MiB per ingestion receipt. Original load audits use
absolute ingestion receipt paths; these must remain available. Moving only the
audit does not relocate its linked receipts. No "latest" audit is selected implicitly.

Receipts are trusted local evidence, not cryptographic attestations. The selected
audit binds source commit/checksum identities; ingestion receipt hashes bind the
metadata observed during this execution. Keep the evidence and destination directories
under trusted control. A successful old load can be transformed as a historical
snapshot: this operation does not renew source retrieval freshness.

## Completion, failures and retry

Each source produces the usual curation receipt and content-addressed Parquet.
A separate `manifests/ons_pay_transform_<run-id>.json` batch audit records success
or failure, the load audit path/digest/run ID, any original load job digest,
completed source identities, ingestion receipt hashes, curation receipt paths,
row counts and Parquet publication/reuse counts. Failure evidence identifies
`preflight` or `curation`, the exception class and the zero-based source index where available. Raw
exception text and source rows are excluded.

| Outcome | Exit code |
| --- | --- |
| Valid preview or successful transformation | `0` |
| Execution/preflight failure, guard setup failure or audit publication failure | `1` |
| Invalid CLI arguments or failed preview validation | `2` |
| A cooperating writer holds either local destination | `3` |

Execution acquires the same [local guards](ons-pay-refresh.md#overlapping-runs)
as refresh/load operations before preflight and holds them through the final audit.
Busy rejection and guard setup failure create no batch audit. Standalone
`ons-pay-curate` and other external writers still require separate coordination.
The Python API also accepts a `BlobStore`; nonlocal stores have no distributed guard.

A later failure preserves earlier successful sources. After addressing its cause,
rerun the same command: it revalidates receipts and raw bytes and reuses identical
Parquet without changing its timestamps. It fails on existing Parquet drift.
This is not an atomic multi-source transaction; a process crash or audit-write
failure can leave outputs without a completed batch receipt. Keep process exit codes
and logs, and use an explicit retry to reconcile matching outputs.

Transformation does not create `ons_pay_refresh` evidence or alter the original
load/job status. [`ons-pay-job-status`](ons-pay-job-status.md) continues to describe
the original saved load or refresh job. The [curated monitor](ons-pay-monitoring.md)
can verify the resulting vintage's tables, but transformation does not reset its
full-refresh recency. Inspect the transformation audit for this batch's progress.
In a new output root with no full-refresh audit, that monitor reports overall
`unhealthy` for missing refresh evidence even when curation/table checks pass.

For Python callers, `plan_ons_pay_transform` and `transform_ons_pay` are in
`data_platform_lab.orchestration.ons_pay_transform`. They return immutable
`TransformPlan` and `TransformResult` records. `TransformError.receipt_path` identifies
a published failure audit when available; `TransformBusyError` represents contention.
Single-receipt `ons-pay-curate` usage remains supported.
