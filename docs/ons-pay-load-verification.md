# Verify a completed ONS raw load

Use `ons-pay-load-verify` to check that a successful raw-load batch still has the
exact local or S3 bytes recorded by its ingestion receipts. This is useful before
[offline transformation](ons-pay-offline-transform.md) or as a scheduled integrity
check for projects that stop at loading.

```bash
cd python
poetry run data-platform-lab ons-pay-load-verify \
  --load-audit ../data/manifests/ons_pay_load_<run-id>.json \
  --object-root ../data/object-store

# Also require source retrieval dates to be at most 30 UTC calendar days old.
poetry run data-platform-lab ons-pay-load-verify \
  --load-audit ../data/manifests/ons_pay_load_<run-id>.json \
  --object-root ../data/object-store --max-source-age-days 30
```

The command requires no analytics extra, source checkout, original handoff,
source provider connection or database. It reads the selected local load audit, its linked
ingestion receipts and the specified raw storage. It creates no directories,
locks, receipts or model outputs, and never refetches source data, repairs or rewrites objects.
A missing object root produces missing-object results without creating that root.

## Verify S3 raw storage

Use the same explicit bucket and prefix as the load, with the existing
[S3 configuration and SDK credential chain](ons-pay-s3-storage.md):

```bash
poetry run data-platform-lab ons-pay-load-verify \
  --load-audit ../data/manifests/ons_pay_load_<run-id>.json \
  --backend s3 --bucket "$DPL_S3_BUCKET" \
  --endpoint-url "$DPL_S3_ENDPOINT_URL" --region "$AWS_DEFAULT_REGION" \
  --key-prefix ons/raw
```

Omit endpoint/region for standard SDK resolution. `--object-root` and S3 settings
are mutually exclusive. S3 verification needs boto3 and read access to the selected
objects; it issues GET requests, with no HEAD, listing, upload, deletion or bucket
creation. It validates the complete evidence and byte budgets before constructing
the S3 client. Authentication uses runtime SDK configuration, never credential flags.

The GET response provides both the body length and stream. A different length is
reported without consuming the body; matching lengths are hashed in bounded chunks.
ETags and server-provided checksums do not replace the expected SHA-256 comparison.
Every opened response body is closed, including on mismatch or read failure.
Explicit missing-key responses produce `missing`; access denial, unavailable
buckets, transport errors and malformed responses produce `unreadable`. A truncated
SDK stream may raise a read error and therefore report `unreadable` rather than
`size_mismatch`. Later sources are still checked. SDK diagnostics are not printed.

The S3 report uses credential-free `object_store` settings instead of `object_root`.
The local report shape is unchanged. The explicitly selected location is checked;
receipt destination URIs are not followed, so verified copies can be inspected too.

## Results and exit codes

| Outcome | Exit code | Output |
| --- | --- | --- |
| All raw objects match their checksums/sizes and the optional age policy passes | `0` | JSON with `status: "healthy"`. |
| Any object is missing, unreadable, unsafe or mismatched, or a source is stale | `1` | JSON with `status: "unhealthy"` and results for every source. |
| S3 client cannot initialize | `1` | A fixed diagnostic on stderr; no JSON report. |
| Invalid options, limits, storage configuration or load evidence | `2` | A fixed diagnostic on stderr; no partial JSON report. |

The report includes `schema_version: 1`, `writes: false`, the check's UTC timestamp,
load path/digest/run ID, optional saved-job digest, raw storage location, limits,
`objects_expected`, `objects_verified`, `bytes_verified` and ordered `sources`.
Each source includes its commit, expected SHA-256, storage key, ingestion receipt
path/digest, retrieval date and age, and these results:

| Field | Meaning |
| --- | --- |
| `integrity` | `verified`, `missing`, `unreadable`, `unsafe`, `size_mismatch` or `checksum_mismatch`. |
| `expected_size_bytes` | Byte count recorded by ingestion. |
| `stored_size_bytes` | Opened file size or S3 GET content length, or `null` if unavailable/unsafe. |
| `bytes_read` | Bytes consumed by the checksum check; a size mismatch detected before reading has zero. |
| `observed_sha256` | Hash of a complete body of the expected length; otherwise `null`. |
| `freshness_passed` | `true`/`false` for the selected age policy, or `null` when disabled. |

Raw integrity and freshness are separate. Stale but intact objects still count in
`objects_verified` and `bytes_verified`; the overall report is unhealthy.
`--max-source-age-days` accepts a non-negative integer and uses UTC calendar dates
from the source's `retrieved_at`, not load completion time. Equality passes. A
future retrieval date invalidates the input. Verification never refreshes that date.

## Evidence and read limits

The command reuses the offline transformation preflight: only a successful whole
`ons_pay_load` v1 batch with consistent source identities, counts and linked
ingestion receipts is accepted. Failed/partial batches and full refresh audits are
rejected. The entire metadata batch is validated before any raw object is opened.
This retains the existing limits of 256 sources, 4 MiB per load audit and 1 MiB per
ingestion receipt, along with strict JSON and symlink checks. Receipt paths remain
absolute; moving a load audit does not rebase its links.

Pass `--load-sha256 <64-lowercase-hex-characters>` to bind inspection to previously
selected exact audit bytes. The report identifies the ingestion-receipt snapshots
used for comparison as well. Hashes establish content identity, not authenticity;
keep evidence and local directories under trusted control.

Defaults permit **16 MiB per raw object** and **64 MiB of expected raw bytes per
batch**. Raise them explicitly with `--max-object-bytes` and `--max-total-bytes`,
both positive integers. Expected sizes are checked before reading any object.
Regular local files and S3 streams are hashed in chunks of at most 64 KiB, with at most
one extra probe byte per object to detect a body longer than its initial size. These
limits bound bytes consumed by verification, not SDK retries or network buffering.
S3 timeouts and retries use the existing SDK configuration. Local symlinks,
paths through symlinked directories and special files are rejected. Repeated
content shared by different vintages is checked and counted once per source.

This is a point-in-time scan, not an atomic batch snapshot or a new ingestion
audit. It can run while a cooperating writer holds destination guards; later
changes can invalidate an earlier report. The command checks the single approved
ONS raw CSV per source, not other derived registry objects or CSV/model quality.
Offline transformation performs its own checks before curation. This command does
not install scheduling or alert delivery and does not pin S3 object version IDs.

Use [saved-job status](ons-pay-job-status.md) for the latest completed job outcome
and [curated monitoring](ons-pay-monitoring.md) for Silver/Gold quality and health.
This check does not change either history or release state.

The Python API `verify_ons_pay_load` in
`data_platform_lab.observability.ons_pay_load_verification` accepts `load_audit`,
exactly one of `object_root` or `object_store=S3Storage(...)`, optional
`expected_load_sha256`, `max_source_age_days`, byte limits
and a timezone-aware `now`. It returns `LoadVerificationReport` with `to_dict()`,
or raises `LoadVerificationError` for invalid input and `LoadVerificationStorageError`
if S3 client initialization fails.
