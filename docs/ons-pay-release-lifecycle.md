# Manually activate and roll back pinned ONS pay releases

The [release comparison](ons-pay-release-comparison.md) answers whether a
candidate satisfies an operator's quality thresholds. This subsequent step
records **which exact curated vintage was manually activated**, with a
reviewable preview, immutable audit history, and an atomic pointer update.

There is no scheduler, auto-promotion, deployment, hosted database, approval
service, or supplier choice. The active pointer is a local metadata record,
**not** an alias used by the existing analytical data endpoints.

## Prerequisites and contract

Curate both vintages first with [pinned refresh/backfill](ons-pay-refresh.md)
or [ONS pay curation](ons-pay-analytics.md). You need their **complete**
40-character Git registry commits and 64-character source SHA-256 hashes.
Every release also records the v1 SQL model version. The command validates
source receipts and actual Silver/Gold Parquet integrity before a decision.

```bash
cd python
poetry install --extras serving

# Read-only: no release metadata is created when none exists.
poetry run data-platform-lab ons-pay-release --data-root ../data status
```

`status` prints `{"active": null}` before the first release, otherwise the
active exact pin, generation, SHA-256 event hash, and the complete verified
event chain. The `--data-root` option precedes the `status`, `activate`, or
`rollback` subcommand.

## First activation: explicit bootstrap

A new installation has no active baseline. An operator must expressly opt in
to `--bootstrap`; comparisons are impossible at this point and their six
threshold flags must be **omitted**. This exception only applies to the first
generation, and it does not waive curated data integrity checks.

```bash
# Preview (default); verifies the candidate but writes nothing.
poetry run data-platform-lab ons-pay-release --data-root ../data activate \
  --candidate-registry-commit FIRST_COMMIT_SHA40 \
  --candidate-source-sha256 FIRST_SOURCE_SHA256 \
  --expected-generation 0 --bootstrap \
  --reason "Initial verified vintage"

# Repeat exactly the reviewed command with --apply to publish generation 1.
poetry run data-platform-lab ons-pay-release --data-root ../data activate \
  --candidate-registry-commit FIRST_COMMIT_SHA40 \
  --candidate-source-sha256 FIRST_SOURCE_SHA256 \
  --expected-generation 0 --bootstrap \
  --reason "Initial verified vintage" --apply
```

The sample SHA strings are placeholders, **not** actual pins. Bootstrap is
never implicitly inferred and cannot be used again after generation 1.

## Promotion: six mandatory quality gates

For all later releases, the current active pin is the comparison baseline.
All six policy flags are **required**, each with an explicit nonnegative
limit; there are no hidden defaults. A skipped or breached comparison check
cannot activate a candidate, even if `--apply` is supplied. A different Git
commit with identical business data is a valid candidate if its curation and
all six checks pass.

```bash
# Preview baseline generation 1 -> candidate; no mutation.
poetry run data-platform-lab ons-pay-release --data-root ../data activate \
  --candidate-registry-commit NEXT_COMMIT_SHA40 \
  --candidate-source-sha256 NEXT_SOURCE_SHA256 \
  --expected-generation 1 --reason "Reviewed 2026 pay refresh" \
  --max-added-rows 2000 \
  --max-removed-rows 0 \
  --max-revised-rows 300 \
  --max-newly-missing-pay-rows 0 \
  --max-missing-pay-increase-pp 1 \
  --max-absolute-pay-revision-gbp 100

# After reviewing the JSON output, append --apply to the SAME command.
```

These numbers only demonstrate the CLI and are **not approved release
policy**. Choose bounds based on the source's expected scope and changes.
The preview includes the full null-safe comparison report, `eligible`,
`applied: false`, and the active baseline pin. An applied pass has
`applied: true`. Policy breaches return `eligible: false, applied: false`;
an attempted apply does not create a new event or move the pointer.

`--expected-generation` acts as an optimistic concurrency check: when another
operator has activated or rolled back a release since your preview, the apply
is rejected. The implementation **re-runs** validation and comparison inside
an exclusive local write lock, so preview evidence is never blindly trusted.

## Rollback: restore a previously active pin

A rollback is also an explicit event, not deletion of the newer release. It
targets an **earlier generation whose exact pin was active**, and verifies
that its curated tables remain intact. It must differ from the current pin.
For example, after promoting to generation 2:

```bash
# Read-only preview of generation 2 -> the pin active at generation 1.
poetry run data-platform-lab ons-pay-release --data-root ../data rollback \
  --to-generation 1 --expected-generation 2 \
  --reason "Regression found in downstream validation"

# Explicitly apply the reviewed rollback, creating generation 3.
poetry run data-platform-lab ons-pay-release --data-root ../data rollback \
  --to-generation 1 --expected-generation 2 \
  --reason "Regression found in downstream validation" --apply
```

The full event chain remains auditable: bootstrap (1), promote (2), rollback
(3), each recording UTC timestamp, reason, exact before/after pins, previous
event SHA, and—for promotions—the six quality checks and comparison summary.
Rollbacks cannot silently bypass a missing or corrupted target.

The [active-release health assessor](ons-pay-release-health.md) can run the
existing pinned integrity/freshness checks against the current active event
and identify a previously activated, healthy version for **manual review**.
Its advice does not modify the pointer or replace the explicit rollback
preview and `--apply` steps above.
When desired, the [health-history archive](ons-pay-release-health-history.md)
can record individual assessments and summarize health trends across this
release chain without modifying the selected vintage.

## Durable local state and safety boundaries

The command writes only under the curated data root:

| Path | Purpose |
| --- | --- |
| `releases/ons-pay/events/<event-sha256>.json` | Canonical, SHA-256 content-addressed release decision; records are never overwritten |
| `releases/ons-pay/active.json` | One atomic pointer to the active event hash, generation, and exact pin |
| `releases/ons-pay/.release.lock` | Local POSIX advisory writer lock |

On apply, the event is written and synced before the active pointer is
replaced through an atomic rename and directory sync. If the pointer update
fails before replacement, the prior release stays active; an unused event may
remain on disk without becoming visible as the active generation. Read-only
previews do not create even the lock file. An interrupted operation should be
followed by `status` before retrying, since a crash during the final sync can
leave the outcome uncertain. Do not manually edit event or pointer JSON.

History reads re-hash referenced event files, validate the entire linked
event chain and policy decisions, and re-verify the **currently active**
Parquet. A broken link, modified event, missing pinned data, or unsafe
symlink fails closed. Event hashes provide tamper evidence under a **trusted
local filesystem**, not cryptographic signatures or human identity. A user
who can rewrite the entire trusted data root can forge both data and history.

This is a single-machine, trusted-operator workflow on a POSIX filesystem
supporting durable rename, fsync and advisory locks. It is **not** a shared
database transaction across machines and does not provide authorization,
multi-host locking, automatic recovery, deployment, or external notifications.
Do not expose the local serving API publicly without an authenticated gateway.

## API discovery and exit statuses

Once manually activated, the read-only serving API exposes:

```text
GET /v1/ons-pay/releases/active
```

The response contains `generation`, `event_sha256`, and the verified
`vintage` metadata. Before bootstrap the endpoint returns `404`; a damaged
release chain or missing active Parquet yields a sanitized `503`. This does
**not** change the existing `geography-year` and `areas/{area_code}/trend`
routes: callers must still pass a full exact commit + source SHA. No
`latest` alias or implicit active-data selection is introduced.

| CLI exit code | Meaning |
| --- | --- |
| `0` | Read succeeded, preview eligible, or an eligible action was applied |
| `1` | A configured promotion policy breached; no new active generation |
| `2` | Missing pins, stale generation, unsafe/corrupt data or history, or invalid arguments |

## Offline verification

```bash
cd python
poetry run pytest tests/test_ons_pay_release.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Tests build real synthetic DuckDB/Parquet vintages and validate previews,
bootstrap, promotion gates, immutable SHA-linked records, generation checks,
rollback, the read-only endpoint, corrupt history, missing pinned files,
failed pointer replacement, and CLI exit codes. No provider account, hosted
database, or network connection is required.
