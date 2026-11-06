# ONS pay publishing release readiness

Assessment date: **2026-11-06**. Scope: the Python ONS workflow from registry
handoff to persisted publication outcomes. This is a **portfolio-ready local
data engineering workflow**, subject to the verification evidence below. It is
not a production deployment certification or authorization to activate a release.

## Capability checklist

| Checked capability | Implementation and evidence |
| --- | --- |
| Exact source provenance | `ingestion/registry_handoff.py` requires the pinned checkout HEAD, committed blob, byte count and SHA-256; existing objects and destination read-back are verified. Synthetic-demo tests also reject forged checksums over dirty bytes. |
| Validated analytical output | `curation/ons_pay.py` and its SQL preserve suppressed values, reject invalid identities/measures/dates/numbers, conserve Silver rows and publish three immutable Parquet artifacts. The catalog rechecks hashes, schemas, counts and lineage. |
| Deterministic SQL/document contracts | `serving/publish_plan.py` checks both Gold grains, full vintage pins, exact fields/types, nulls, duplicate IDs and record fingerprints. Both shapes derive their IDs from the same vintage and grain. |
| Idempotent writes and vintage isolation | PostgreSQL uses parameterized upserts in one transaction; MongoDB replaces stable `_id` documents. Neither adapter deletes extra records or older vintages. Offline and live suites cover both Gold tables. |
| Complete reconciliation and acceptance | The reconciler reads the entire selected vintage within its cap. The gate requires matching pins, complete counts, a write receipt and independently recomputed expected sink hashes. Read-only verification cannot generate acceptance. |
| Explicit failure and recovery | Partial MongoDB writes, SQL rollback/commit errors, drift and read/close errors withhold acceptance. A lost SQL commit acknowledgment is treated as uncertain even after rollback succeeds. Recovery uses an explicit replay with the same source pins. |
| Durable outcomes | `PublicationArchive` records accepted/rejected/failed states through the existing gate, with canonical content addresses, POSIX locking, no-replace publication and fsync. Tests cover corruption, interrupted persistence, duplicate attempts and conflicting local writers. |
| Safe operator interface | `ons-pay-publish` defaults to preview, requires explicit pins/target, lazily loads clients and settings, revalidates source after connection, and emits sanitized JSON with documented exit codes. New execution attempts use new UUIDs. |
| Separate release control | Publication never changes the manual release pointer. `ons-pay-release` remains a separate operator action. Evidence tests preserve earlier outcomes and release state after failure. |
| Reproducible walkthrough | The bundled synthetic Git source runs through production APIs in isolated disposable services; two-run smoke tests verify source/plan pins, rejection/recovery, evidence and cleanup. |
| Runtime and CI | The offline suite covers Python 3.11–3.14; quality tools run once on 3.11. Live services and the packaged demo run on Linux/Python 3.11. Feature PRs and `main` pushes are checked without duplicate branch-push runs. |

The audit also corrected the PostgreSQL container's process handling: an init
process reaps orphaned health probes, and `pg_isready` runs without a shell.
This applies the setup already verified by the packaged demonstration to the
regular local platform stack.

## Verification evidence

The merged demonstration baseline was verified at commit
`4162749af82a5fa9553904355703aafdc181a67a`:

- [Python suite and quality checks](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42201538866): 837 passed, 25 opt-in skips; Ruff/format and strict mypy passed.
- [PostgreSQL](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42201539282): 6 live tests passed. [MongoDB](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42201538740): 18 live tests passed.
- [Packaged demo](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42201539208): two independent executions passed in one smoke test, with retained reports/evidence. Recovery produces 12 accepted outcomes and 4 expected rejections.
- A local Windows/Docker Desktop execution also verified artifact hashes,
  durable outcomes and service cleanup. Processing ran inside Linux containers.

Audit-branch results, including the added commit-acknowledgment regressions and
Python version matrix, will be recorded here after validation. Historical
[March reviews](current-state-review.md) retain their original findings and
counts; they do not describe the current publishing implementation.

## Supported execution paths

1. **Complete local demo:** Python 3.11–3.14 on the host plus Docker Compose v2
   with Linux containers, then
   `python scripts/ons_pay_publishing_demo.py --exercise-recovery` from the
   repository root. The mounted output filesystem must support POSIX evidence
   persistence. See the [walkthrough](ons-pay-publishing-demo.md).
2. **Operator/library workflow:** Linux or WSL with a suitable local filesystem;
   install `poetry install --extras serving` from `python/`, adding `mongodb`
   for PyMongo. Use explicitly pinned local curated artifacts and configured
   disposable targets. Follow the [publishing guide](ons-pay-portable-publishing.md).
3. **Offline development:** install the serving extra, run `poetry run pytest`,
   `poetry run ruff check --config pyproject.toml . ../scripts`,
   `poetry run ruff format --config pyproject.toml --check . ../scripts`, and
   `poetry run mypy src/ ../scripts` from `python/`. Native Windows does not
   support the POSIX evidence-writing contract; use the Linux execution path
   for the complete suite. PyMongo is optional for offline tests.

The package explicitly excludes Python 3.15. The host Python version does not
change the demo runner's pinned Python 3.11 image.

## Resource limits and trust boundaries

| Boundary | Current limit or assumption |
| --- | --- |
| Source plans | `--max-rows` is 1–100000; default 100000. Validation checks the complete selected table, rather than sampling. |
| Read-back population | At most 100000 records; readers request one additional record to detect overflow and fail closed. The source `--max-rows` setting does not lower this separate sink cap. |
| SQL write batch | 1–5000 rows per `executemany`, default 500; all batches share one transaction. MongoDB performs one acknowledged replacement per document. |
| Memory and scan cost | Ingestion loads object bytes; curation uses DuckDB; planning/reconciliation materialize records, copies and JSON hashes in memory. Row caps do not bound bytes per field, total memory, scan cost or duration. Catalog lookup rehashes artifacts across discovered vintages. This is not a streaming bulk loader. |
| Archive growth | 16 KiB per outcome, at most 10000 outcomes per archive; history validates the entire archive before `--last` filtering. New attempts fail when full. No automatic retention or compaction. |
| Timeouts | CLI connection/selection timeouts are 10 seconds; MongoDB socket timeout is 30 seconds. SQL query/lock deadlines and whole-workflow deadlines are operator-owned. |
| Persistence | Trusted local POSIX filesystem with advisory locks, hard links and directory fsync. Sink writes and local evidence are not a distributed transaction; a killed process can leave writes without evidence. |
| Concurrency | One controlled writer per source and selected sink vintage. The archive lock serializes manifest publication, not database writes. Reconciliation proves the checked observation, not a durable snapshot against later writers. |
| Identity | Hashes establish integrity, not authenticated provenance or server/operator identity. Archive records name the logical sink/table, not its physical database. Operators must retain the attempt-to-target association. A historical UUID retry never rechecks the current sink. |
| Dependencies | Server image versions are pinned; Python dependency ranges and the base image's OS packages are not locked to immutable digests. Reproduction guarantees assume the same resolved versions; a future install can differ. |

## Outside the verified scope

Production adoption still requires deployment-specific authentication/TLS,
least-privilege roles and schema/index management; backup/restore and retention;
resource sizing and deadlines; concurrency/isolation policy; and operational
monitoring. MongoDB batch atomicity, replica-set failover, secondary consistency,
cross-host evidence locking and signed attestations are not demonstrated.
Other MongoDB-compatible products are not certified by the MongoDB live suite.

The repository's dependency-alert snapshot on 2026-11-06 contains seven open
alerts (four high, three moderate), all in development dependencies recorded in
`javascript/yarn.lock`: `js-yaml`, `brace-expansion` and `@humanfs/node`. They
require a separate JavaScript tooling update; this Python publishing audit does
not certify the repository's overall dependency security. Python's unlocked
dependency ranges also require deployment-specific review and version control.

No tag, package publication, infrastructure deployment or release activation is
part of this assessment. The [unreleased notes](ons-pay-publishing-release-notes.md)
describe the delivered operator capabilities without changing the release lifecycle.
