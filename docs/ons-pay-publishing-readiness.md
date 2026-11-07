# ONS pay publishing release readiness

Assessment date: **2026-11-06**. Scope: the Python ONS workflow from registry
handoff to persisted publication outcomes. This is a **portfolio-ready local
data engineering workflow**, based on the verification evidence below. It is
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

The audited implementation and tests at commit
`a273bf4c4e1781a0e08411b60627aa020772f01c` passed all eight workflows:

| Verification | Observed result |
| --- | --- |
| [Python matrix and quality checks](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111102) | **839 passed, 25 opt-in skips on each of Python 3.11, 3.12, 3.13 and 3.14**; both existing demo smokes passed on every version. Ruff/format and strict mypy passed (89 source files). |
| [PostgreSQL](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111149) | **6 live tests passed**, plus run-metadata smoke. |
| [MongoDB](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111351) | **18 live tests passed**, including partial-write recovery and immutable outcomes. |
| [Packaged ONS demo](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111135) | **1 smoke test passed**, executing the launcher twice. Recovery verifies 12 accepted outcomes and 4 expected rejections; [reports and evidence](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111135/artifacts/13496390498) are retained subject to CI artifact retention. |
| [S3](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111210), [Kafka](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111059), [Iceberg](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111145), [platform recovery](https://github.com/DiogoRibeiro7/data-platform-lab/actions/runs/42204111213) | All passed with disposable services. |

The matrix includes the two new SQL commit-acknowledgment regressions. Its first
3.14 run exposed a test-specific assumption about JSON decoder recursion: the
same invalid policy can instead reach schema validation. The corrected test
requires safe rejection and CLI exit 2 without a traceback on every version.
An existing Starlette/httpx deprecation warning remains; it did not fail tests.

Local targeted checks passed: 45 adapter tests and 18 malformed-policy cases,
Ruff/format, mypy using Linux platform semantics, documentation links and
whitespace validation. The merged demo had also been executed on Windows/Docker
Desktop, verifying artifact hashes, outcomes and cleanup inside Linux containers.
Native Windows is not presented as a POSIX persistence validation environment.

Historical
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

Since the audit, the operator CLI has added opt-in PostgreSQL statement and lock
timeouts, with live cancellation and post-commit read-back tests. The audit's
verification table above remains a historical snapshot. See the
[timeout controls](ons-pay-portable-publishing.md#postgresql-statement-and-lock-timeouts)
for scope and recovery behavior.

| Boundary | Current limit or assumption |
| --- | --- |
| Source plans | `--max-rows` is 1–100000; default 100000. Validation checks the complete selected table, rather than sampling. |
| Read-back population | At most 100000 records; readers request one additional record to detect overflow and fail closed. The source `--max-rows` setting does not lower this separate sink cap. |
| SQL write batch | 1–5000 rows per `executemany`, default 500; all batches share one transaction. MongoDB performs one acknowledged replacement per document. |
| Memory and scan cost | Ingestion loads object bytes; curation uses DuckDB; planning/reconciliation materialize records, copies and JSON hashes in memory. Row caps do not bound bytes per field, total memory, scan cost or duration. Catalog lookup rehashes artifacts across discovered vintages. This is not a streaming bulk loader. |
| Archive growth | 16 KiB per outcome, at most 10000 outcomes per archive; history validates the entire archive before `--last` filtering. New attempts fail when full. No automatic retention or compaction. |
| Timeouts | CLI connection/selection timeouts are 10 seconds; MongoDB socket timeout is 30 seconds. PostgreSQL `--statement-timeout-ms` and `--lock-timeout-ms` configure per-statement/per-lock limits on owned sessions, including post-commit read-back. Omitted flags preserve existing settings; whole-workflow deadlines remain operator-owned. |
| Persistence | Trusted local POSIX filesystem with advisory locks, hard links and directory fsync. Sink writes and local evidence are not a distributed transaction; a killed process can leave writes without evidence. |
| Concurrency | One controlled writer per source and selected sink vintage. The archive lock serializes manifest publication, not database writes. Reconciliation proves the checked observation, not a durable snapshot against later writers. |
| Identity | Hashes establish integrity, not authenticated provenance or server/operator identity. Archive records name the logical sink/table, not its physical database. Operators must retain the attempt-to-target association. A historical UUID retry never rechecks the current sink. |
| Dependencies | Python runtime/development dependencies and optional extras are pinned in `python/poetry.lock`, with artifact hashes and markers for Python 3.11–3.14. CI and the packaged demo validate and consume the lock. Server image tags, OS packages and Poetry bootstrap dependencies are not pinned to immutable digests by this lock. |

## Outside the verified scope

Production adoption still requires deployment-specific authentication/TLS,
least-privilege roles and schema/index management; backup/restore and retention;
resource sizing and deadlines; concurrency/isolation policy; and operational
monitoring. MongoDB batch atomicity, replica-set failover, secondary consistency,
cross-host evidence locking and signed attestations are not demonstrated.
Other MongoDB-compatible products are not certified by the MongoDB live suite.

The dependency-alert snapshot taken during the 2026-11-06 audit identified
seven JavaScript development-tooling alerts (four high, three moderate).
The subsequent [JavaScript dependency remediation](../javascript/README.md#dependency-security-checks)
updates `js-yaml`, `brace-expansion` and `@humanfs/node` to patched versions
and adds a recursive CI audit. This Python publishing assessment does not
certify the repository's overall dependency security. Python now uses a
committed lockfile; see the [dependency update workflow](local-development.md#dependency-lockfiles).
Locked versions still require security review and deliberate updates.

No tag, package publication, infrastructure deployment or release activation is
part of this assessment. The [unreleased notes](ons-pay-publishing-release-notes.md)
describe the delivered operator capabilities without changing the release lifecycle.
