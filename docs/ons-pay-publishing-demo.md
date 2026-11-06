# Reproduce ONS pay publishing locally

This walkthrough takes a bundled **synthetic** ONS-compatible CSV through real
Git provenance verification, ingestion, Silver/Gold curation, PostgreSQL and
MongoDB publication, read-back reconciliation, acceptance and durable evidence.
The values and area names are invented. They are not official ONS statistics.

## Run the complete demonstration

From the repository root, with Python **3.11–3.14** and Docker Compose v2
available, run:

```bash
python scripts/ons_pay_publishing_demo.py --exercise-recovery
```

The host needs no Poetry environment or database clients. Docker builds the
Python runner and starts PostgreSQL **18.6** and MongoDB **9.0.4**. Initial setup
downloads public images and Python/OS packages; it downloads **no dataset** and
needs no hosted database account. Later builds reuse Docker's cache.

The runner installs its Python packages from the committed `python/poetry.lock`
with the `analytics` and `mongodb` extras. The image build checks that the lock
matches `pyproject.toml`; changing the lock invalidates the dependency layer.
Poetry 2.5.1 runs in an isolated build environment and is absent from the final
runtime image. OS packages and image digests remain outside the Python lock.

`--exercise-recovery` explicitly authorizes the scripted mutation, expected
gate rejection, and subsequent replay. Without that flag the demo performs
normal publication and an idempotent repeat, with no drift exercise. Unexpected
errors stop the run; there is no automatic retry or repair loop.

Each invocation creates a fresh `data/ons-pay-demo/run-<UUID>/` directory and
an independent Compose project. Rerunning the same command preserves the
earlier artifacts. Choose another output parent when needed:

```bash
python scripts/ons_pay_publishing_demo.py \
  --output-dir /tmp/ons-pay-walkthrough --exercise-recovery
```

Use Docker's local `default` context, as required by the launcher. On Windows,
Docker Desktop must be running Linux containers; a short output parent such as
`C:/t/ons-demo` also makes deeply pinned artifact paths easier to inspect.
The actual workflow runs on Linux because durable evidence uses POSIX locks,
hard links and directory fsync. The mounted output filesystem must support
those operations.

## Architecture

```mermaid
flowchart LR
    A[Bundled synthetic CSV] --> B[Deterministic local Git checkout + handoff]
    B --> C[Verify commit, blob, SHA-256 and bytes]
    C --> D[Local content-addressed object store]
    D --> E[DuckDB Silver + two Gold Parquet tables]
    E --> F[Exact-vintage relational and document plans]
    F --> G[Disposable PostgreSQL / MongoDB]
    G --> H[Complete vintage read-back and reconciliation]
    H --> I[Existing acceptance gate]
    I --> J[Immutable local publication evidence]
    J --> K[JSON walkthrough report]
```

The workflow does not activate a release or change the manual release pointer.

## What happens

1. **Prepare a synthetic source.** The packaged CSV contains eight observations,
   including one suppressed pay value. Fixed source bytes, Git identity, commit
   date and message produce the same source SHA-256 and registry commit across
   fresh runs. No remote is configured. The handoff uses the registry identifier
   required by the existing ONS contract; its synthetic commit and URI label
   do not represent an upstream release and are never fetched.
2. **Verify and ingest.** `ingest_handoff` checks the actual checkout HEAD,
   committed blob, source checksum, byte count and content-addressed destination.
   It writes a real ingestion receipt. The demo does not construct a substitute
   success receipt or import test helpers.
3. **Curate.** `curate_ons_pay` applies the existing quality gates and SQL models:
   eight Silver rows, five geography/year Gold rows and seven local-authority
   year-on-year Gold rows. Suppressed pay remains null. Parquet hashes and the
   curation receipt are retained.
4. **Plan.** `plan_ons_pay_publishing` independently verifies the source artifacts
   and creates four plans: each Gold table in relational and document form.
   Plans retain the model version, registry commit and source SHA-256.
5. **Publish and accept.** Existing adapters write to fresh disposable services.
   The reconciler reads the complete selected vintage and compares identities,
   counts, fields and fingerprints. `PublicationArchive.record` invokes the
   existing acceptance gate before storing an accepted outcome. Source artifacts
   are revalidated immediately before each write.
6. **Repeat.** An explicit second publication must produce the original counts
   and fingerprints. A new attempt UUID records this new observation; it is
   distinct from returning an old outcome using the same UUID.
7. **Exercise drift, when requested.** A new attempt writes the plan, then
   changes one synthetic field before read-back. The gate must reject it with
   exactly one changed record and no missing or unexpected IDs. Its durable
   outcome is `rejected`, with no acceptance object.
8. **Recover explicitly.** The requested exercise invokes publication again
   with the same exact plan and a new UUID. Whole-record replacement/upsert
   restores the original sink fingerprint and produces fresh accepted evidence.
   Earlier accepted and rejected records remain available.
9. **Inspect and clean up.** Read-only history validation checks the evidence
   archive. With recovery enabled there are **16 outcomes: 12 accepted and 4
   rejected**. Without recovery there are eight accepted outcomes. The launcher
   retains artifacts and removes its services, network and generated runner
   image tag on both success and failure.

## Read the result

The command prints JSON with the overall status, exact source pins, elapsed
time, cleanup status and paths to the artifacts and report. Exit `0` means
the requested walkthrough and service cleanup succeeded; exit `1` means setup,
execution or cleanup failed. Invalid launcher arguments exit `2`.

| Artifact inside `run-<UUID>/` | Purpose |
| --- | --- |
| `report.json` | Ordered stages, timings, source pins, row counts, integrity checks, fingerprints and acceptance summaries |
| `launcher.json` | Overall execution/cleanup status and the unique Compose project name |
| `work/registry/`, `work/handoff.json` | Real local synthetic Git history and canonical handoff |
| `work/object-store/`, `work/ingestion-receipts/` | Verified raw bytes and ingestion evidence |
| `work/curated/silver/`, `work/curated/gold/` | Content-addressed Parquet artifacts |
| `work/plans/` | Four complete deterministic JSON plans over synthetic records |
| `work/curated/manifests/ons_pay_publication/` | Content-addressed accepted/rejected/failed publication outcomes |
| `build.log`, `services.log`, `demo.log`, `database.log`, `cleanup.log` | Retained setup, execution and teardown diagnostics |

Each publication stage includes the existing machine-readable operator result
shape: `vintage`, `counts`, `fingerprints`, `reconciliation`, `acceptance` and
the durable evidence identity. A successful **controlled_drift stage** means
the rejection was expected and verified; its publication status is still
`rejected`. Inspect that nested status rather than interpreting the stage's
success as acceptance of modified data.

The report lists artifact paths relative to the output directory. Original
ingestion and curation receipts retain their container-local `/output/...`
paths. Source pins and plan/sink fingerprints are reproducible for the same
fixture and dependency versions; run UUIDs, timestamps, elapsed times and
evidence file hashes intentionally differ between independent executions.

## Isolation and limits

The dedicated Compose file has **no host ports**, uses disposable database
storage and an internal network, and never joins the regular platform stack.
The launcher ignores ambient Compose overrides and remote Docker targets;
the workflow does not load the operator's database DSNs or credentials.
Only the new run directory is mounted. No source directory or Docker socket
is mounted into the runner. Neither the runner nor cleanup deletes unrelated
local files or database state.

If setup fails before the runner starts, inspect `launcher.json` and the setup
logs; no processing report exists yet. If the workflow fails, inspect its final
failed stage and any failed publication evidence. A failure is not an instruction
to retry against an external database. Run the launcher again for a new isolated
exercise. An abrupt machine shutdown can prevent teardown; the exact project
name and Compose file in `launcher.json` identify the resources to inspect.

PostgreSQL commits before reconciliation. MongoDB replacements are individually
atomic, **not an atomic document batch**. The demo uses one writer, primary
reads and acknowledged writes; it does not demonstrate failover, secondary
consistency, concurrent-writer isolation or production deployment. Evidence
hashes protect content integrity, not writer authentication. See the
[portable publishing contract](ons-pay-portable-publishing.md) for those limits.

## Automated verification

Offline tests exercise deterministic source preparation, actual Git/checksum
verification, rejection of tampered fixtures, failure evidence, preservation
of previous output and cleanup after setup/execution/diagnostic failures.

The [demo CI workflow](../.github/workflows/ons-pay-demo.yml) builds the packaged
runner and executes the documented entry point twice: with recovery, then with
the default publication exercise. It checks matching source pins and plan
fingerprints, exact row counts, all four sink/table combinations, evidence
hashes, expected rejection/recovery, preservation of unrelated files and service
cleanup. Timings are reported without pass/fail performance thresholds.

```bash
# From an existing development environment with pytest:
ONS_PAY_LIVE_DEMO=1 python -m pytest -q python/tests/test_ons_pay_demo_live.py

cd python
poetry run pytest -q tests/test_ons_pay_demo.py tests/test_ons_pay_demo_launcher.py
```
