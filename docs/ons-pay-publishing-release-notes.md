# ONS pay publishing — unreleased notes

## Capabilities

- Preview, explicitly publish, or independently verify either ONS Gold table
  using `data-platform-lab ons-pay-publish`. Every operation requires the model
  version, full registry commit, source SHA-256, table and sink.
- Publish deterministic relational records to PostgreSQL or nested documents
  through optional PyMongo, using verified Silver/Gold source artifacts.
- Set PostgreSQL statement and lock timeouts with `--statement-timeout-ms` and
  `--lock-timeout-ms`. Limits cover writes and post-commit read-back; cancellation
  withholds acceptance and supports explicit recovery.
- Reconcile the complete selected vintage and require matching identities,
  counts, values and independently computed fingerprints before acceptance.
- Opt into immutable local publication outcomes with `--apply --record-evidence`;
  inspect accepted, rejected and failed attempts using
  `data-platform-lab ons-pay-publication-history`.
- Run the complete synthetic workflow with
  `python scripts/ons_pay_publishing_demo.py --exercise-recovery`, retaining
  readable reports, source artifacts, plans and durable outcomes.

## Reliability and validation

PostgreSQL failures now report uncertain commit outcomes accurately: losing a
commit acknowledgment can leave writes even if a subsequent rollback succeeds.
No complete write receipt or accepted outcome is created for that failure.
Regression tests cover the persisted failure and a new explicit recovery attempt.

The regular PostgreSQL Compose service uses an init process and a direct
`pg_isready` probe to prevent orphaned health-check exits from restarting the
database. The offline CI suite covers Python 3.11–3.14 with separate interpreter
caches. Python/platform workflows run on PRs and `main` pushes, avoiding duplicate
feature-branch runs. Live database tests and the packaged walkthrough remain
separate checks of driver behavior and installation/orchestration.
The malformed-policy test accepts safe rejection at either JSON decoding or
schema validation, accommodating interpreter differences in nesting limits.

## Operator contract and limitations

Preview is the default and opens no sink connection. `--verify` is read-only and
does not fabricate acceptance. `--apply` can leave writes after an error:
PostgreSQL commits before read-back, and MongoDB replacements are individually
atomic rather than an atomic batch. Extra sink records are never automatically
deleted. Inspect the source/target, then explicitly replay the same pins.

Reusing a recorded attempt UUID returns historical evidence without database
I/O; a new observation or recovery requires a new UUID. Evidence needs a trusted
POSIX filesystem and does not authenticate the writer or physical database.
Source/record fingerprints describe integrity at observation time.

Supported Python versions are 3.11–3.14. MongoDB remains optional. The Docker
demo needs Linux containers and downloads images/dependencies during setup,
but no dataset or hosted credentials. There is no automatic release activation,
schema migration, production deployment or change to the manual release pointer.
The package version remains unchanged; these notes do not create a release.

See the [readiness assessment](ons-pay-publishing-readiness.md) for verification
evidence, resource limits, supported local paths and production gaps.
