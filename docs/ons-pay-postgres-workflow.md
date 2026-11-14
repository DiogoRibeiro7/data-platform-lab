# Publish a pinned ONS vintage to PostgreSQL

`ons-pay-postgres` connects the existing registry ingestion, Silver/Gold curation,
publication gate and immutable evidence to the [persistent PostgreSQL
service](persistent-postgres-datasets.md). It publishes both Gold tables with model
version 1. Source approval and licensing remain in `DiogoRibeiro7/data`; there is
no discovery of newer sources or automatic release activation.

For a hosted Neon development branch, the workflow can read the direct
`DATABASE_URL_UNPOOLED` from an explicitly supplied `--env-file` during
`--apply`. The schema migration command accepts the same option. Both require
reviewed operator actions; previews never open the file. Follow the separate
[Neon real-data runbook](neon-ons-pay-runbook.md) for the Windows/WSL boundary,
temporary branches and secret-handling rules.

## Explicit operator execution

Use a POSIX filesystem for durable evidence (Linux/WSL), install the locked
`serving` extra, and initialize the service and separate writer/reader logins using
the service runbook. Keep the trusted registry handoff outside Git. Its provenance,
licence and source pins must have been independently reviewed. Preview validates
handoff metadata; apply verifies the actual bytes and, for checkout mode, Git
identity. A matching hash alone is not a new source approval.

The following selects the previously approved ONS snapshot explicitly. Supply
your own private handoff path and absolute storage paths; directories need not
exist for preview. Run from `python/` in Bash:

```bash
registry_commit=f407638aa9795b0a71d3bfe92eaf8b3dd344d7ba
source_sha256=285a76982afdfd38e9366535b43bf3e68c878ada7b819d722d2ec188c38eff17
handoff=/absolute/private/ons-handoff.json
data_root=/absolute/private/ons-curated
object_root=/absolute/private/ons-raw
target_id=local-curated
attempt_id=$(python -c 'from uuid import uuid4; print(uuid4())')
args=(
  --handoff "$handoff"
  --registry-commit "$registry_commit"
  --dataset-id ons-gross-median-weekly-pay
  --source-sha256 "$source_sha256"
  --data-root "$data_root"
  --object-root "$object_root"
  --target-id "$target_id"
  --attempt-id "$attempt_id"
  --reuse-verified
)

# Metadata-only: no HTTP, storage/DB clients, credential reads or filesystem changes.
poetry run data-platform-lab ons-pay-postgres "${args[@]}"

# Explicit opt-in to the pinned HTTPS fetch and storage/database writes.
export ONS_PAY_POSTGRES_DSN='service=dpl_local_writer'
poetry run data-platform-lab ons-pay-postgres "${args[@]}" --apply > publication.json
```

Keep `publication.json` outside the repository. HTTPS uses the existing bounded
fetcher: 30-second socket timeout, one attempt, 16 MiB per object, 64 MiB total.
`--reuse-verified` rechecks cached bytes before reuse; corruption fails closed.
For a local pinned checkout, remove `--reuse-verified` and add
`--registry-root /absolute/registry-checkout`. Both local destinations must be
outside that checkout. To use S3 instead of `--object-root`, supply
`--backend s3 --bucket BUCKET` and optional `--endpoint-url`, `--region`,
`--key-prefix`; credentials use the existing SDK chain. Parquet and evidence remain
local even when raw storage is S3.

Apply checks service readiness before ingestion, validates both curated plans,
then uses the existing publisher for each table. Session settings fix the schema
to `public` and default to 30-second statement / 5-second lock timeouts; override
with `--statement-timeout-ms` / `--lock-timeout-ms`. These are per-operation limits,
not an overall run deadline. Schema migrations are never applied automatically.

`--attempt-id` is a namespace UUID: the JSON `attempts` map reports the two
deterministic UUIDv5 publication IDs. Preserve that map for independent verification.
Repeating the same command reuses verified raw/Parquet bytes and accepted evidence,
then **reconciles current database rows again**. Once both tables reconcile, their
references are inserted atomically into `dpl_curated.publications` under table
locks. Existing conflicting index entries fail; they are never silently replaced.
Evidence paths are relative to `data_root`, and digests identify immutable files.

The two existing table publications are separate transactions. If the second
fails, the first can retain valid per-table acceptance; the failed table cannot
be accepted, the workflow exits nonzero, and no new pair of index entries commits.
Earlier accepted index entries remain historical observations, not a guarantee of
current health. After a rejected/failed attempt, inspect receipts and use a new
namespace UUID for a deliberate retry; failed evidence is immutable. A crash or
connection loss may leave rows or evidence already persisted. Reconcile before
retrying. The JSON status is `published` only after both references commit and the
connection closes. No batch-level acceptance or release pointer is invented.

Exit 0 means successful preview/publication; 2 means preflight failure; other
nonzero exits indicate execution, reconciliation or evidence failures. Diagnostics
omit nested exception text because drivers may include credentials or row values.
Per-table results and existing refresh/publication receipts retain safe details.

## Independent read-only verification and query

Use a private libpq service entry for a login with **only** `dpl_curated_reader`
membership, including `options=-c search_path=public`. Read the exact table attempt
IDs from the successful output; do not select a latest attempt:

```bash
export ONS_PAY_POSTGRES_DSN='service=dpl_local_reader'
for table in geography_year local_authority_yoy; do
  baseline=$(python -c 'import json,sys; print(json.load(open(sys.argv[1]))["attempts"][sys.argv[2]])' \
    publication.json "$table")
  poetry run data-platform-lab ons-pay-publish \
    --data-root "$data_root" --model-version 1 \
    --registry-commit "$registry_commit" --source-sha256 "$source_sha256" \
    --table "$table" --sink postgresql --target-id "$target_id" \
    --verify --baseline-attempt-id "$baseline" || exit "$?"
done
psql 'service=dpl_local_reader' -v ON_ERROR_STOP=1 \
  -v registry_commit="$registry_commit" -v source_sha256="$source_sha256"
```

```sql
SELECT geography, pay_year, areas_total, areas_with_pay, areas_without_pay,
       median_area_weekly_pay_gbp
FROM public.ons_pay_geography_year
WHERE model_version = 1
  AND registry_commit = :'registry_commit'
  AND source_sha256 = :'source_sha256'
ORDER BY geography, pay_year;

SELECT table_name, records_accepted, source_plan_sha256, sink_snapshot_sha256,
       attempt_id, evidence_sha256
FROM dpl_curated.publications
WHERE dataset_id = 'ons-gross-median-weekly-pay' AND model_version = 1
  AND registry_commit = :'registry_commit'
  AND source_sha256 = :'source_sha256'
ORDER BY table_name, published_at;
```

Verification checks the accepted archive baseline before opening the database,
then reconciles the entire exact vintage, including NULLs and unexpected rows.
Missing, changed, unexpected or ambiguous rows/evidence fail nonzero. It does not
fetch raw sources, write rows, create acceptance evidence or activate a release.
The local Parquet, receipts and evidence are still required for this verification.
Repeat it after `docker compose restart postgres`. Preserve the named volume.

## Tests and opt-in real-source smoke

Ordinary CI uses only bounded synthetic fixtures. The existing isolated PostgreSQL
test service generates credentials and a separate Compose project/volume; it
does not accept an operator DSN:

```bash
poetry run pytest -q tests/test_ons_pay_postgres_workflow.py
DPL_POSTGRES_SERVICE_LIVE=1 poetry run pytest -q \
  tests/test_ons_pay_postgres_workflow_live.py
```

For a genuine-data smoke, explicitly execute the pinned commands above against
your initialized local service, repeat apply with the same namespace UUID, restart
the service, then run both reader verifications. Preserve the JSON output outside
Git with actual counts, source-plan/sink fingerprints and evidence digests. This
manual procedure is never part of PR CI. Do not commit raw source data, credentials,
handoffs or machine-specific receipts. It proves local persistence; remote hosting,
off-host backups and independent availability require separate operational work.

### Observed genuine-data execution

On 2026-11-14 at 10:40 UTC, the exact registry/source pins above were fetched over
HTTPS and published to a fixture-owned local PostgreSQL 18.6 named volume. Both
tables passed acceptance, repeat apply reused the same evidence, and independent
reader reconciliation passed after restarting the same container and volume.
The isolated database was removed after verification; this is an execution record,
not a claim that a shared service is currently running. Full receipts/evidence and
the report were retained outside Git.

| Gold table | Source / persisted / reconciled rows | Source-plan SHA-256 | Observed sink SHA-256 |
| --- | --- | --- | --- |
| `geography_year` | 93 / 93 / 93 | `1e2a033c68817d5d3a80c80faede7173f0c29027689d83773e6f6dcb73c85bd2` | `412c8fde2a83c868db323c5df384650f0a87302557bbce0d981638eebd391777` |
| `local_authority_yoy` | 5276 / 5276 / 5276 | `517e393050854c3fd4d1bb7594e9f5d937a3a84ef624fb3c515b49fa9b8387d6` | `b7772839ad1492f7943ff4364ff6384fda5a7a84b4821b080d3cd79a406a53d7` |

The reader query returned, for example, `Combined Authority or City Region`, 2023,
11 areas (11 with pay, 0 without), and median area weekly pay of GBP 539.90.
Exactly two publication references remained after repeat apply and restart. Each
observed sink fingerprint equalled its independently computed expected fingerprint.
