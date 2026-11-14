# Read accepted PostgreSQL datasets from research clients

The existing ONS API now has an optional PostgreSQL backend. Its default remains
the pinned Parquet catalog. The same discovery, geography/year, area trend and
manual release routes are retained; data queries always require exact source pins.
The server owns validation and publication evidence. A research client only needs
the HTTP contract or read-only SQL access, without running ingestion itself.

## Start the local reader API

Initialize the [persistent service](persistent-postgres-datasets.md), create a login
with only `dpl_curated_reader` membership, and publish both Gold tables with the
[explicit ONS workflow](ons-pay-postgres-workflow.md). Keep its trusted local data
directory, including Parquet, curation receipts and immutable publication evidence.
The PostgreSQL index is a reference to those artifacts, not an approval registry.

```bash
cd python
poetry install --extras serving
export ONS_PAY_POSTGRES_READER_DSN='service=dpl_local_reader'
poetry run data-platform-lab ons-pay-serve \
  --backend postgresql --data-root /absolute/private/ons-curated \
  --target-id local-curated --host 127.0.0.1 --port 8000
```

The reader setting is deliberately separate from writer settings. It does not
fall back to `ONS_PAY_POSTGRES_DSN`, `DATABASE_URL`, `.env.local` or Neon owner
credentials. Operators can inject a dedicated reader's PostgreSQL URL, including
a Neon reader URL, into this variable. The existing publishing configuration is
unchanged. No hosted database is contacted by tests or app construction.

Python embedders can call `create_app(data_root, backend="postgresql",
postgres_dsn=reader_dsn, target_id="local-curated")`. Configuration validation is
inert; the first data request opens a connection. `/health` reports process
liveness only. Missing settings prevent startup; unavailable/unsafe data returns
503 without revealing credentials, SQL or paths.

Every database operation starts a `REPEATABLE READ, READ ONLY` transaction, sets
transaction-local schema and 30-second statement / 5-second lock timeouts, and
rolls back before closing. Connection establishment is capped at 10 seconds.
Transaction-local settings avoid depending on pooled session state; see the
[Neon pooling contract](https://neon.com/docs/connect/connection-pooling) and
[psycopg transaction behavior](https://www.psycopg.org/psycopg3/docs/basic/transactions.html).
No Neon infrastructure, roles, schema or release pointer is created by this reader.

## Discovery and page contract

| Route | Contract |
| --- | --- |
| `GET /v1/ons-pay/vintages` | Accepted vintages for the configured target, deterministically ordered by model, registry commit and source hash. |
| `GET /v1/ons-pay/geography-year` | Exact `registry_commit`, `source_sha256`, model 1; optional geography/pay year; ordered by geography, year and record ID. |
| `GET /v1/ons-pay/areas/{area_code}/trend` | Same exact pins; optional inclusive start/end year; ordered by year, area and record ID. |
| `GET /v1/ons-pay/releases/active` | Existing manually activated pin, validated against indexed acceptance; never activates or selects a newer vintage. |
| `GET /openapi.json` | Machine-readable response and parameter schemas, including PostgreSQL publication metadata. |

`limit` is 1–100 (default 50), `offset` is 0–100000, and years are 1900–2100.
Filters, pins, limits and offsets are bound SQL parameters. Only fixed table,
column and ordering definitions enter SQL text. There is no arbitrary SQL endpoint.
Pages retain the existing `vintage`, `total`, `limit`, `offset` and `rows` fields;
area pages also retain `area_code`. NULLs remain JSON null, dates remain ISO dates,
and numeric values retain the existing PostgreSQL/Parquet contract.

PostgreSQL vintage objects include every existing attribution/licence field plus:

| Field | Type / meaning |
| --- | --- |
| `dataset_id` | `ons-gross-median-weekly-pay` |
| `target_id` | Explicit operator-selected publication target label |
| `tables[].table` | `geography_year` or `local_authority_yoy` |
| `tables[].grain` | Natural key fields from the existing publishing contract |
| `tables[].first_year`, `last_year` | Inclusive observation-year extent; null for an empty table |
| `tables[].records_accepted` | Validated exact-vintage row count |
| `tables[].source_plan_sha256`, `sink_snapshot_sha256` | Source plan and accepted sink content fingerprints |
| `tables[].evidence[]` | Every equivalent accepted `attempt_id`, `evidence_sha256`, `accepted_at` reference |

Discovery is bounded to 1000 publication references per target. Every returned
vintage requires both tables, matching immutable evidence, exact source pins and
matching curated source plans. Multiple attempts are allowed only when each proves
the same accepted content; all references are returned, with no latest selection.
Conflicting or malformed references fail closed. An entirely absent vintage is
404; incomplete or inconsistent acceptance is 503. Invalid HTTP selections are 422.

Discovery establishes historical acceptance, not current database health. Each
data query independently reconciles the **entire selected table vintage**, then
counts and fetches the requested page inside that same database snapshot. Missing,
extra, duplicate or changed rows fail 503, including changes outside the requested
page/filter. A concurrent write cannot slip between reconciliation and page reads.
This favors validation over throughput: the server may read up to 100001 rows for
reconciliation, even for a small page. It is intended for these bounded research
datasets; no unverified cache or high-throughput claim is introduced.

The server still needs the local evidence and Parquet directory. PostgreSQL alone
does not yet supply every provenance artifact needed to establish acceptance.
Source approvals and licensing remain in `DiogoRibeiro7/data`.

## Python consumer in an independent process

[`scripts/ons_pay_reader.py`](../scripts/ons_pay_reader.py) uses only the standard
library. Its typed `AcceptedVintage`, `GeographyYearRow` and `GeographyYearPage`
records retain source and acceptance fingerprints. It bounds HTTP responses,
checks response identities and fails nonzero on invalid/unavailable data.

```bash
# Run from any directory; no platform installation, DB credentials or data root.
python /path/to/data-platform-lab/scripts/ons_pay_reader.py \
  --base-url http://127.0.0.1:8000

# Choose one discovered identity explicitly; save output outside this repository.
python /path/to/data-platform-lab/scripts/ons_pay_reader.py \
  --base-url http://127.0.0.1:8000 --target-id local-curated \
  --registry-commit "$registry_commit" --source-sha256 "$source_sha256" \
  --limit 50 --offset 0 > research-page.json
```

The first command only lists identities. It never picks a default vintage.
The live integration test runs this script with Python isolated mode (`-I`) in a
separate process, strips database/cloud settings, and queries an actual loopback
HTTP server backed by fixture-owned PostgreSQL. Reads leave data/evidence unchanged.

## Direct SQL and R/DBI

With a dedicated reader service, this psql query demonstrates bounded access:

```sql
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30000ms';
SELECT geography, pay_year, areas_total, median_area_weekly_pay_gbp,
       registry_commit, source_sha256
FROM public.ons_pay_geography_year
WHERE model_version = 1
  AND registry_commit = :'registry_commit'
  AND source_sha256 = :'source_sha256'
ORDER BY geography, pay_year, record_id LIMIT 100;
ROLLBACK;
```

[`scripts/ons_pay_reader.R`](../scripts/ons_pay_reader.R) uses DBI/RPostgres,
parameterized filters and a private libpq service. Install those packages in the
consumer's R environment, then run:

```bash
export ONS_PAY_PG_SERVICE=dpl_local_reader
Rscript /path/to/data-platform-lab/scripts/ons_pay_reader.R \
  "$registry_commit" "$source_sha256" local-curated
```

It checks the reader identity and the two publication references, prints their
fingerprints, and returns at most 100 geography/year rows. SQL/R clients trust the
database index; they do not reproduce the server's archive validation and full
reconciliation. Use the API for that stronger read contract, or perform the
documented baseline verification before direct SQL analysis. RPostgres accepts
the **service name**, not a connection URL in `dbname`; see its
[connection reference](https://rpostgres.r-dbi.org/reference/Postgres.html).
The R script is syntax-checked; the independent process integration proof is Python.

## Permissions, failures and deployment limits

The reader rejects superuser/role-admin/database-admin/replication/bypass-RLS
identities, writer membership, schema/database CREATE, and effective table or
column write privileges on service relations. PostgreSQL also enforces a read-only
transaction. Provision the login with only the reader group; these checks are not
a general audit of every inherited role, function or other database object.

There is no HTTP authentication or TLS termination. The PostgreSQL CLI backend
refuses non-loopback hosts. Do not expose an embedded app publicly without adding
TLS, authentication, authorization and rate limiting. Database credentials remain
on the server. Remote hosting and always-on availability are separate work.

Earlier `ons-pay-postgres` indexes could record the incorrect directory
`manifests/ons_pay_publish` while the archive correctly used
`manifests/ons_pay_publication`. New publications derive their reference from the
archive itself. The reader rejects old incorrect paths. For an existing affected
index, first verify each exact accepted attempt using `ons-pay-publish --verify
--baseline-attempt-id`; then have the administrator correct only that verified
row's `evidence_path` to its actual relative archive path. Do not rewrite evidence,
drop rows, or bypass checks. The reader and writer never silently repair old references.

CI uses synthetic committed registry fixtures and isolated PostgreSQL projects.
It covers parity, independent consumption, different vintages, NULLs, filter
injection attempts, pagination, malformed/missing evidence, index conflicts,
source drift, denied writer identities, column grants, outages and concurrent writes.
No source download or hosted database is required:

```bash
poetry run pytest -q tests/test_ons_pay_postgres_serving.py
DPL_POSTGRES_SERVICE_LIVE=1 poetry run pytest -q tests/test_ons_pay_postgres_serving_live.py
```

An opt-in genuine-data run on 2026-11-14 at 11:10 UTC reused checksum-verified ONS
bytes from registry commit `f407638aa9795b0a71d3bfe92eaf8b3dd344d7ba`, source SHA-256
`285a76982afdfd38e9366535b43bf3e68c878ada7b819d722d2ec188c38eff17`. Discovery validated
93 geography/year and 5276 local-authority records. The API returned all 93
geography/year rows and an area trend; a separate isolated Python process consumed
a two-row page with the selected acceptance fingerprints. Source and evidence files
were unchanged by the reads. The disposable PostgreSQL service was removed after
the run, and the full report remains outside Git; this does not claim a shared
remote service is running.
