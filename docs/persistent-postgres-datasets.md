# Persistent PostgreSQL datasets

The existing Compose `postgres` service retains its named `postgres-data` volume
at `/var/lib/postgresql`. Its loopback port, PostgreSQL image, metadata schema and
development defaults are unchanged. The curated service adds numbered migrations,
reader/writer groups and a publication-evidence index in the same database.
It does not provision a hosted service or make the laptop remotely available.

## Initialize and apply migrations

```bash
# From the repository root; preserves an already initialized volume.
docker compose up -d --wait postgres
cd python
poetry install --extras serving

# Preview: packaged SQL metadata only, no connection/settings lookup or file writes.
poetry run data-platform-lab postgres-service

# Supply a private libpq service configuration or inject your administrator DSN.
export DPL_POSTGRES_SERVICE_DSN='service=dpl_local_admin'
poetry run data-platform-lab postgres-service --apply
poetry run data-platform-lab postgres-service --verify
```

For local setup, define `dpl_local_admin` in your private `~/.pg_service.conf` with
`host=127.0.0.1`, `port=5432`, `dbname=data_platform_lab`, `user=data_platform_lab`.
Supply its password through your private libpq password file or environment. Use
the credentials configured when the existing volume was initialized; changing
Compose environment values does not rotate an existing database password. Keep
the existing development identity confined to the local stack. Other environments
need independently supplied administrator credentials. Never commit connection or
password files.

`DPL_POSTGRES_SERVICE_DSN` has no default and is read only for apply/verify.
Python callers can inject settings through `postgres_service_cli.main`, or pass
an idle psycopg connection to `apply_migrations`/`verify_service`. Borrowed handles
stay open; active transactions and autocommit are rejected. CLI-owned connections
are closed after use and have a 10-second connection timeout.

Migrations require an administrator able to create roles/schema and grant access
to the existing ONS tables. Numbered SQL lives in
`python/src/data_platform_lab/serving/sql/`. Digests are recorded in
`dpl_curated.schema_migrations`; do not edit applied SQL or its bootstrap DDL.
Add a new numbered migration and extend the manifest for a later change.
Application uses one transaction, a nonblocking advisory transaction lock,
30-second statement timeouts and 5-second lock timeouts. These are not whole-run
deadlines. Rerunning a current migration reports an empty `applied` list.

Unknown versions, edited digests, incompatible tables and unsafe group privileges
fail without resetting data or automatically repairing permissions. Inspect
readiness and database configuration before retrying. A connection loss around
commit requires a fresh `--verify` before an explicit retry. Do not erase migration
history to bypass a mismatch.

## Bootstrap separate local logins

The migration creates NOLOGIN groups without application passwords:

| Group | Service privileges |
| --- | --- |
| `dpl_curated_writer` | SELECT/INSERT/UPDATE on ONS tables, SELECT/INSERT on publication references, SELECT on migration history. |
| `dpl_curated_reader` | SELECT on ONS tables, publication references and migration history; no service-table writes or schema creation. |

The writer also has CREATE on `public`: the existing publisher issues
`CREATE TABLE IF NOT EXISTS` before upserting. Treat it as a trusted operator role.
Neither group receives `pipeline_runs` access; existing metadata credentials keep
that responsibility. Only administrators can modify the migration ledger.

Open the existing local administrator's psql session and create each login once:

```bash
docker compose exec postgres psql -U data_platform_lab -d data_platform_lab
```

```sql
CREATE ROLE dpl_local_writer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS IN ROLE dpl_curated_writer;
CREATE ROLE dpl_local_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOREPLICATION NOBYPASSRLS IN ROLE dpl_curated_reader;
\password dpl_local_writer
\password dpl_local_reader
```

The password prompts avoid putting chosen passwords in SQL history. If the logins
already exist, inspect their memberships instead of recreating them. Rotate passwords
only as an explicit operator action. Give a reader only its reader group, with no
ownership or elevated memberships: PostgreSQL privileges are additive. Readiness
checks group roles and service-object permissions, not every login/function in the
database. Other role memberships can invalidate a read-only identity.

Create private libpq service entries for the writer and reader. For example:

```ini
[dpl_local_writer]
host=127.0.0.1
port=5432
dbname=data_platform_lab
user=dpl_local_writer
options=-c search_path=public
```

Use `ONS_PAY_POSTGRES_DSN='service=dpl_local_writer'` with the existing
[`ons-pay-publish --apply`](ons-pay-portable-publishing.md) command. Use the reader
service for `--verify`, retaining exact source pins and an accepted baseline.
Publication acceptance, immutable evidence and manual release controls are unchanged.

## Readiness, restart and safe shutdown

```bash
docker compose ps postgres
docker compose exec -T postgres pg_isready -U data_platform_lab -d data_platform_lab

# From python/, verify application readiness through the reader identity.
DPL_POSTGRES_SERVICE_DSN='service=dpl_local_reader' \
  poetry run data-platform-lab postgres-service --verify

docker compose restart postgres
docker compose up -d --wait postgres
# Repeat the same --verify command after restart.

# Retains postgres-data; start again with up -d --wait postgres.
docker compose stop postgres
```

`pg_isready` checks server availability. `postgres-service --verify` uses a read-only
transaction to inspect migration hashes, required relations, column/primary-key
contracts and group permissions. It does not create acceptance or reconcile data
contents. Use existing pinned publication verification for that separate check.

| Outcome | Exit/output |
| --- | --- |
| Preview or successful apply | `0`, JSON plan or applied migration versions. |
| Ready / not ready | `0` / `1`, JSON with named readiness checks. |
| Connection, migration or permission failure | `1`, credential-safe stderr diagnostic. |
| Invalid options or missing connection setting | `2`, stderr diagnostic. |

Do not use `docker compose down -v`, volume pruning or reset scripts on the
persistent stack. The integration test removes volumes only inside its own
randomly named Compose project.

## Publication index and persistence boundary

`public.ons_pay_geography_year` and `public.ons_pay_local_authority_yoy` retain their
names, types, null semantics, stable record IDs and full vintage pins.
`dpl_curated.publications` can index an accepted attempt's dataset, table, target,
registry/source pins, model version, count, plan/sink fingerprints, publication
time and immutable evidence path/digest. It has no active-release or `latest` pointer.

The [`ons-pay-postgres` workflow](ons-pay-postgres-workflow.md) registers both Gold
tables only after validating accepted immutable evidence and reconciling current
rows. The single-table `ons-pay-publish` CLI does not populate the index. An index
row alone cannot establish acceptance or current health. Source approvals,
licensing and canonical provenance remain in `DiogoRibeiro7/data`.
The [research reader](ons-pay-postgres-research.md) validates those references and
serves exact-vintage PostgreSQL queries using a dedicated read-only login.

PostgreSQL rows, roles and migration history survive restarts in the named volume.
Raw S3 objects, local Parquet, receipts and immutable evidence have separate
storage/recovery requirements. A database evidence path does not copy those files
or make them remotely reachable. Off-host backups, TLS/authentication, secret
rotation, upgrades and remote deployment are still operator decisions; local
persistence is not production certification.

## Isolated restart proof

On a POSIX host with Docker Compose and locked serving dependencies:

```bash
cd python
DPL_POSTGRES_SERVICE_LIVE=1 poetry run pytest -q tests/test_postgres_dataset_service_live.py
```

The fixture generates its own password, project and named volume, binds a random
loopback port and accepts no external DSN or remote Docker context. It publishes
both synthetic Gold tables using current adapters/gates, records immutable evidence,
restarts the same container, then reconciles through the reader identity. It checks
the pre-existing `pipeline_runs` record, migration idempotency, denied reader writes
and failure on privilege/history drift. Cleanup targets only fixture-owned resources.
The PostgreSQL CI workflow runs this alongside existing metadata/publishing checks.

When running the test client inside Docker Desktop with access to the local Docker
socket, `DPL_PG_TEST_CONTAINER_CLIENT=1` selects `host.docker.internal` for that
fixture's random published port. It still cannot select an external database.
