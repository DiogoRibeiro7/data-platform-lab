# First real ONS publication to a Neon development branch

This is an opt-in runbook for publishing the existing real Office for National
Statistics (ONS) pay vintage to Neon PostgreSQL. It reuses the repository's
[`ons-pay-postgres` workflow](ons-pay-postgres-workflow.md), which already
validates approved registry handoffs, raw bytes, curated Parquet, transactionally
publishes both Gold tables, reconciles every record and records immutable
acceptance evidence. No new downloader or dataset definitions are introduced.

**No command in this document is executed by CI.** The two `--apply` steps below
change a database. Review the branch and source pins before running either.

## 1. Check the Neon branch on Windows

In PowerShell at the repository root:

```powershell
neon status
```

For the initial rollout, the branch must be `dev-data-platform` in project
`lively-thunder-53881970`, **not** `production`. Neon branches created under
this repository's policy expire after seven days. Check the reported `Expires At`
date before starting a long-running load or relying on the branch for storage.
The checkout normally refreshes the root `.env.local`. If it is missing,
`neon env pull` refreshes Neon-managed variables for the checked-out branch.
Never print, commit or paste this file: it contains database and Auth secrets.

## 2. Prepare a POSIX evidence environment

Publication evidence requires POSIX directory operations. Windows PowerShell
can run metadata-only previews, but **the real `ons-pay-postgres --apply` must
run in Linux/WSL2**, with Parquet, raw cache and evidence paths on a native
Linux filesystem (not the Windows `/mnt/g` mount). This preserves the existing
durability contract; the command intentionally refuses to apply on Windows.

From WSL2, enter the checked-out repository. The example assumes the Windows
repository is on `G:` and WSL exposes it under `/mnt/g`:

```sh
cd /mnt/g/github/data-platform-lab/python
poetry install --extras serving

# No database I/O: inspect the compiled migration plan.
poetry run data-platform-lab postgres-service --env-file ../.env.local
```

The Linux Poetry environment is separate from Windows Python. Supply a private
handoff path, a Linux data directory and a Linux raw-object directory. Do not
place these artifacts in Git. The existing
[registry handoff guide](registry-handoff-ingestion.md) explains how to obtain
a trusted handoff from `DiogoRibeiro7/data`; do not invent one from an unreviewed
URL or infer a new approval from a matching checksum.

## 3. Initialize the schema only after review

The repository's numbered migration creates the `dpl_curated` schema, two ONS
Gold tables, an immutable publication index and separate reader/writer group
roles. The Neon database owner needs sufficient SQL privileges to create these
objects and roles. Test first with a **read-only** readiness query:

```sh
# Expected to report not ready on a new, empty development database.
poetry run data-platform-lab postgres-service \
  --env-file ../.env.local --verify
```

After reviewing the [migration SQL](../python/src/data_platform_lab/serving/sql/0001_curated_dataset_service.sql)
and confirming that the connection belongs to `dev-data-platform`, opt in:

```sh
poetry run data-platform-lab postgres-service \
  --env-file ../.env.local --apply

poetry run data-platform-lab postgres-service \
  --env-file ../.env.local --verify
```

The administrative CLI accepts **only** the direct `DATABASE_URL_UNPOOLED`
from this explicit file; it never falls back to Neon's pooled URL. An explicit
`DPL_POSTGRES_SERVICE_DSN` process variable overrides the file. Migrations are
transactional and recorded by digest; they do not delete existing data.
If the migration cannot create the group roles or fails a permission check,
stop and investigate the Neon owner's privileges. Do not weaken readiness
checks, grant broad rights to a reader or wipe the branch to force success.

## 4. Preview and publish the approved ONS source

The ONS vintage below is the same real snapshot recorded in the existing
PostgreSQL workflow's [execution report](ons-pay-postgres-workflow.md#observed-genuine-data-execution):

| Field | Value |
| --- | --- |
| Dataset ID | `ons-gross-median-weekly-pay` |
| Registry commit | `f407638aa9795b0a71d3bfe92eaf8b3dd344d7ba` |
| Source SHA-256 | `285a76982afdfd38e9366535b43bf3e68c878ada7b819d722d2ec188c38eff17` |
| Previously verified tables | `geography_year`, `local_authority_yoy` |
| Previously observed local PostgreSQL rows | 93 and 5,276 respectively |

Use the reviewed private handoff for exactly that commit and source checksum.
In WSL, replace the three example paths with directories on the Linux filesystem:

```sh
registry_commit=f407638aa9795b0a71d3bfe92eaf8b3dd344d7ba
source_sha256=285a76982afdfd38e9366535b43bf3e68c878ada7b819d722d2ec188c38eff17
handoff=/home/YOUR_WSL_USER/dpl-neon/ons-handoff.json
data_root=/home/YOUR_WSL_USER/dpl-neon/curated
object_root=/home/YOUR_WSL_USER/dpl-neon/raw
attempt_id=$(poetry run python -c 'from uuid import uuid4; print(uuid4())')

args=(
  --handoff "$handoff"
  --registry-commit "$registry_commit"
  --source-sha256 "$source_sha256"
  --dataset-id ons-gross-median-weekly-pay
  --data-root "$data_root"
  --object-root "$object_root"
  --target-id neon-dev-ons
  --attempt-id "$attempt_id"
  --reuse-verified
  --env-file ../.env.local
)

# Preview is metadata-only: no HTTP, DB connection or filesystem writes.
poetry run data-platform-lab ons-pay-postgres "${args[@]}"

# After inspecting the preview, opt into a bounded fetch and two-table publish.
poetry run data-platform-lab ons-pay-postgres "${args[@]}" --apply \
  > /home/YOUR_WSL_USER/dpl-neon/publication.json
```

Use the same `attempt_id` when rerunning an identical accepted publication. A
new attempt needs a new UUID; do not overwrite or fabricate acceptance evidence.
The `--env-file` flag reads PostgreSQL URLs only during `--apply`. Within the
file, the direct URL is preferred. Process variables `ONS_PAY_POSTGRES_DSN`,
`DATABASE_URL_UNPOOLED` and `DATABASE_URL`, in that order, override the file;
check for stale or pooled overrides before a Neon run.

For least privilege outside this first development-branch trial, create
separate writer and reader logins and configure private libpq service entries,
as described in [persistent PostgreSQL datasets](persistent-postgres-datasets.md).
Do not use the database owner as an ordinary production application identity.

## 5. Independent read-only reconciliation

The successful JSON summary contains the two stable table attempt IDs, count
and content fingerprints. Preserve it outside Git. Verify each table against
its own accepted baseline using the same Linux `data_root`:

```sh
for table in geography_year local_authority_yoy; do
  baseline=$(poetry run python -c 'import json,sys; print(json.load(open(sys.argv[1]))["attempts"][sys.argv[2]])' \
    /home/YOUR_WSL_USER/dpl-neon/publication.json "$table")
  poetry run data-platform-lab ons-pay-publish \
    --env-file ../.env.local --data-root "$data_root" --model-version 1 \
    --registry-commit "$registry_commit" --source-sha256 "$source_sha256" \
    --table "$table" --sink postgresql --target-id neon-dev-ons \
    --verify --baseline-attempt-id "$baseline" || exit "$?"
done
```

Acceptance is not a production release. The Neon branch is temporary. For a
long-lived data service, design a separately approved production migration,
reader/writer permissions, backups, data-retention policy and repeatable
rebuild procedure. Do not copy branch credentials into CI or grant a coding
agent access to production merely to automate this smoke test.
