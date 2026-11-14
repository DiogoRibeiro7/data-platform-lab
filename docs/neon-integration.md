# Neon PostgreSQL integration

The repository's ONS pay publishing CLI can publish a pinned Gold vintage to
Neon's PostgreSQL service using its existing `psycopg` driver. Neon Auth may be
provisioned alongside PostgreSQL, but this repository currently has no user
login flow: enabling Auth does not add a login UI or change the publisher.

## Link the project and deploy

Run commands from the **repository root**, where `neon.ts` and `package.json`
live. Install Node.js and the Neon CLI first if needed. For the existing
project, the setup commands are:

```sh
npm install
npm i -g neon@latest
neon init -y --org-id org-rapid-bar-97006726 --project-id lively-thunder-53881970 --branch production --service postgres --service auth
neon config plan
neon deploy
```

Use separate `--service` flags for compatibility with Windows PowerShell.
`neon deploy` applies the checked-in `neon.ts` and writes the branch-scoped
connection settings into the root `.env.local`. It does **not** wire the Python
publisher or implement Auth in the application. The generated `.env.local`
and `.neon` context are git-ignored; never commit or paste their contents.

The Neon-generated variables include:

| Name | Purpose |
| --- | --- |
| `DATABASE_URL_UNPOOLED` | Direct PostgreSQL URL; preferred for ONS Gold publishing, DDL and session settings |
| `DATABASE_URL` | Transaction-pooled PostgreSQL URL; fine for normal application requests |
| `NEON_AUTH_*` | Auth service settings; not consumed by the current ONS publisher |

If you have already run `neon init` and `neon deploy` in this directory, **do
not repeat them** just to run the Python publisher. The generated environment
file remains local to the machine where you deployed.

## Publish or verify an ONS vintage

From `python/`, install the project's dependencies and use the locally generated
file explicitly. Replace the example source pins with the values from an
existing, curated ONS vintage:

```sh
poetry install --extras serving

# Read-only reconciliation: the sink must already contain the requested vintage.
poetry run data-platform-lab ons-pay-publish \
  --env-file ../.env.local --data-root ../data --model-version 1 \
  --registry-commit COMMIT_SHA40 --source-sha256 SOURCE_SHA256 \
  --table local_authority_yoy --sink postgresql --verify

# Preview requires no credentials or database connection; omit --verify.
poetry run data-platform-lab ons-pay-publish \
  --data-root ../data --model-version 1 \
  --registry-commit COMMIT_SHA40 --source-sha256 SOURCE_SHA256 \
  --table local_authority_yoy --sink postgresql
```

The CLI also accepts connection settings injected by a job runner or shell.
For PostgreSQL the precedence is:

1. Non-empty `ONS_PAY_POSTGRES_DSN`, `DATABASE_URL_UNPOOLED`, `DATABASE_URL`
   from the process environment, in that order.
2. If `--env-file` is specified, `DATABASE_URL_UNPOOLED` then `DATABASE_URL`
   from that file.

Only the two PostgreSQL URLs are read from the file. No Auth tokens are loaded.
No secret values appear in the CLI's JSON summaries or error messages. The
publisher still needs exact source pins, and it never writes without an
explicit `--apply`. `--verify` performs read-only reconciliation; it reports
drift rather than repairing the database. A fresh empty Neon database cannot
pass this reconciliation until a vintage has been deliberately published.

**Use a Neon development branch for the first write.** This project's initial
link targets `production`, so a careless `--apply` would mutate production
tables. Neon supports isolated copy-on-write branches:

```sh
# Run from the repository root, after linking the project.
neon checkout dev-ons-pay --create
neon config plan
neon deploy
```

The checkout replaces the local branch context and refreshes `.env.local` for
that branch. Confirm the intended branch before publishing. Follow the
[portable publishing guide](ons-pay-portable-publishing.md) for explicit
`--apply`, evidence recording, conflict recovery and acceptance conditions.
The integration does not automatically schedule cloud writes or run migrations.

The separate `postgres-service` command manages privileged roles and numbered
schema migrations for the [persistent dataset service](persistent-postgres-datasets.md).
It requires either `DPL_POSTGRES_SERVICE_DSN` or an **explicit** `--env-file`
pointing to a Neon file containing `DATABASE_URL_UNPOOLED`. Pooled connections
are never used for administrative migrations. No connection is opened for a
preview; `--apply` is always required to modify the schema.

The end-to-end `ons-pay-postgres` workflow also accepts `--env-file` with the
same PostgreSQL resolution order as `ons-pay-publish`. It preserves the existing
source pins, acceptance checks and mandatory POSIX evidence storage. See the
[Neon ONS runbook](neon-ons-pay-runbook.md) before executing it against the
temporary development branch. Do not run either administrative or publication
`--apply` against the linked production branch as a connection test.

## Claude Code and Claude Desktop

The Neon CLI can install the official Neon agent plugin at the user level,
including its MCP server and agent skills:

```sh
neon plugins -y --agent claude-code
```

For Claude Desktop, use `neon plugins -y --agent claude-desktop` instead.
If both clients are used, both flags may be supplied. Restart the relevant
Claude client so it discovers the plugin. This plugin is a **tooling** setup;
it does not place the PostgreSQL URL into Claude's prompts or config. The
application's connection strings remain in `.env.local`. Do not paste them
into a chat, a coding-agent prompt or a tracked `CLAUDE.md` file.

Neon's official guidance: [CLI environment variables](https://neon.com/docs/cli/env),
[PostgreSQL connection pooling](https://neon.com/docs/connect/connection-pooling.md),
and [Claude Code agent setup](https://neon.com/docs/ai/agent-setup/claude-code).
