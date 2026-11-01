# ONS pay operational monitoring and alert signals

The [refresh/backfill pipeline](ons-pay-refresh.md) now leaves audit receipts;
the [read-only serving catalog](ons-pay-serving.md) verifies immutable curated
outputs. This monitoring command checks both for **one explicitly selected
model version, registry commit and source SHA-256**. It works on a laptop,
without Supabase, a metrics server, or a cloud account.

The monitor is read-only. It does not promote the latest data, restart failed
jobs, fetch from ONS, write health files, or send external notifications.

After a vintage has been explicitly activated through the
[manual release lifecycle](ons-pay-release-lifecycle.md), the separate
[active-release assessor](ons-pay-release-health.md) can monitor that saved
pin and offer **non-executing** rollback advice. The original `ons-pay-monitor`
continues to require a caller-supplied exact pin and never selects a release
on behalf of a data-query client.

## Run a pinned health check

After [one audited refresh](ons-pay-refresh.md), copy the exact
`registry_commit` and `source_sha256` from the refresh receipt or
`GET /v1/ons-pay/vintages`:

```bash
cd python
poetry install --extras serving

poetry run data-platform-lab ons-pay-monitor \
  --data-root ../data \
  --registry-commit COMMIT_SHA40 \
  --source-sha256 SOURCE_SHA256 \
  --max-source-age-days 365 \
  --max-refresh-age-hours 30 \
  --max-missing-pay-percent 5 \
  --format json
```

Replace `COMMIT_SHA40` and `SOURCE_SHA256` with the real lowercase hexadecimal
pins, not a branch name. JSON includes `checked_at_utc`, the exact `vintage`,
overall `status`, individual checks (`pass`, `fail`, `skipped`), observed
metrics, configured thresholds, and the latest batch/selected-vintage success
run IDs. Unconfigured optional thresholds are explicitly **skipped**, never
treated as if they had been tested.

| Signal | What it verifies | Failure means |
| --- | --- | --- |
| `curation_integrity` | Successful curation receipts, exact pinned paths, output hashes, conflict-free replay | Missing/altered/untrusted Parquet or receipt |
| `table_contract` | All three Silver/Gold schema columns, Parquet row counts, row lineage pins, Silver reported-pay count | Table/lineage drift, even with an updated file hash |
| `source_freshness` | UTC calendar days since upstream `source.retrieved_at` | Source older than configured SLA or a future/invalid source date |
| `missing_pay_rate` | 100 × suppressed/unavailable observations ÷ source rows | A user-configured tolerance for missing pay was exceeded |
| `refresh_audit_integrity` | Every recorded refresh audit has the expected schema, identifiers, counts and timestamps | Missing, malformed, unsafe or contradictory audit |
| `last_batch` | Globally latest ONS pay refresh batch succeeded | Latest attempt failed, even when a previous vintage remains readable |
| `vintage_refresh_success` | Selected pin appears in a successful refresh batch | Requested snapshot was never successfully refreshed |
| `refresh_recency` | Hours since latest successful refresh **of that exact pin** | No such success or time since it exceeds SLA |

The source-age and refresh-age checks are different. A job can run successfully
every hour and still serve an old upstream snapshot. Conversely, a newly
retrieved source can have an overdue or failed refresh job. Both need separate
thresholds. The last-batch check is deliberately *pipeline-global* to signal
operational failures during other backfill items; the vintage recency check
always applies to the requested pin.

Missing ONS pay is sometimes intentionally suppressed (`x` or `u` upstream).
The percentage is an observable quality metric, **not a claim that those
values should be imputed or deleted**. Choose the tolerance in context;
the current ONS snapshot has 129 missing values out of 7,109 observations
(approximately 1.81%).

## Exit codes and numeric metrics

| Exit code | Meaning | Scheduler action |
| --- | --- | --- |
| `0` | All enabled checks passed; no required evidence missing | Continue normally |
| `1` | One or more integrity/freshness/refresh checks failed | Alert or stop downstream publication |
| `2` | Invalid command options or SLA thresholds | Correct configuration |

For a numeric scrape/export instead of JSON:

```bash
poetry run data-platform-lab ons-pay-monitor \
  --data-root ../data \
  --registry-commit COMMIT_SHA40 \
  --source-sha256 SOURCE_SHA256 \
  --max-source-age-days 365 \
  --max-refresh-age-hours 30 \
  --max-missing-pay-percent 5 \
  --format prometheus
```

This emits Prometheus-compatible gauge lines to standard output, including
`dpl_ons_pay_monitor_healthy`, `dpl_ons_pay_curation_integrity`,
`dpl_ons_pay_table_integrity`, `dpl_ons_pay_source_age_days`,
`dpl_ons_pay_missing_pay_percent`, `dpl_ons_pay_last_batch_success` and
`dpl_ons_pay_last_vintage_refresh_age_hours`. Each metric is labelled by the
exact model version, commit and source hash. If a value is unknown, its
metric is **omitted**, not silently encoded as zero. The overall health gauge
and CLI exit code still indicate failure. No scrape endpoint or telemetry
service is created.

For external observability, the output additionally provides
`dpl_ons_pay_monitor_checked_at_seconds` (stale-run detection),
`dpl_ons_pay_monitor_checks_failed`, and `dpl_ons_pay_monitor_checks_skipped`.
The [opt-in textfile exporter and alert/dashboard examples](ons-pay-alerting.md)
show how to connect these metrics to a collector without changing the default
read-only monitor or enabling an external service.

## Schedule explicitly, when appropriate

The repository includes a [cron template](examples/ons-pay-ops.crontab.example)
showing a refresh followed by a pinned monitoring check. Copy it to your
*own* scheduler only after you choose your paths, pins, thresholds, cadence,
and log retention policy. On a machine using an in-project Poetry virtual
environment, the example invokes the installed `data-platform-lab` executable
without depending on an interactive shell's `PATH`.

No GitHub Actions schedule, cron entry, Docker daemon or external alert sink
is enabled by this PR. This is deliberate: a recurring process would incur
resources and must have an explicit operator-owned pinned source, failure
destination and schedule. Cron, GitHub Actions, Dagster and managed schedulers
can all use these same exit codes and reports once configured.

## Verification and operational boundaries

```bash
cd python
poetry install --extras serving
poetry run pytest tests/test_ons_pay_monitor.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Offline tests generate real Git-pinned ONS fixtures, run ingestion plus
DuckDB curation, and exercise healthy reports, missing audits, failed batches,
replayed data, corrupted Parquet, schema/lineage changes, SLA breaches,
Prometheus output, read-only behavior, and scheduler exit codes.

This is a trusted local single-writer monitoring contract. SHA-256 and local
receipts detect drift relative to the recorded audit, not tampering by an
attacker who can rewrite both bytes and receipts. It neither discovers a new
upstream source automatically nor performs an external alerting action.
