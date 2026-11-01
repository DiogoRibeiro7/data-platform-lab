# Pinned ONS pay: opt-in Prometheus and Grafana integration

This exercise takes the [read-only ONS pay monitor](ons-pay-monitoring.md) to an
operator-owned observability stack. The monitor continues to select exactly one
Git-pinned dataset and never chooses a new source or database supplier. No
network service, notification, alert receiver, scheduler, or dashboard is
deployed by the repository.

## Publish a complete metrics snapshot when requested

First create a directory that the local metrics collector can read, then
choose one `.prom` path for one pinned monitoring job:

```bash
cd python
poetry install --extras serving
mkdir -p ../data/metrics-textfile

poetry run data-platform-lab ons-pay-monitor \
  --data-root ../data \
  --registry-commit COMMIT_SHA40 \
  --source-sha256 SOURCE_SHA256 \
  --max-source-age-days 365 \
  --max-refresh-age-hours 30 \
  --max-missing-pay-percent 5 \
  --format prometheus \
  --textfile ../data/metrics-textfile/ons-pay.prom
```

Replace both pins with their exact lowercase hexadecimal values. The command
still prints Prometheus metrics to standard output. With `--textfile`, it also
atomically replaces *only* the explicitly specified `.prom` file. This is the
**only opt-in write** added to the monitoring command. Without the flag,
monitoring remains read-only, as before.

The writer requires an existing, real directory and rejects symlinked targets
or parent directories, non-`.prom` files and directory targets. It writes a
temporary file beside the destination, flushes it, then renames it. A failed
write preserves the previous complete file, which becomes stale rather than
being mistaken for a fresh healthy result. Each output file should have one
trusted writer and a deliberate retention policy. The newly published file is
world-readable (`0644`) for collectors running as a separate local user; do
not include secrets in the directory.

| CLI result | `.prom` behaviour | Next action |
| --- | --- | --- |
| `0` healthy | Fresh health, timestamp and quality gauges published | No incident |
| `1` unhealthy | Fresh *unhealthy* metrics are still published | Alert using the health gauge |
| `2` invalid configuration or textfile error | Prior complete file preserved; no new sample | Fix the error; stale/absent gauges reveal a stopped exporter |

The `--textfile` option requires `--format prometheus`. A monitor JSON report
remains available with `--format json` (without the textfile flag), including
individual check details and pinned run IDs.

## Capture missing and stalled monitoring runs

The Prometheus output now always includes these pinned gauges:

| Gauge | Meaning |
| --- | --- |
| `dpl_ons_pay_monitor_healthy` | 1 when no required or configured check failed, else 0 |
| `dpl_ons_pay_monitor_checked_at_seconds` | UTC Unix timestamp of the last completed evaluation |
| `dpl_ons_pay_monitor_checks_failed` | Count of actual failed checks |
| `dpl_ons_pay_monitor_checks_skipped` | Count of skipped checks, including optional SLAs not configured |

The timestamp is part of the *evaluation*, not the scrape or filesystem
modification time. A stopped scheduler therefore cannot make an old healthy
report look fresh simply because Prometheus still scrapes its file. Metric
values that cannot be established (such as refresh age before any success)
remain omitted, never fabricated as zero.

An operator can point an existing [Prometheus Node Exporter textfile
collector](https://github.com/prometheus/node_exporter#textfile-collector)
at the directory using `--collector.textfile.directory=/ABS/metrics-textfile`.
Configure Prometheus to scrape that Node Exporter. **Do not** point Prometheus
at a raw `.prom` file or treat standard output as an HTTP metrics endpoint.

The examples are provided for *manual import* into an existing stack:

- [Prometheus alert rules](examples/ons-pay-alert-rules.json): valid JSON/YAML
  data defining unhealthy, stale and absent-sample conditions. Replace
  `COMMIT_SHA40` and `SOURCE_SHA256` with the exact monitored pin before
  including this file in Prometheus `rule_files`.
- [Grafana dashboard](examples/ons-pay-health-dashboard.json): a Classic
  dashboard import with a Prometheus datasource picker, exact-pin textboxes,
  health and failure counts, time since last evaluation, source/refresh ages,
  and missing-pay percentage. Enter both pins after import.

These assets are *templates*, not runtime configuration. No existing Compose
file or GitHub Actions workflow refers to them. Alertmanager routing, contact
points, and Prometheus/Grafana provisioning are operator-owned. The Node
Exporter textfile collector and Prometheus rules do not send notifications on
their own without a configured alert delivery path.

## Adjust the policy to your execution cadence

The example alerts distinguish three failure modes:

1. **Unhealthy:** the pinned evaluation completed with one or more failures;
   the zero-valued health gauge persists for five minutes.
2. **Stale:** no completed evaluation in two hours (`time() - checked_at`);
   the signal persists for ten minutes. This assumes at least an hourly job.
3. **Missing:** the exact pin has no timestamp gauge at all for 15 minutes,
   including a never-started monitor or a broken scraping target.

Choose the evaluation interval, rule windows and SLA thresholds together. A
daily cron job should *not* reuse the example two-hour staleness threshold;
increase it beyond the expected job interval plus some recovery allowance.
The [example cron schedule](examples/ons-pay-ops.crontab.example) is still
disabled and does not become active by including these templates.

When promoting a newer immutable source pin, update the scheduler, alert
rule selectors and Grafana textbox values **as one change**. Do not retain old
`.prom` files with active, obsolete pins in the same collector directory.
Using a single named file for each monitored pipeline makes replacement
explicit. Keep data availability and pipeline freshness conceptually separate:
running the same old source again can reset refresh age but never source age.

## Local checks and operational limitations

```bash
cd python
poetry run pytest tests/test_ons_pay_monitor.py tests/test_ons_pay_observability_export.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Offline tests simulate healthy and failing monitor reports, symlink/path
rejection, atomic replacement failure, and example query compatibility with
the actual emitted metric names. The tests do not provision Prometheus,
Grafana, Node Exporter or an external alert receiver. No signed attestation,
multi-writer locking, job scheduler, or cross-host monitoring store is added.
