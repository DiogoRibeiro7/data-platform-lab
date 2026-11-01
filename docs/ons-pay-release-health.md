# Active-release health and manual rollback recommendations

The [release lifecycle](ons-pay-release-lifecycle.md) records an exact,
manually activated ONS pay vintage. The existing
[pinned operational monitor](ons-pay-monitoring.md) checks its curation,
Silver/Gold table contracts, freshness, missing pay and refresh audit.

`ons-pay-release assess` combines these capabilities: it evaluates the
**currently activated** exact pin, reports its health, and may identify an
earlier **previously activated, still-verified** version for human rollback
review. It never selects a floating `latest`, changes release state, applies
a rollback, triggers a refresh, or chooses a database provider.

## Assess the active generation

First [refresh and curate](ons-pay-refresh.md) the candidate sources and
[manually bootstrap/promote](ons-pay-release-lifecycle.md) a release. Run:

```bash
cd python
poetry install --extras serving

poetry run data-platform-lab ons-pay-release --data-root ../data assess \
  --max-source-age-days 365 \
  --max-refresh-age-hours 30 \
  --max-missing-pay-percent 5
```

Thresholds here are **illustrative**, not approved ONS release limits.
Unconfigured optional SLAs remain `skipped` in the underlying monitor and
must not be mistaken for passed checks. `assess` reads the exact active pin
and SHA-linked event chain; it does not require a caller-supplied pin. The
two analytical data-query endpoints **still require both exact pins**.

The JSON response contains:

| Field | Meaning |
| --- | --- |
| `generation`, `event_sha256`, `vintage` | Current active generation, content-addressed event and exact model/Git/source identity |
| `active_monitor` | Existing pinned monitoring report, with timestamps, per-check outcomes, metrics and audit provenance |
| `failed_checks` | Named failures affecting the active vintage |
| `advice.action`, `advice.reason` | `keep`, `investigate` or `review_rollback`, with an explanation |
| `advice.to_generation`, `advice.target` | Earlier activation generation and its exact pin, **only** for eligible review recommendations |
| `advice.reviewed_candidates` | Ordered inspection results for earlier distinct active pins: `healthy`, `unhealthy`, or `unverifiable`, plus failed checks |

## Conservative recommendation rules

| Active state | Advice | Rationale |
| --- | --- | --- |
| No configured checks fail | `keep` | The selected release satisfies the requested readiness policy |
| **Only** `missing_pay_rate` fails; an earlier activated distinct pin passes every configured monitor check and the read-only rollback preflight | `review_rollback` | A prior, demonstrably healthier vintage exists and can be considered manually |
| Missing pay fails but no earlier healthy and verifiable pin exists | `investigate` | No supported rollback target can be identified |
| Curated output corruption, table-contract failure, freshness failure, failed batch, missing refresh audit, or multiple different failures | `investigate` | Reverting data is not demonstrated to repair the underlying problem |

Candidates are considered from the most recent earlier active generation
backwards. Repeated pins are evaluated once. Source freshness, refresh age,
pipeline-wide last-batch status and the same missing-pay SLA are checked for
**each** candidate. The tool will not recommend a corrupt, unrefreshed,
stale, or otherwise unhealthy earlier vintage merely because it existed in
release history. This is intentionally stricter than automatically selecting
the last version.

Historical event records can be inspected even when the current curated
Parquet is corrupt: the event hash chain and active pointer are validated
independently of curated data health. Such corruption yields `investigate`,
**not** an unverified rollback target. Invalid or tampered release history
fails closed. The assessor also checks the active generation and event hash
again before returning, rejecting a pointer switch during evaluation.

When you receive `review_rollback`, inspect the full report and determine
whether rolling back makes sense for the downstream consumers. For example,
if it names `to_generation: 1` while the active generation is `2`:

```bash
# Still read-only: the release command's default is a preview.
poetry run data-platform-lab ons-pay-release --data-root ../data rollback \
  --to-generation 1 --expected-generation 2 \
  --reason "Manual review of active missing-pay alert"

# Add --apply only after an operator approves that preview.
```

The recommendation is **not an approval**, and the later rollback must
revalidate the selected pin and expected generation under the release lock.
This workflow is local, supplier-neutral and designed for a trusted operator;
it has no automatic mitigation or distributed coordination.

## Exit codes and Prometheus output

| Code | Meaning |
| --- | --- |
| `0` | Active release is healthy under configured checks (`keep`) |
| `1` | Active release unhealthy; review its `investigate` or `review_rollback` advice |
| `2` | No active release, invalid policy/configuration, tampered release chain, or generation changed while assessing |

The command can output the existing full pinned-monitor gauges plus:

| Metric | Interpretation |
| --- | --- |
| `dpl_ons_pay_release_generation` | Current active generation |
| `dpl_ons_pay_release_rollback_recommended` | `1` when a verified prior pin merits **manual review**, otherwise `0` |
| `dpl_ons_pay_release_rollback_candidates_reviewed` | Number of earlier distinct pins examined |
| `dpl_ons_pay_release_rollback_target_generation` | Prior generation to review; **omitted** unless a target is supported |

All release gauges use the same exact version/commit/source labels as the
existing monitor. The `dpl_ons_pay_monitor_checked_at_seconds` gauge allows a
collector to detect stale assessment runs; absent values remain absent.

```bash
# Print metrics without creating files.
poetry run data-platform-lab ons-pay-release --data-root ../data assess \
  --max-missing-pay-percent 5 --format prometheus

# Explicit, atomic metrics export only: it never writes release state.
# The destination directory must already exist and be a real directory.
poetry run data-platform-lab ons-pay-release --data-root ../data assess \
  --max-missing-pay-percent 5 --format prometheus \
  --textfile ../data/metrics/ons-pay-active-release.prom
```

No cron schedule, Prometheus/Grafana deployment, remote alert notification,
database provider, or rollback action is enabled by these examples.
An operator may configure an external collector or use the exit codes in a
scheduled check. As with the existing monitor, release hashes are local
tamper-evidence, **not** signed attestations or user authentication.

## Offline verification

```bash
cd python
poetry run pytest tests/test_ons_pay_release_health.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Tests refresh and curate real synthetic Git-pinned data, then exercise
healthy and unhealthy active vintages, multi-generation candidate evaluation,
source freshness and failed-batch blockers, broken Parquet, tampered history,
concurrent generation changes, read-only behavior, Prometheus export, and
CLI exit codes. No hosted services or credentials are required.
