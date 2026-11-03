# Read-only replay of historical ONS pay alert policies

The [versioned alert policies](ons-pay-history-alerting.md) can be evaluated
against previously archived release-health assessments, without choosing a
database supplier or executing any release action. `ons-pay-release
replay-alerts` produces a deterministic checkpoint timeline for operators
reviewing thresholds **before** using them to monitor a running platform.

This is an **assessment-sequence replay**, not a scheduler simulation or a
statistical estimate of false-positive/false-negative rates. It uses existing,
verified recorded health and missing-pay metrics; it never reopens historical
Parquet to recalculate the monitor's original health decisions.

## Run the replay

First [record assessments](ons-pay-release-health-history.md) from manually
activated releases. Then run:

```bash
cd python
poetry install --extras serving

# Inspect the most recent 50 archived assessment checkpoints as JSON.
poetry run data-platform-lab ons-pay-release --data-root ../data replay-alerts \
  --policy-file ../configs/ons-pay-alert-policy.example.json \
  --replay-last 50

# Flat CSV for pandas, DuckDB, R or a BI tool; output is stdout only.
poetry run data-platform-lab ons-pay-release --data-root ../data replay-alerts \
  --policy-file ../configs/ons-pay-alert-policy.example.json \
  --replay-last 100 --format csv

# Compare sensitivity without editing the policy file.
poetry run data-platform-lab ons-pay-release --data-root ../data replay-alerts \
  --policy-file ../configs/ons-pay-alert-policy.example.json \
  --last 20 --alert-after-unhealthy 2 \
  --max-cumulative-missing-pay-drift-pp 5
```

`--replay-last` controls **how many checkpoints are returned** (1–1000,
default 50). The policy's `last` value independently controls **how many
preceding assessments each checkpoint may use** (1–1000, default 50). A
replay with `--replay-last 1` still uses older verified observations needed
for that checkpoint's drift and unhealthy-streak calculations; shortening the
output never silently truncates the assessment prefix. Ordinary live `alerts`
and replay use the **same policy evaluator**, so their policy logic does not
drift into two implementations.

The policy file is optional. The `--last` and threshold flags can be supplied
directly, with the same precedence as live alerts: **defaults < policy file <
explicit CLI overrides**. The effective policy's SHA-256 fingerprint is
included in JSON and repeated in the CSV. Different policy windows or
thresholds produce distinct fingerprints.

## What each checkpoint means

The replay sorts all verified assessment observations by UTC `checked_at_utc`,
breaking ties by the immutable assessment SHA. At each checkpoint, it passes
only the current and earlier assessments to the shared alert evaluator,
bound to that assessment's historical generation, event SHA and vintage.
**A later assessment cannot change an earlier checkpoint's result.**

| JSON field | Interpretation |
| --- | --- |
| `total_records` | Full count of verified archived assessments, before selecting output |
| `replay_last` | Maximum number of returned checkpoints |
| `policy` / `policy_sha256` | Exact resolved policy and canonical fingerprint |
| `summary.evaluated` | Number of checkpoints returned |
| `summary.ok` / `alert` / `unavailable` | Counts of simulated statuses at the returned checkpoints |
| `summary.first_checked_at_utc` / `last_checked_at_utc` | Endpoints of the displayed observation timeline |
| `points[].record_sha256` | Immutable assessment that defines this checkpoint |
| `points[].generation` / `event_sha256` / `vintage` | Exact historical release lineage |
| `points[].recorded_health_status` | Health marked by the original archived assessor, under its original monitor thresholds |
| `points[].alert_status` / `checks` | Replayed policy decision and its per-check pass/alert/unavailable/skipped evidence |
| `points[].window_assessments` | Prefix size available to the policy at this checkpoint |

CSV includes these key fields, fixed columns for every policy check state,
and semicolon-separated alerting/unavailable policy names. Missing evidence
remains `unavailable`, never imputed as zero or a passing observation. No
output file is created unless the operator explicitly redirects stdout; no
archive record, release pointer, metric textfile or notification is written.

### Interpretation limits

- A replay checkpoint exists **only when an assessment was recorded**. An
  unassessed promotion or missing scheduled run does not create a checkpoint.
  The `active_assessment` check therefore passes at observed checkpoints,
  even when the live evaluator would have alerted between them.
- Assessment age is zero **at its own recorded timestamp**. This replay cannot
  tell how long a release remained stale between assessments. Use live `alerts`
  or an explicit external schedule to evaluate current freshness.
- Unhealthy share counts observations, not hours. Historical checks were
  recorded using their original assessment SLAs; replay does not reconstruct
  alternative health decisions for a different upstream policy.
- Cross-generation missing-pay comparisons require adjacent generations with
  measurable observations **inside that checkpoint's policy window**. A
  skipped generation or absent rate is `unavailable`; a later measurement
  cannot fill an earlier gap retroactively.
- The replay's status counts are not confusion-matrix labels or measured alert
  precision/recall. A snapshot date is an observation timestamp, not proof of
  an external page, notification or successful scrape.

The entire content-addressed assessment archive and release chain are
validated **before** applying `--replay-last`, even when it is `1`. The
active pointer is rechecked after replay; a concurrent release change fails
closed. Full validation requires reading all archived records into memory,
so operators should consider the archive size before replaying large local
datasets. Only the output checkpoint count is bounded; there is no retention
or background compaction.

## Exit codes and offline verification

- `0`: at least one checkpoint was evaluated, **even if historical alerts are
  present**. Replay describes history; it does not signal a current incident.
- `1`: no archived checkpoints are available. JSON/CSV still communicates
  the empty result; it is not silently treated as healthy.
- `2`: invalid limits or policy, tampered historical records, unsafe paths,
  or a concurrent release-pointer change.

```bash
cd python
poetry run pytest tests/test_ons_pay_history_replay.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Offline fixtures activate A/B/C vintages, record health snapshots, replay
step and cumulative drift, verify bounded output with complete earlier
context, check CSV determinism, and exercise tamper and pointer-change
failures. There is no provider choice, external network, scheduler,
automatic promotion or automatic rollback.
