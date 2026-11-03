# Paired comparison of historical ONS pay alert policies

Use `ons-pay-release compare-alert-policies` to review **what would change**
when replacing one versioned, opt-in alert policy with another. The command
replays both policies on the **same verified archive snapshot** and aligns
results by exact assessment SHA, UTC timestamp, generation, event SHA and
vintage. It does not select a database provider or send any notification.

This is a paired *recorded-checkpoint comparison*, **not** an incident
classification benchmark, replay of a scheduler, or estimate of precision,
recall, false-positive rate or alert latency. There are no external incident
ground-truth labels in the assessment archive.

## Run a comparison

Record assessment history as described in the
[release-health history guide](ons-pay-release-health-history.md), then:

```bash
cd python
poetry install --extras serving

# Both policies are explicit, strict schema-v1 JSON files.
poetry run data-platform-lab ons-pay-release --data-root ../data \
  compare-alert-policies \
  --baseline-policy-file ../configs/ons-pay-alert-policy.example.json \
  --candidate-policy-file ../configs/ons-pay-alert-policy.candidate.example.json \
  --replay-last 50

# Export one flat, paired row per archived checkpoint to stdout.
poetry run data-platform-lab ons-pay-release --data-root ../data \
  compare-alert-policies \
  --baseline-policy-file ../configs/ons-pay-alert-policy.example.json \
  --candidate-policy-file ../configs/ons-pay-alert-policy.candidate.example.json \
  --replay-last 100 --format csv
```

Both policy paths are **required**: the comparison never silently takes a
default or reads the current deployment configuration. Each is checked using
the same strict parser as live alerts and replay, rejecting unknown keys,
duplicate keys, malformed numbers and invalid thresholds. The canonical
SHA-256 of each effective policy appears in JSON and every CSV row.

The `--replay-last` option selects the most recent 1–1000 **output**
checkpoints (default 50). Each policy's independent `last` setting controls
the historical window it can see (1–1000 assessments). Both policies start
from the complete, verified timeline, so a smaller output limit cannot erase
their preceding observation context. A policy with a shorter window can
legitimately have `unavailable` drift evidence that a longer-window policy
can measure; the comparison shows this difference rather than inventing data.

## Interpret the output

The JSON report contains:

| Field | Meaning |
| --- | --- |
| `total_records` | Entire verified archive count, before display selection |
| `baseline_policy` / `candidate_policy` | Complete immutable configuration for each arm |
| `baseline_policy_sha256` / `candidate_policy_sha256` | Canonical, exact policy fingerprints |
| `summary.evaluated` | Number of returned, aligned checkpoints |
| `summary.status_unchanged` / `status_changed` | Same or different overall advisory status |
| `summary.check_evidence_changed` | Checkpoints with any changed check state, observed value, threshold or detail, even if the overall status stays the same |
| `summary.alert_added` | Baseline `ok` **or** `unavailable` became candidate `alert` |
| `summary.alert_cleared` | Baseline `alert` became candidate `ok` **only** |
| `summary.alert_to_unavailable` | Baseline `alert` became candidate `unavailable`; **never counted as cleared** |
| `summary.became_unavailable` | Any non-`unavailable` baseline became candidate `unavailable` |
| `summary.no_longer_unavailable` | Baseline `unavailable` became either `ok` or `alert`; **not necessarily an improvement** |
| `transitions` | All nine baseline-to-candidate pairs, including zero-count cells |
| `points[].record_sha256` / `generation` / `event_sha256` / `vintage` | Shared source identity and release lineage |
| `points[].baseline_status` / `candidate_status` | Advisory result for each arm |
| `points[].baseline_window_assessments` / `candidate_window_assessments` | Number of prior observations each policy could use |
| `points[].baseline_checks` / `candidate_checks` | Every check's state, observed value, threshold and explanation |
| `points[].check_differences` | The exact checks whose evidence differs; includes both complete check objects |

The complete transition matrix uses three possible statuses. Read rows as
baseline states and columns as candidate states:

| Baseline \ Candidate | `ok` | `alert` | `unavailable` |
| --- | --- | --- | --- |
| `ok` | Still OK | New alert | Evidence now unavailable |
| `alert` | Alert cleared | Still alert | **Evidence missing; not a resolution** |
| `unavailable` | Now measurable, OK | Now alerting | Still unavailable |

The CSV has one checkpoint per row, both policy fingerprints, the shared
source pins, recorded health status, both advisory statuses, both policy
window sizes, semicolon-delimited `changed_check_names`, and separate
`baseline_<check>_state` / `candidate_<check>_state` columns for each of the
six fixed checks. The JSON report preserves observed values, thresholds,
check details, the full status matrix and summary. The CSV is suitable for
pandas, DuckDB, R or BI tools; it is written only to stdout.

For explicit, reproducible CI pass/fail review over these same paired
counts, add an opt-in [comparison acceptance gate](ons-pay-alert-comparison-gates.md).
Without a gate, the comparison remains descriptive and does not reject a
candidate policy merely because simulated decisions differ.

## Integrity and limits

- The release history and **every archived assessment** are checked against
  the content-addressed records and release-event chain **once**, before
  selecting the output. A corrupt older record fails even with
  `--replay-last 1`.
- Both replays run against that same in-memory verified snapshot. Every
  checkpoint is evaluated with its own prefix only: later data cannot affect
  earlier results. The active release pointer is checked again after both
  policies finish; concurrent promotion or rollback invalidates the report.
- Missing source metrics and generation gaps remain `unavailable`. An alert
  changing to `unavailable` is explicitly distinguished from one becoming
  `ok`. Changes in thresholds do not change the *recorded* upstream health
  decision; this is a counterfactual alert-rule evaluation only.
- Archived checkpoints do not represent unrecorded intervals or jobs.
  Freshness is evaluated at each assessment's own recorded timestamp; it
  cannot estimate staleness or alert delivery between assessments. Counts
  are observations, not durations, precision/recall or economic impact.
- Full validation reads the entire archive into memory. No file, release
  pointer, record, notification, metric sink, scheduler, provider or automatic
  rollback is created or changed.

Exit status is `0` when at least one checkpoint was compared (even if it
alerts), `1` when no archived checkpoints exist (an empty JSON/CSV result is
still emitted), or `2` for an invalid configuration, unsafe/corrupt archive,
invalid limit or pointer change.

```bash
cd python
poetry run pytest tests/test_ons_pay_alert_policy_comparison.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

See the [single-policy replay guide](ons-pay-history-replay.md) for the
shared evaluator and its assessment-sequence limitations.
