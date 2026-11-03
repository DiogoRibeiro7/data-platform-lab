# History-based ONS pay release-health alerts

The [release-health archive](ons-pay-release-health-history.md) records individual,
generation-pinned quality assessments. The `ons-pay-release alerts` command
evaluates that verified history against **explicit operator-defined policies**.
It detects missing or stale assessments, repeated unhealthy results and
increases in the missing-pay rate across manual releases. All decisions remain
**advisory** and independent of a database or cloud provider.

It does not collect assessments, schedule checks, notify anyone, activate a
vintage, or automatically roll back a release. Use `assess --record` deliberately
to produce observations; use `alerts` to read them. Neither action needs a
hosted service.

## Evaluate release quality over history

Start with a [manually activated vintage](ons-pay-release-lifecycle.md) and
one or more [archived health assessments](ons-pay-release-health-history.md):

```bash
cd python
poetry install --extras serving

# This is the explicit write that produces an assessment to trend.
poetry run data-platform-lab ons-pay-release --data-root ../data assess \
  --max-missing-pay-percent 5 --record

# History policies are read-only, even if they trigger an alert.
poetry run data-platform-lab ons-pay-release --data-root ../data alerts \
  --last 50 \
  --max-assessment-age-hours 36 \
  --alert-after-unhealthy 3 \
  --max-unhealthy-share-percent 50 \
  --max-missing-pay-increase-pp 3 \
  --max-cumulative-missing-pay-drift-pp 8
```

The thresholds are **examples**, not recommended production SLOs. Select them
from your expected refresh cadence, monitoring schedule, source variability and
acceptable data-quality risk. Reassess historical comparisons if different
releases were assessed with different health thresholds.

The command prints structured JSON containing the active generation, release
event SHA, exact model/registry/source pins, the selected observation-window
sizes and every policy's state, observed value, threshold and explanation.
It also includes the **effective policy** and its canonical SHA-256 fingerprint.

## Versioned policy-as-code

Commit a policy file alongside the application to review alert thresholds as
code. A [strict, versioned example](../configs/ons-pay-alert-policy.example.json)
is provided:

```json
{
  "schema_version": 1,
  "name": "ons-pay-quality-example",
  "last": 50,
  "max_assessment_age_hours": 36,
  "alert_after_unhealthy": 3,
  "max_unhealthy_share_percent": 50,
  "max_missing_pay_increase_pp": 3,
  "max_cumulative_missing_pay_drift_pp": 8
}
```

The thresholds are examples, not approved SLAs. `schema_version` must be `1`
and `name` a lowercase identifier with optional hyphens. Both are required;
the observation window and all five optional thresholds can be omitted.
Unconfigured checks remain `skipped`, and `last` defaults to 50. This file
contains no credentials or provider-specific connection strings.

```bash
cd python

# Validate the policy without a curated-data root or active release.
poetry run data-platform-lab ons-pay-release validate-alert-policy \
  --policy-file ../configs/ons-pay-alert-policy.example.json

# Apply the pinned policy to verified release history (read-only).
poetry run data-platform-lab ons-pay-release --data-root ../data alerts \
  --policy-file ../configs/ons-pay-alert-policy.example.json

# Explicit CLI flags override matching fields in the file.
poetry run data-platform-lab ons-pay-release --data-root ../data alerts \
  --policy-file ../configs/ons-pay-alert-policy.example.json \
  --last 100 --max-assessment-age-hours 24
```

Precedence is **built-in defaults < versioned policy file < explicit CLI flags**.
The standard flag-only command still works. The JSON response includes the
resolved `policy` object and `policy_sha256`, computed over canonical JSON
(sorted keys, stable separators, UTF-8) **after all overrides**. It is stable
across file whitespace and key-order changes, and differs when the name,
window or thresholds differ. The fingerprint represents the effective
decision settings, not the byte-for-byte file or its Git commit. Use Git
history to establish who approved the file and when.

The validation-only command returns `policy` and `policy_sha256` with exit `0`
for valid input or `2` for errors. It requires neither a release nor an
archive and performs no writes. Duplicate or unknown keys, unsupported schema
versions, boolean numbers, invalid types, non-finite values, impossible limits,
missing files, symlinked policy files and files over 64 KiB are errors. A
malformed file is
rejected **even if a CLI override would replace the offending threshold**.
Nothing implicitly loads a policy from a directory or environment variable.

For read-only evaluation of a policy at *historical* assessment checkpoints,
including a deterministic CSV timeline, see the
[alert replay guide](ons-pay-history-replay.md). Historical signals must not
be confused with current incident status or missing scheduled observations.

### Policy semantics

| JSON check name | When it alerts | Missing evidence |
| --- | --- | --- |
| `active_assessment` | **Always enabled.** No verified assessment matches the *currently active generation and release event*, or its latest timestamp is in the future | Always `alert`, never treat an older generation as current |
| `assessment_age_hours` | Current assessment age **exceeds** `--max-assessment-age-hours` | `unavailable` |
| `consecutive_unhealthy` | Trailing current-generation unhealthy observations **reach** `--alert-after-unhealthy` | `unavailable` |
| `unhealthy_share_percent` | Percent of current-generation assessments marked unhealthy **exceeds** `--max-unhealthy-share-percent` | `unavailable` |
| `missing_pay_increase_pp` | Latest observed missing-pay rate minus that of the **immediately previous release generation** exceeds `--max-missing-pay-increase-pp` | `unavailable` if either generation or rate is absent |
| `cumulative_missing_pay_drift_pp` | Sum of **positive** missing-pay-rate changes across consecutive observed release generations exceeds `--max-cumulative-missing-pay-drift-pp` | `unavailable` if the selected history has fewer than two generations, a generation gap or any missing rate |

Percentage-point drift is not a relative percent increase: a missing-pay rate
of 5% followed by 8% is a **3 pp** increase. A later recovery does not erase
previous increases from the *cumulative positive* measure. A rollback counts
as a **new release generation**, even when it reuses an earlier vintage.

The missing-pay comparisons use the **latest recorded assessment per
generation**, not necessarily an assessment made immediately after the
activation. All trend policies are descriptive, not statistically significant
drift tests, and the unhealthy share is **weighted by observation count, not
time**. Recording more observations during incidents changes its denominator.

`--last` selects 1–1000 recent records (default 50) **after the complete archive
has been validated**. It defines the evidence window. If it excludes an
adjacent release generation, the affected drift check becomes `unavailable`;
the evaluator does not interpolate missing observations or silently pass a
policy. Widen the window or record assessments at each generation to enable
meaningful cross-generation comparisons. A complete selected sequence means
contiguous *recorded* generations in that window; the cumulative sum is not
claimed to cover earlier releases outside it.

### JSON states and exit codes

| Value | Meaning |
| --- | --- |
| `pass` | Enough evidence is present; this check is within its bound |
| `alert` | Its threshold fired, or the active generation lacks a valid current assessment |
| `unavailable` | A configured check cannot be computed without guessing |
| `skipped` | Optional threshold was not configured |

Overall `status` is `alert` if **any** check alerts, otherwise `unavailable`
if any configured check lacks evidence, otherwise `ok`. This precedence keeps
an incident visible even when another check cannot be computed. An unconfigured
optional check is neither a pass nor an error.

- Exit `0`: overall `ok`.
- Exit `1`: `alert` or `unavailable` (JSON/metrics are still printed).
- Exit `2`: invalid policy/options, tampered or unsafe historical evidence,
  changed release pointer during evaluation, or an export error.

`validate-alert-policy` only returns `0` or `2`, because it checks the policy
schema without assessing an active release.

Inputs must be finite and non-negative; unhealthy share and single-step drift
thresholds cannot exceed 100. The consecutive-unhealthy threshold must be a
positive integer. An absent active release is an explicit alert and is not
materialized into a new release.

## Optional Prometheus textfile export

For operators running a local node exporter with a textfile collector, request
a `.prom` export **explicitly**:

```bash
poetry run data-platform-lab ons-pay-release --data-root ../data alerts \
  --max-assessment-age-hours 36 \
  --alert-after-unhealthy 3 \
  --max-missing-pay-increase-pp 3 \
  --format prometheus \
  --textfile ../data/ons-pay-history-alerts.prom
```

The output directory must already exist, contain no symlink components, and
the destination must be a real `.prom` file. Only this explicit option performs
an atomic textfile replacement. **No release pointer, archived health record,
curated data or scheduler is changed.** `--format prometheus` without a
textfile simply writes exposition text to stdout; `--textfile` with JSON is
rejected.

Useful gauges include:

| Metric | Interpretation |
| --- | --- |
| `dpl_ons_pay_history_alert` | `1` when the overall result contains an alert |
| `dpl_ons_pay_history_unavailable` | `1` when the overall result is unavailable without a stronger alert |
| `dpl_ons_pay_history_checked_at_seconds` | UTC time when policies were evaluated |
| `dpl_ons_pay_history_active_generation` | Active generation, omitted if none exists |
| `dpl_ons_pay_history_assessments` | Assessment count in the selected window |
| `dpl_ons_pay_history_checks_alerting` | Number of alerting checks |
| `dpl_ons_pay_history_checks_unavailable` | Number of checks lacking evidence |
| `dpl_ons_pay_history_policy_info{policy_name="...",policy_sha256="..."}` | `1` labels the effective versioned policy used for this evaluation |
| `dpl_ons_pay_history_policy_state{policy="..."}` | Per-check state: `0` pass, `1` alert, `2` unavailable, `3` skipped |

The broad gauges are labeled with current model version, registry commit and
source SHA when a release exists. The policy-info gauge carries the canonical
policy fingerprint and name; the policy-state gauge has its fixed policy
label. These metrics describe **an evaluation**, not proof that an external
collector successfully scraped the textfile. Operators must configure their
own scrape cadence, alert receiver, retention and stale-file monitoring;
missing checks or an obsolete export should not be interpreted as healthy.

## Local regression checks

```bash
cd python
poetry run pytest tests/test_ons_pay_history_alerts.py -q
poetry run pytest tests/test_ons_pay_alert_policy.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

The tests generate locally curated A/B/C vintages, manually activate each
generation, record healthy and unhealthy observations, verify one-step and
cumulative drift, simulate generation gaps and broken Parquet, and enforce
fail-closed archive validation, CLI exit codes and read-only defaults. No
database provider, credentials, network, alert service or automatic rollback
is required.
The policy suite adds schema hardening, malformed/duplicate JSON, unsafe input
paths, fingerprint stability, explicit override precedence and validation
without an active release.
