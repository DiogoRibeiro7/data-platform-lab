# Read-only acceptance gates for historical alert policy changes

An optional, versioned acceptance gate converts the
[paired policy comparison](ons-pay-alert-policy-comparison.md) into a
reviewable CI pass/fail decision. It checks **observed archived checkpoint
counts** against limits chosen by the operator. There is no automatic policy
promotion, release activation, external notification or supplier selection.

The gate answers *whether this candidate stays inside the review budget*,
**not** whether it is statistically more accurate. Historical assessments
have no external incident ground-truth labels, and a missing measurement is
not evidence that an alert was resolved.

## Validate and evaluate a gate

Store the gate as a strict, versioned JSON file. The
[example](../configs/ons-pay-alert-comparison-gate.example.json) demonstrates
five review budgets; set thresholds appropriate to the history being reviewed.

```bash
cd python
poetry install --extras serving

# Validate configuration independently of releases and optional dependencies.
poetry run data-platform-lab ons-pay-release validate-alert-comparison-gate \
  --gate-file ../configs/ons-pay-alert-comparison-gate.example.json

# Compare the exact same verified checkpoints and evaluate the optional gate.
poetry run data-platform-lab ons-pay-release --data-root ../data \
  compare-alert-policies \
  --baseline-policy-file ../configs/ons-pay-alert-policy.example.json \
  --candidate-policy-file ../configs/ons-pay-alert-policy.candidate.example.json \
  --gate-file ../configs/ons-pay-alert-comparison-gate.example.json \
  --replay-last 50 --format json
```

`--gate-file` is opt-in. Without it, the comparison and its exit-code
semantics are unchanged. JSON output with a gate includes a new
`acceptance_gate` object containing the exact resolved configuration,
canonical `gate_sha256`, overall `pass`/`fail`, and each observed budget check.
The original comparison evidence remains available in the same response.
The CSV retains its fixed paired-checkpoint schema; with a gate, **stdout
remains CSV** and the process exit code conveys acceptance. To inspect the
individual budget checks and fingerprint, request JSON.

| Gate configuration field | Interpretation |
| --- | --- |
| `schema_version` | Required; currently `1` |
| `name` | Required lowercase hyphenated identifier, up to 64 characters |
| `min_evaluated` | Minimum paired checkpoints required (1–1000; defaults to 1) |
| `max_alert_added` | Maximum baseline `ok` **or** `unavailable` to candidate `alert` transitions |
| `max_became_unavailable` | Maximum baseline non-`unavailable` to candidate `unavailable` transitions |
| `max_alert_to_unavailable` | Maximum baseline `alert` to candidate `unavailable` transitions; this is not a cleared alert |
| `max_status_changed` | Maximum total checkpoints with different baseline and candidate statuses |

Each configured maximum must be an integer from 0 to 1000 (inclusive),
or `null` to disable it. An empty gate that only checks the default single
checkpoint is rejected; specify at least one maximum, or explicitly require
`min_evaluated` above 1. The mandatory minimum check ensures an empty archive
**never passes** because all maximum counts happened to be zero.

For example, a `max_alert_added` of `3` allows three transitions to `alert`
in the selected checkpoints but fails on four. It does not label those alerts
as true or false positives. A `max_alert_to_unavailable` of `0` explicitly
rejects any `alert → unavailable` outcome, even if an operator is otherwise
comfortable with changes in total alert volume. A failed check is returned
with `metric`, `observed`, `limit`, `operator` and `status`.

The gate works over the **selected** `--replay-last` checkpoints; policies
still read their own preceding windows from the complete verified archive.
Do not mistake a small output limit for a long-term performance guarantee.
Using a larger `min_evaluated` than `--replay-last` must fail, not silently
expand the comparison or fabricate observations.

## Exit codes, safety and reproducibility

- `0`: at least one verified checkpoint exists and **all** configured gate
  checks pass, or no gate was supplied and the comparison has observations.
- `1`: no archived checkpoints are available **or** any opt-in gate check
  fails. The JSON/CSV report is still written to stdout for diagnosis.
- `2`: malformed or unsafe gate/policy files, unsupported schema, invalid
  threshold, corrupt archive or concurrent release-pointer change.

The gate parser rejects unknown and duplicate JSON keys, non-finite numbers,
booleans used as counts, symlinked configuration files, oversized files and
unsupported versions. Equivalent JSON formatting produces the same canonical
SHA-256 fingerprint. `validate-alert-comparison-gate` performs only file
validation and does not need an active release or hosted database.

The entire content-addressed assessment archive and SHA-linked release chain
are validated before evaluating either policy. Both sides see one verified
snapshot and only past observations at each checkpoint; a concurrent pointer
change invalidates the comparison. Neither the acceptance gate nor the
comparison modifies release data, writes a record, sends a notification or
activates a candidate policy.

```bash
cd python
poetry run pytest tests/test_ons_pay_alert_comparison_gate.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```
