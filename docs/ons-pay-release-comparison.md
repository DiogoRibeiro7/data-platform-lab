# Compare two pinned ONS pay data releases

The [audited refresh/backfill pipeline](ons-pay-refresh.md) can curate multiple
immutable source snapshots, while [operational monitoring](ons-pay-monitoring.md)
checks the health of one selected snapshot. This release-comparison command
answers a different question: **what changed in the observations between two
explicit versions, and does that change violate our acceptance policy?**

No hosted database, source discovery, deployment, or automatic promotion is
required. The command only reads vetted local Parquet and receipts.

## Run a reproducible comparison

First materialize *both* exact vintages using the refresh/backfill guide. Copy
the complete registry commit and source SHA-256 for the baseline and candidate
from the curation receipts or `GET /v1/ons-pay/vintages`:

```bash
cd python
poetry install --extras serving

poetry run data-platform-lab ons-pay-compare \
  --data-root ../data \
  --baseline-registry-commit BASELINE_COMMIT_SHA40 \
  --baseline-source-sha256 BASELINE_SOURCE_SHA256 \
  --candidate-registry-commit CANDIDATE_COMMIT_SHA40 \
  --candidate-source-sha256 CANDIDATE_SOURCE_SHA256 \
  --max-added-rows 2000 \
  --max-removed-rows 0 \
  --max-revised-rows 300 \
  --max-newly-missing-pay-rows 0 \
  --max-missing-pay-increase-pp 1 \
  --max-absolute-pay-revision-gbp 100 \
  --examples-per-change 5
```

All four pins must be genuine lowercase hexadecimal values, never a branch
name or a floating `latest` label. The example limits illustrate the API,
**not approved quality thresholds** for a future ONS release. Different
snapshots can legitimately add years, change names, withdraw estimates, or
revise historic pay; choose policy limits based on the intended release.

The JSON report contains the two exact pins, row counts, difference categories,
missing-pay transitions, field-level revision counts, maximum absolute change
among comparable numeric weekly pay values, threshold outcomes and sorted,
limited examples from each change category.

## Equality and row-grain semantics

The comparison first validates the curation receipts, output SHA-256 hashes,
all three Silver/Gold column contracts, row counts, lineage pins and reported
pay totals for both vintages. A corrupt or missing output prevents comparison.
It additionally rejects duplicate/null Silver observation keys.

One Silver observation is identified by **`area_code × reference_date`**. A
DuckDB full outer join classifies its changes:

| Category | Meaning |
| --- | --- |
| `added` | Key exists only in the candidate |
| `removed` | Key exists only in the baseline |
| `revised` | Key exists in both; at least one business field differs |
| `unchanged` | Key exists in both; all business fields match, including NULLs |

Business fields are area name, geography, pay year, weekly GBP pay, confidence
bounds, status code and notes. `source_sha256` and `registry_commit` are
**lineage fields**, excluded from revision detection; otherwise a perfectly
identical dataset re-registered under a new Git commit would look entirely
changed. Comparisons use SQL `IS DISTINCT FROM`, so `NULL` against `NULL`
remains unchanged while a numeric-to-NULL transition is a genuine revision.

Two additional metrics distinguish *newly missing* pay (a previously reported
value becomes unavailable) from *newly reported* pay (an unavailable value
becomes numeric). These refer only to matching keys; new and removed keys are
counted separately. The overall missing-pay share is calculated independently
for each vintage from the source row totals. It can stay unchanged even when
the two transition counts are nonzero.

Each example gives the observation key, changed fields, and before/after
business values. Examples are sorted by code and date, capped at 0–100 per
category; the aggregate counts always cover *all* observations. The maximum
absolute pay revision uses nominal GBP per week for keys with reported pay on
both sides; it is `null` when no comparison is possible. This is a data change
statistic, not inflation adjustment, statistical significance, or causal effect.

## Release checks and exit codes

Each policy is optional. If omitted, its check is explicitly marked `skipped`
in JSON, not quietly assumed to have passed.

| Policy flag | Fails when |
| --- | --- |
| `--max-added-rows` | Too many new observation keys |
| `--max-removed-rows` | Too many prior keys disappeared |
| `--max-revised-rows` | Too many existing business records changed |
| `--max-newly-missing-pay-rows` | Too many previously reported pay values became unavailable |
| `--max-missing-pay-increase-pp` | Overall missing-pay rate increased by more than the permitted *percentage points* |
| `--max-absolute-pay-revision-gbp` | Any overlapping reported weekly pay value changed by more than the maximum nominal GBP amount; an unmeasurable comparison breaches this configured policy |

All integer limits must be nonnegative; the missing-pay increase limit must
lie between 0 and 100. A negative change in missing-pay share is an improvement
and cannot breach a positive maximum. The command also validates
`--examples-per-change` as an integer between 0 and 100.

| Exit | Meaning | Suggested next action |
| --- | --- | --- |
| `0` | No *configured* policy breached | Review results and any skipped policies; do not infer approval automatically |
| `1` | One or more policies breached | Block candidate publication and inspect the changed keys |
| `2` | Invalid pins, unsafe/missing Parquet, broken contract or invalid arguments | Repair the evidence/configuration before evaluating release policy |

The direction matters: `baseline → candidate` is not interchangeable with
`candidate → baseline`. A change of source hash alone is not a content revision.
Both identical-pin reruns and different-commit/same-content snapshots compare
with zero business changes.

## Testing and limitations

```bash
cd python
poetry run pytest tests/test_ons_pay_compare.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

Offline tests materialize actual v1 DuckDB/Parquet vintages, including changed
GBP values, missingness transitions, additions/deletions, unchanged byte
replays, policy breaches, corrupted Parquet, duplicate keys and CLI exit codes.
The command creates only temporary in-memory DuckDB tables and does not write
comparison history, change receipts, source data, metrics, or publication
state. This is a deterministic *pre-publication evidence gate* that a future
operator or CI workflow may invoke after explicitly selecting candidate pins.
