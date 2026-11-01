# Historical ONS pay release-health assessments

The [active-release assessor](ons-pay-release-health.md) evaluates the
currently selected ONS pay vintage and, when justified, suggests a prior
version for **manual** rollback review. A single assessment only shows one
point in time. This extension lets an operator deliberately record those
assessments, inspect a validated timeline, and export it to CSV for further
analysis without choosing a database provider.

No collection, retention, scheduler, alert receiver, external database,
automatic promotion, or automatic rollback is enabled.

## Explicitly record an assessment

First complete the [manual release lifecycle](ons-pay-release-lifecycle.md)
and an audited refresh. The existing `assess` command remains **read-only by
default**. Add `--record` only when you want a durable local assessment:

```bash
cd python
poetry install --extras serving

poetry run data-platform-lab ons-pay-release --data-root ../data assess \
  --max-source-age-days 365 \
  --max-refresh-age-hours 30 \
  --max-missing-pay-percent 5 \
  --record
```

Replace the example SLAs with your own release-readiness policy. The JSON
response adds `record_sha256` when recording succeeds. An unhealthy assessed
release still returns exit code `1` **after** its incident is safely recorded;
this allows the history to include failed checks rather than just successes.
Invalid options, a changed active generation, unsafe paths, or inconsistent
evidence return exit code `2` without publishing a new record.

Each assessment is stored beneath the curated data root:

```text
releases/ons-pay/health/assessments/<assessment-sha256>.json
```

The file contains the full pinned health report: monitoring timestamp,
generation, SHA-256-linked release event, exact model/Git/source pins,
checks, observed metrics, and any advisory rollback recommendation. It is
canonical JSON whose filename is its content SHA-256. Repeating the exact
report is idempotent; a different observation creates another immutable
record. There is no mutable index to corrupt or keep in sync.

The writer validates the recorded schema, report consistency, and release
event lineage. It holds the same local POSIX writer lock used by activation
while linking and syncing the record, so an activation cannot switch the
pointer between evidence validation and publication. A failed write cannot
expose a partially written `.json` record; a crash could leave an unused
temporary file, which trend readers ignore. A damaged active Parquet **can
still produce a recorded `investigate` assessment**, because the release
event chain is verified separately from the current data's health.

Only `--record` writes archive data. The existing `--format prometheus
--textfile /path/metrics.prom` remains a separate, explicitly requested
metrics export, not an archive or a release change.

## Read the validated timeline

```bash
# Read-only JSON summary plus recent individual observations.
poetry run data-platform-lab ons-pay-release --data-root ../data trends \
  --last 50

# Restrict the timeline to assessments made while generation 2 was active.
poetry run data-platform-lab ons-pay-release --data-root ../data trends \
  --generation 2 --last 100

# Flat, deterministic CSV for pandas, DuckDB, R or a BI tool.
poetry run data-platform-lab ons-pay-release --data-root ../data trends \
  --last 200 --format csv
```

`--last` accepts 1–1000 and defaults to 50. It limits returned samples
**after** the optional generation filter. `total_records` always counts the
complete archive; the summary describes only the selected time window.
Every stored record is validated *before* applying the filters, so an older
tampered file cannot be hidden with `--last 1`.

### JSON contents

| Field | Interpretation |
| --- | --- |
| `total_records` | Number of verified archive records across all generations |
| `samples` | Timestamp-ordered observations with SHA, exact pins, generation, status, failures, missing-pay %, source age, refresh age and rollback advice |
| `summary.assessments` / `summary.generations` | Samples and distinct active generations within the selected window |
| `summary.healthy_assessments` / `summary.unhealthy_assessments` | Health counts; this is not an estimate of time spent healthy |
| `summary.healthy_to_unhealthy` / `summary.unhealthy_to_healthy` | Adjacent observed status transitions within the selected timeline |
| `summary.rollback_reviews` | Count of assessments recommending manual rollback review |
| `summary.missing_pay_change_pp` | Last minus first observed missing-pay percentage, in **percentage points**, when both are measurable |
| `by_generation` | Sample counts, first/last times, latest status and within-generation missing-pay change for each selected generation |

Sorting uses UTC assessment time, then assessment SHA for deterministic ties.
Missing metrics remain JSON `null` or empty CSV cells, never imputed as zero.
The CSV has fixed column names for downstream ingestion and does not contain
the entire nested monitor check payload; the immutable JSON retains that
evidence. A trend computed from two observations is only a descriptive
difference, **not** a statistical drift test, interpolation, or proof of a
cause. Different thresholds on different runs can change a health status
without any underlying Parquet modification; inspect the recorded policy
values before interpreting the transitions.

Records remain bound to the **event SHA of the generation evaluated at the
time**, even after a later promotion or rollback changes the active pin.
Trend reads re-verify the entire linked release history and each canonical
record/hash/reference. A corrupted record, unsafe symlink, disconnected
release event, or concurrent pointer change fails closed.

An archive query with no records returns an empty summary and creates no
directories. Archive data is local and assumes trusted filesystem access:
content hashes detect accidental drift but do not authenticate a user who
can rewrite both the data and its references. There is currently no purge
operation or retention automation; size and retention remain operator-owned.

## Offline verification

```bash
cd python
poetry run pytest tests/test_ons_pay_release_history.py -q
poetry run ruff check src/ tests/
poetry run ruff format --check src/ tests/
poetry run mypy src/
```

The suite exercises real locally curated and refreshed Git-pinned vintages;
idempotent archive publication; linked history across rollback; health
transitions and per-generation summaries; CSV; invalid and tampered records;
symlinks; interrupted publication; malformed CLI parameters; and read-only
default behavior. No hosted services or credentials are required.
