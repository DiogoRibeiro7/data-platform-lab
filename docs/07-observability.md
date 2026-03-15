# Exercise 07: Observability Utilities

## Problem Statement

Every pipeline in this project produces some form of run metadata — row counts, timing, file counts, error lists — but each module invents its own ad-hoc result type. This makes it hard to build consistent logging, alerting, or dashboards across pipelines. This exercise extracts the common observability patterns into reusable utilities that any pipeline can use.

## Observability Goals

The observability module provides three things:

1. **Timing** — measure how long any operation takes, with a clean start/stop API and context-manager support.
2. **Run tracking** — collect standardized metadata (rows, files, warnings, errors, custom fields) during a pipeline run.
3. **Formatting** — produce human-readable summaries and JSON-serializable metadata objects.

These utilities are building blocks, not a framework. They don't impose a pipeline structure — they work alongside the orchestration runner, ingestion pipelines, or any standalone script.

## API Overview

### Timer

A simple execution timer with two usage modes:

```python
# Python — context manager
with Timer() as t:
    do_work()
print(f"Took {t.elapsed:.2f}s")

# Python — manual
t = Timer()
t.start()
do_work()
t.stop()
print(t.elapsed)
```

```javascript
// JavaScript — manual (no context manager in JS)
const timer = new Timer();
timer.start();
await doWork();
timer.stop();
console.log(`Took ${timer.elapsed.toFixed(2)}s`);
```

Properties:
- `elapsed` — seconds elapsed (live while running, frozen after stop, 0 before start)
- `running` — boolean, true between start and stop

### RunTracker

Collects all run metadata in one place. Supports context-manager usage in Python.

```python
# Python
tracker = RunTracker("csv_ingest")
with tracker:
    records = read_csv(path)
    tracker.inc_rows_read(len(records))
    valid, rejected = validate(records)
    tracker.inc_rows_written(len(valid))
    tracker.inc_rows_rejected(len(rejected))
    tracker.add_warning(f"{len(rejected)} rows had missing emails")
    tracker.set_extra("source_file", "customers.csv")

print(format_run_metadata(tracker.metadata))
```

```javascript
// JavaScript
const tracker = new RunTracker("csv_ingest");
tracker.start();
try {
  const records = await readCsv(path);
  tracker.incRowsRead(records.length);
  const { valid, rejected } = validate(records);
  tracker.incRowsWritten(valid.length);
  tracker.incRowsRejected(rejected.length);
  tracker.addWarning(`${rejected.length} rows had missing emails`);
  tracker.setExtra("source_file", "customers.csv");
  tracker.finish();
} catch (err) {
  tracker.addError(err.message);
  tracker.finish("failed");
}
console.log(formatRunMetadata(tracker.metadata));
```

Counter methods:
- `inc_rows_read(n)` / `incRowsRead(n)`
- `inc_rows_written(n)` / `incRowsWritten(n)`
- `inc_rows_rejected(n)` / `incRowsRejected(n)`
- `inc_files_processed(n)` / `incFilesProcessed(n)`
- `inc_files_rejected(n)` / `incFilesRejected(n)`

Diagnostic methods:
- `add_warning(msg)` / `addWarning(msg)` — also logs at WARNING level (Python)
- `add_error(msg)` / `addError(msg)` — also logs at ERROR level (Python)
- `set_extra(key, value)` / `setExtra(key, value)` — store arbitrary key-value metadata

Lifecycle:
- `start()` — begin timing, set status to "running"
- `finish(status)` — stop timing, set final status (default: "success")
- Context manager (Python only) — calls start/finish automatically, sets "failed" on exception

## Run Metadata Schema

The `metadata` property returns a structured object:

```
{
  pipeline_name: string,      // "csv_ingest"
  run_id: string,             // "20240601_103000"
  status: string,             // "success", "failed", "running"
  started_at: string | null,  // ISO 8601 timestamp
  ended_at: string | null,    // ISO 8601 timestamp
  duration_seconds: float,    // wall-clock seconds
  rows_read: int,
  rows_written: int,
  rows_rejected: int,
  files_processed: int,
  files_rejected: int,
  warnings: string[],
  errors: string[],
  extra: { key: value, ... }, // custom metadata
}
```

This schema covers the fields that appear across all existing pipeline result types in the project:

| Existing module | Fields covered by RunMetadata |
| --- | --- |
| CSV pipeline (`PipelineResult`) | rows_read, rows_written, files_processed, files_rejected |
| API pipeline (`ApiRunResult`) | run_id, duration_seconds, errors |
| Incremental ETL (`RunSummary`) | rows_read (records_seen), rows_written (records_processed), rows_rejected (records_failed) |
| Orchestration (`PipelineResult`) | status, started_at, ended_at, duration_seconds |

The `extra` field lets pipelines attach module-specific data (checkpoint status, API URL, page counts) without extending the base schema.

## Example Output

```
=== Run: csv_ingest (20240601_103000) ===
Status: success
Started: 2024-06-01T10:30:00+00:00
Ended:   2024-06-01T10:30:02+00:00
Duration: 2.34s

Rows read:     1500
Rows written:  1423
Rows rejected: 77
Files processed: 3
Files rejected:  1

Warnings (2):
  - 42 rows had null email addresses
  - 35 rows had invalid date formats

Extra:
  source_dir: data/raw/customers
  output_path: data/bronze/customers/cleaned.csv
```

## Integration with Other Modules

The observability utilities are designed to work alongside, not replace, existing code:

**With the orchestration runner** — use RunTracker inside a pipeline step:
```python
def extract_step(ctx):
    tracker = RunTracker("extract")
    with tracker:
        data = read_csv(ctx["input_path"])
        tracker.inc_rows_read(len(data))
    ctx["extract_metadata"] = tracker.metadata
    return data
```

**With CLI tools** — print the formatted summary at the end:
```python
result = run_pipeline(...)
tracker.inc_rows_read(result.rows_read)
tracker.inc_rows_written(result.rows_written)
tracker.finish()
print(format_run_metadata(tracker.metadata))
```

**For audit logging** — serialize to JSON:
```python
import json
meta_dict = metadata_to_dict(tracker.metadata)
with open("audit.json", "a") as f:
    f.write(json.dumps(meta_dict) + "\n")
```

## Differences Between Python and JavaScript

| Aspect | Python | JavaScript |
| --- | --- | --- |
| Timer precision | `time.perf_counter()` (sub-microsecond) | `Date.now()` (millisecond) |
| Context manager | `with Timer() as t:` / `with RunTracker() as t:` | Not available (use start/finish explicitly) |
| Error capture | `__exit__` catches exception type and message | Manual try/catch with `addError` |
| Data types | `@dataclass RunMetadata` | Plain object from `metadata` getter |
| Logging | Python `logging` module (WARNING/ERROR) | No built-in logging (methods just collect) |
| Dict conversion | `metadata_to_dict()` using `dataclasses.asdict` | Already a plain object |
| Counter naming | `inc_rows_read()` (snake_case) | `incRowsRead()` (camelCase) |

## Running Tests

```bash
# Python (20 tests)
cd python && python -m pytest tests/test_observability.py -v

# JavaScript (18 tests)
cd javascript && node --test tests/observability.test.js
```

## Limitations

- **No persistence.** Metadata exists only in memory. A production system would write to a database, log aggregator, or metrics service.
- **No distributed tracing.** There are no trace IDs, span IDs, or parent-child relationships between runs. The `run_id` is purely local.
- **No metrics aggregation.** There is no facility to compare metrics across runs (e.g., "rows_read dropped 50% from yesterday"). That requires a time-series store.
- **No alerting.** The module collects data but does not act on it. Threshold-based alerts would need to be built on top.
- **Single-threaded counters.** The increment methods are not thread-safe. For concurrent pipelines, each thread/task should use its own RunTracker.

## Future Extensions

- Add a `MetricsStore` that persists RunMetadata to a JSONL file or SQLite database for historical comparison.
- Add threshold-based alerts (e.g., fail if rows_rejected > 10% of rows_read).
- Add a `compare_runs()` function to diff two RunMetadata objects and flag regressions.
- Add OpenTelemetry-compatible span export for distributed tracing.
- Integrate with the orchestration runner to automatically create a RunTracker per pipeline step.
- Add a CLI flag (`--audit`) that enables automatic metadata persistence.
