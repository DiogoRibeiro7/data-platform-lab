# Exercise 04: Incremental ETL Pipeline

## Problem Statement

Production pipelines rarely reprocess an entire dataset from scratch on every run. Instead, they track what has already been processed and only handle new data — this is incremental or delta processing. Getting this right requires explicit checkpointing, idempotent writes, and safe failure recovery. This exercise builds an incremental ETL pipeline that processes JSONL event files, skipping previously seen records and writing only new output on each run.

## Checkpoint Design

The pipeline persists a checkpoint as a JSON file in `data/checkpoints/`. The checkpoint tracks every event ID that has been successfully processed:

```json
{
  "pipeline_name": "events_etl",
  "last_run_at": "2024-06-01T10:00:00Z",
  "processed_ids": ["evt-001", "evt-002", "evt-003"],
  "total_runs": 3
}
```

| Field | Purpose |
| --- | --- |
| `pipeline_name` | Identifies which pipeline owns this checkpoint |
| `last_run_at` | ISO timestamp of the most recent successful run |
| `processed_ids` | Sorted list of every event ID that has been processed |
| `total_runs` | Count of runs that actually processed new data |

On startup, the pipeline loads the checkpoint (or creates an empty one if the file is missing). After successfully transforming and writing new events, it saves the updated checkpoint with the new IDs added. If no new events are found, the checkpoint is left untouched.

## Data Flow

```text
data/sample/*.json (input events — the pipeline reads .jsonl by convention)
  │
  ├── readEvents / read_events
  │     (read all JSONL files from input dir, skip blank lines)
  │
  ├── loadCheckpoint / load_checkpoint
  │     (load processed_ids set from checkpoint file)
  │
  ├── filter + deduplicate
  │     (remove already-processed IDs and duplicates within the batch)
  │
  ├── transformEvent / transform_event
  │     (enrich each event with derived fields)
  │
  ├── write output → data/output/{timestamp}.jsonl
  │     (one JSONL file per run, only new events)
  │
  └── saveCheckpoint / save_checkpoint
        (persist updated processed_ids — only on success)
```

## Transform Logic

Each raw event is enriched with derived fields:

| Field | Type | Source |
| --- | --- | --- |
| `event_date` | string | Date portion of `timestamp` (YYYY-MM-DD) |
| `hour` | int | Hour extracted from `timestamp` |
| `is_purchase` | boolean | `true` if `type` is `"checkout"` |
| `has_user` | boolean | `true` if `user_id` is present and non-empty |
| `processed_at` | string | ISO timestamp of when the event was processed |

Events missing any of `event_id`, `timestamp`, or `type` are skipped and counted as failed.

## Run Summary

Each run returns a structured summary:

```
{
  pipeline_name: string,
  run_at: string,
  records_seen: int,
  records_skipped: int,
  records_processed: int,
  records_failed: int,
  checkpoint_updated: boolean,
}
```

## Idempotency Strategy

The pipeline is idempotent through three mechanisms:

1. **ID-based deduplication.** The checkpoint stores every successfully processed event ID. On each run, events whose ID appears in the checkpoint are skipped. This means rerunning with the same input data produces zero new output.

2. **Batch-level deduplication.** If the same event ID appears multiple times within a single batch of input files, only the first occurrence is processed. This prevents duplicates even if the source data contains them.

3. **Atomic checkpoint update.** The checkpoint is only written after all events have been successfully transformed and the output file has been written. If the process crashes mid-run, the checkpoint still reflects the previous state, and the next run will reprocess the incomplete batch.

## Failure Handling

| Scenario | Behavior |
| --- | --- |
| Checkpoint file missing | Start fresh with an empty processed set |
| Input directory empty | Return summary with zero counts, checkpoint unchanged |
| Event missing required fields | Skip that event (count as failed), continue processing others |
| Crash before checkpoint save | Checkpoint not advanced — next run reprocesses the same events |
| Rerun after crash | Safe — events from the failed run are not in the checkpoint, so they are reprocessed |
| Duplicate event IDs in input | Deduplicated within the batch — only the first occurrence is processed |

## Edge Cases

- **Clock skew in output filenames.** Output files are named by UTC timestamp. If two runs happen within the same second, the second could overwrite the first. The JavaScript implementation includes milliseconds in the filename to reduce this risk.
- **Growing checkpoint.** The `processed_ids` list grows without bound. For a long-running pipeline processing millions of events, this would need to be replaced with a high-water mark, a bloom filter, or an external store.
- **Non-unique event IDs.** If events genuinely lack unique IDs, this approach cannot deduplicate them. A content hash could be used as a fallback.
- **Concurrent runs.** The checkpoint file has no locking. Running two instances of the pipeline simultaneously could cause both to process the same events and corrupt the checkpoint.
- **Partial output files.** If the process crashes while writing the output JSONL file, a partial file may be left on disk. The next run will reprocess those events and write a new complete file, but the partial file is not cleaned up automatically.

## Differences Between Python and JavaScript

| Aspect | Python | JavaScript |
| --- | --- | --- |
| Data types | `@dataclass Checkpoint`, `@dataclass RunSummary` | Plain objects |
| Set for processed IDs | `set[str]` (native) | `Set` built from array at runtime, serialized as array |
| File I/O | `pathlib.Path`, `json` | `node:fs/promises`, `JSON` |
| Timestamp parsing | `datetime.fromisoformat` | `new Date()` |
| Output filename | `YYYYMMDD_HHMMSS.jsonl` | `YYYYMMDD_HHMMSS_mmm.jsonl` (includes milliseconds) |
| Failure injection | `unittest.mock.patch` on `save_checkpoint` | `_saveCheckpointFn` parameter override |
| Checkpoint update | Only when new events processed | Only when new events processed |

## Usage

### Python

```bash
cd python
python -m pytest tests/test_incremental_etl.py -v
```

### JavaScript

```bash
cd javascript
node --test tests/incremental-etl.test.js
```

## Limitations

- **No high-water mark.** The checkpoint stores all processed IDs rather than a cursor or offset. This is simple but does not scale to millions of events.
- **No output compaction.** Each run writes a separate output file. There is no mechanism to merge output files or deduplicate across runs at the output level.
- **No parallelism.** Events are processed sequentially. A production pipeline might partition events and process partitions in parallel.
- **No schema evolution.** If the event schema changes (new fields, renamed fields), the transform function must be updated manually.
- **File-based only.** Both the checkpoint and the output are JSON files. A production system would use a database or distributed store.

## Future Extensions

- Replace the ID set with a high-water mark (e.g., latest `event_id` or `timestamp`) for bounded checkpoint size.
- Add output compaction: merge small per-run files into larger partitioned files.
- Add a dead-letter file for events that fail transformation.
- Integrate with the validation framework (Exercise 03) to validate events before transformation.
- Add CLI entry points with configurable input/output/checkpoint paths.
- Support partitioned output by date (e.g., `output/event_date=2024-06-01/`).
