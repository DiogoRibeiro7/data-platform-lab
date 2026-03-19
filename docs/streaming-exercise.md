# Exercise 08 — Streaming Event Processor

Process sensor events one by one with validation, deduplication, dead-letter
routing, and per-sensor aggregation.

---

## What is being simulated

Real streaming systems (Kafka, Kinesis, Flink) process events individually as
they arrive. This exercise simulates that pattern locally: events are read line
by line from a JSONL file and processed sequentially. Each event is validated,
deduplicated, and routed to either an accepted output or a dead-letter file.

The simulation is batch (reads a file) but the processing logic is per-event —
no event depends on any other event's outcome, and the processor never loads all
events into memory before starting.

---

## Event model

Input events come from `data/sample/sensor_events.json` (JSONL format):

```json
{
  "sensor_id": "sensor-01",
  "type": "temperature",
  "value": 22.5,
  "unit": "celsius",
  "location": "warehouse-A",
  "timestamp": "2024-06-01T08:00:00Z"
}
```

### Required fields

| Field | Type | Constraint |
|-------|------|------------|
| `sensor_id` | string | Non-empty |
| `type` | string | Non-empty |
| `value` | number | Not null, must be finite |
| `unit` | string | Non-empty |
| `location` | string | Non-empty |
| `timestamp` | string | Parseable as ISO 8601 |

### Sample data quality issues

The 16 sample events include intentional problems:

| Line | Issue | Outcome |
|------|-------|---------|
| 9 | Extreme value (-40.0 celsius) | Accepted (structurally valid) |
| 12-13 | Exact duplicate (same sensor + timestamp) | First accepted, second deduplicated |
| 14 | Null value | Rejected |
| 15 | Unit mismatch (fahrenheit) | Accepted (structurally valid) |

Expected result: 14 accepted, 1 rejected, 1 duplicate.

---

## Output layout

All outputs are written to the specified output directory:

| File | Contents |
|------|----------|
| `accepted.jsonl` | Valid, deduplicated events (one JSON object per line) |
| `dead_letter.jsonl` | Rejected and duplicate events with status and reason |
| `summary.json` | Structured run summary with counts and aggregates |

### Dead-letter format

Each line in `dead_letter.jsonl`:

```json
{
  "event": { "sensor_id": "sensor-02", "value": null, ... },
  "status": "rejected",
  "reason": "null value"
}
```

### Summary format

```json
{
  "pipeline_name": "sensor_stream",
  "run_at": "2024-06-01T12:00:00+00:00",
  "duration_seconds": 0.003,
  "status": "success",
  "events_seen": 16,
  "events_accepted": 14,
  "events_rejected": 1,
  "events_duplicate": 1,
  "dead_letter_count": 2,
  "aggregates": {
    "by_sensor": {
      "sensor-01": { "count": 5, "min_value": -40.0, "max_value": 23.5, "avg_value": 10.38 }
    },
    "by_type": { "temperature": 9, "humidity": 4, "pressure": 2 },
    "by_location": { "warehouse-A": 10, "warehouse-B": 2, "warehouse-C": 2 }
  },
  "rejection_reasons": { "null value": 1, "duplicate event": 1 }
}
```

---

## Running the exercise

### Python

```bash
cd python
poetry run python -m data_platform_lab.streaming.cli \
  --input ../data/sample/sensor_events.json \
  --output-dir ../data/silver/sensor_stream
```

### JavaScript

```bash
node javascript/src/streaming/cli.js \
  --input data/sample/sensor_events.json \
  --output-dir data/silver/sensor_stream
```

Both produce identical output counts and file structures.

---

## Running the tests

```bash
# Python (20 tests)
cd python && python -m pytest tests/test_streaming.py -v

# JavaScript (20 tests)
cd javascript && node --test tests/streaming.test.js
```

---

## Processing pipeline

```text
  JSONL input
      │
      ▼
  Parse JSON line ──── malformed? ──► dead_letter.jsonl
      │
      ▼
  Validate fields ──── invalid? ────► dead_letter.jsonl
      │
      ▼
  Deduplicate ──────── seen key? ───► dead_letter.jsonl
      │
      ▼
  accepted.jsonl
      │
      ▼
  Compute aggregates (by_sensor, by_type, by_location)
      │
      ▼
  summary.json
```

---

## Deduplication strategy

Events are deduplicated by composite key: `{sensor_id}::{timestamp}`.

First occurrence wins. Later duplicates are routed to the dead-letter output
with status `"duplicate"` and reason `"duplicate event"`.

This models at-least-once delivery where the same reading may arrive multiple
times. The dedup key assumes a sensor produces at most one reading per timestamp.

---

## Limitations compared with real streaming systems

| This exercise | Real streaming |
|---------------|----------------|
| Reads from a file | Consumes from a topic/stream |
| Processes all events in one run | Runs continuously |
| In-memory dedup set | Distributed state store (RocksDB, Redis) |
| No windowing | Time windows, session windows, tumbling windows |
| No watermarks | Event-time watermarks for late data |
| No backpressure | Consumer flow control |
| No partitioning | Parallel consumers across partitions |
| Single-node | Distributed across workers |

The exercise teaches the core concepts — per-event validation, dead-letter
routing, deduplication, and aggregation — without the operational complexity.

---

## Extension ideas

- Add time-window aggregation (e.g. 5-minute tumbling windows)
- Add threshold alerting (flag sensors exceeding a configurable range)
- Watch a directory for new JSONL files instead of reading one file
- Add a checkpoint so reruns skip already-processed files
- Convert aggregates to a time-series output format
