# Sensor Pipeline Demo

A showcase workflow that processes sensor events through ingestion, validation,
dead-letter routing, deduplication, hourly aggregation, and structured output.

---

## Quick start

### Python

```bash
cd python
poetry run python -m data_platform_lab.sensor_demo \
  --data-dir ../data/sample \
  --output-dir ../data/silver/sensor_demo
```

### JavaScript

```bash
cd javascript
node src/sensor-demo.js \
  --data-dir ../data/sample \
  --output-dir ../data/silver/sensor_demo
```

---

## What it does

The demo reads `data/sample/sensor_events.json` (16 JSONL sensor readings)
and processes them through 5 orchestrated steps:

| Step | What it does | Result |
|------|-------------|--------|
| **ingest** | Read JSONL file line by line | 16 events parsed |
| **validate** | Check required fields, numeric value, parseable timestamp | 15 accepted, 1 rejected |
| **deduplicate** | Remove duplicates by sensor_id + timestamp | 14 unique events |
| **aggregate** | Compute per-sensor hourly stats and per-location summary | 5 sensor-hour buckets, 3 locations |
| **output** | Write 5 output files + manifest | accepted, dead letter, aggregates, summary |

---

## Data quality issues in sample data

| Event | Issue | Outcome |
|-------|-------|---------|
| sensor-02 @ 08:10 | `value: null` | Rejected to dead_letter.jsonl |
| sensor-01 @ 08:20 (duplicate) | Exact duplicate of previous event | Removed, logged to dead_letter.jsonl |
| sensor-01 @ 08:15 | Extreme value (-40.0 celsius) | Accepted (structurally valid) |
| sensor-05 @ 08:00 | Unit is `fahrenheit` not `celsius` | Accepted (unit mismatch is not a rejection rule) |

---

## Output files

Written to the output directory:

| File | Contents |
|------|----------|
| `accepted.jsonl` | 14 valid, deduplicated events |
| `dead_letter.jsonl` | 2 entries (1 rejected, 1 duplicate) with status and reason |
| `hourly_aggregates.csv` | Per-sensor, per-hour min/max/avg/count |
| `location_summary.csv` | Per-location event count, sensor count, type list |
| `summary.json` | Full run summary with counts and sensor list |

A manifest is also written to `data/manifests/`.

---

## Contrast with the e-commerce demo

| | E-commerce demo | Sensor demo |
|---|---|---|
| **Domain** | Tabular (customers, products, orders) | Event stream (sensor readings) |
| **Input format** | CSV files (4 tables) | JSONL (single event stream) |
| **Processing** | Per-table clean + merge | Per-event validate + deduplicate |
| **Quality handling** | Validation rules + warnings | Dead-letter routing |
| **Aggregation** | SQL queries via SQLite | In-memory hourly + location buckets |
| **Orchestration** | Direct function calls | Pipeline runner with 5 steps |
| **Output** | Cleaned CSVs + SQL reports | JSONL + CSV aggregates + JSON summary |
| **Pattern** | Batch ELT | Stream-style event processing |

Both demos are runnable from a fresh clone with one command.

---

## Orchestration runner usage

This demo is the repository's primary example of the orchestration runner
(Exercise 06) in a real workflow. All 5 steps are registered with
`Pipeline.add_step()` and executed through `pipeline.run(context)`.

The e-commerce demo intentionally does not use the runner — it shows that
simple pipelines work fine with direct function calls. The sensor demo shows
the value of the runner when you need structured step reporting, shared
context, and fail-fast control.

For a comparison of both approaches and a smaller tutorial example
(customer_etl), see [orchestrated-workflow.md](orchestrated-workflow.md).

---

## Tests

```bash
# Python
cd python && python -m pytest tests/test_sensor_demo.py -v

# JavaScript
cd javascript && node --test tests/sensor-demo.test.js
```

---

## Architecture

```text
sensor_events.json
       │
       ▼
  ┌─────────────────────────────────┐
  │ Pipeline("sensor_demo")         │
  │                                 │
  │  Step 1: ingest                 │
  │    Read JSONL line by line      │
  │                                 │
  │  Step 2: validate               │
  │    Required fields + types      │
  │    → accepted[] / rejected[]    │
  │                                 │
  │  Step 3: deduplicate            │
  │    Key: sensor_id::timestamp    │
  │    → deduplicated[] / dupes[]   │
  │                                 │
  │  Step 4: aggregate              │
  │    Hourly per-sensor stats      │
  │    Per-location summary         │
  │                                 │
  │  Step 5: output                 │
  │    Write 5 files + manifest     │
  └─────────────────────────────────┘
       │
       ▼
  accepted.jsonl
  dead_letter.jsonl
  hourly_aggregates.csv
  location_summary.csv
  summary.json
  manifest JSON
```
