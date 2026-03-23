# Orchestration in the Repository

How the orchestration runner (Exercise 06) is used across the repository,
and which demo path to follow depending on what you want to learn.

---

## The orchestration runner

The `Pipeline` class (Exercise 06) provides sequential step execution with
shared context, structured timing, retry logic, skip-on-failure, and
formatted result reporting. See [06-orchestration-runner.md](06-orchestration-runner.md)
for the full API.

---

## Where the runner is used

### Sensor pipeline demo (recommended)

The [sensor pipeline demo](sensor-pipeline-demo.md) is the primary
orchestration showcase. It wires 5 steps through the `Pipeline` class:

```text
Pipeline("sensor_demo")
  ├── ingest      — read JSONL events
  ├── validate    — check fields + types, route invalid to dead letter
  ├── deduplicate — remove duplicates by sensor_id::timestamp
  ├── aggregate   — compute hourly per-sensor and per-location stats
  └── output      — write 5 output files + manifest
```

This is the recommended path for studying how the orchestration runner
works with real data and real validation logic.

```bash
# Python
cd python && poetry run python -m data_platform_lab.sensor_demo

# JavaScript
cd javascript && node src/sensor-demo.js
```

### Customer ETL (tutorial example)

The customer ETL workflow is a smaller, focused example that demonstrates
the same `Pipeline` class using CSV ingestion and validation modules.

It processes only `customers.csv` through 5 steps: extract, validate
(with `allow_skip=True`), clean, load, report. It exists primarily as
a teaching tool for Exercise 06, showing how to wire existing modules
as pipeline steps.

```bash
# Python
cd python
poetry run python -c "
from data_platform_lab.orchestration.customer_etl import run_customer_etl
from data_platform_lab.orchestration.runner import format_result
result = run_customer_etl('../data/sample/customers.csv', '../data/bronze/customers_cleaned.csv')
print(format_result(result))
"
```

| Language | Module | Tests |
|----------|--------|-------|
| Python | `orchestration/customer_etl.py` | `tests/test_customer_etl.py` |
| JavaScript | `orchestration/customer-etl.js` | `tests/customer-etl.test.js` |

---

## What about the e-commerce demo?

The [e-commerce demo](end-to-end-demo.md) (`demo.py` / `demo.js`) processes
all 4 tables but does **not** use the orchestration runner. It uses direct
function calls with `RunTracker` for observability.

This is intentional. The two demos illustrate different approaches:

| | E-commerce demo | Sensor demo |
|---|---|---|
| **Execution model** | Direct function calls | `Pipeline` runner (Exercise 06) |
| **Observability** | `RunTracker` (Exercise 07) | `Pipeline` timing + `RunTracker` |
| **Step coordination** | Implicit (code order) | Explicit (registered steps with context) |
| **When to use which** | Simple pipelines with few steps | Complex pipelines needing retry, skip, structured reporting |

Both are valid patterns. The e-commerce demo shows that not every pipeline
needs a formal runner. The sensor demo shows the value of one when you want
structured step reporting, retry logic, and fail-fast control.

---

## Which demo should a visitor run first?

1. **Start with the e-commerce demo** — it processes the most familiar data
   (customers, products, orders) and produces the clearest output.
2. **Then run the sensor demo** — it shows the same pipeline concepts
   (ingest, validate, output) but through the orchestration runner, with
   event-stream data and dead-letter routing.
3. **Study customer_etl** — if you want to understand the `Pipeline` class
   mechanics in isolation before looking at the full sensor demo.

---

## Runner features demonstrated

| Feature | Customer ETL | Sensor demo |
|---------|-------------|-------------|
| Sequential steps | Yes (5 steps) | Yes (5 steps) |
| Shared context | Yes | Yes |
| Step results | Yes | Yes |
| `allow_skip` | Yes (validate step) | No (all fail-fast) |
| `format_result` | Yes | Yes |
| Manifest generation | No | Yes |
| Dead-letter routing | No | Yes |
| Aggregation output | No | Yes (hourly + location) |
