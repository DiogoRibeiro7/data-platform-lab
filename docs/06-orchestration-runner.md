# Exercise 06: Orchestration Runner

## Problem Statement

Data pipelines are rarely a single function call. They are sequences of steps — extract, validate, transform, load, report — each of which can fail, needs timing, and may need to communicate results to downstream steps. Writing this coordination logic ad-hoc in every pipeline leads to duplicated error handling, inconsistent logging, and no standard way to inspect what happened during a run. This exercise builds a reusable pipeline runner that other exercises can use as their execution backbone.

## Execution Model

The runner uses **sequential, fail-fast execution**:

```text
Pipeline("my_etl")
  │
  ├── Step 1: extract ──── success ──→ continue
  │
  ├── Step 2: validate ─── success ──→ continue
  │                    └── fail + retries=2 ──→ retry up to 2 more times
  │
  ├── Step 3: transform ── success ──→ continue
  │
  ├── Step 4: load ─────── fail ─────→ allow_skip=True → mark "skipped", continue
  │                    └── fail ─────→ allow_skip=False → stop pipeline
  │
  └── Step 5: report ──── success ──→ done
```

1. Steps execute in the order they were registered
2. Each step receives a shared **context** dictionary/object
3. If a step succeeds, its return value is stored in `context["step_results"][step_name]`
4. If a step fails:
   - **With retries**: the step is retried up to N additional times
   - **With allow_skip**: the step is marked "skipped" and execution continues
   - **Otherwise**: the pipeline stops immediately (fail-fast)
5. Every step's start time, end time, duration, and attempt count are recorded
6. The pipeline returns a structured result with per-step details and summary counts

## Step Contract

A step is any callable (Python) or async function (JavaScript) that:

1. **Accepts** a single `context` argument (a mutable dict/object shared across all steps)
2. **Returns** any value (stored in `context["step_results"][step_name]` for downstream steps)
3. **Raises/throws** on failure (the error message is captured in the step result)

```python
# Python
def my_step(context: dict) -> Any:
    data = context["step_results"]["extract"]  # read from previous step
    result = process(data)
    context["my_custom_key"] = "side data"     # write to context directly
    return result                               # stored automatically
```

```javascript
// JavaScript
async function myStep(context) {
  const data = context.step_results.extract;   // read from previous step
  const result = await process(data);
  context.myCustomKey = "side data";           // write to context directly
  return result;                                // stored automatically
}
```

Steps should be kept small and focused. Complex logic belongs in the modules being orchestrated, not in the step function itself.

## Step Options

| Option | Type | Default | Behavior |
| --- | --- | --- | --- |
| `retries` | int | 0 | Number of additional attempts after the initial try. A step with `retries=2` runs up to 3 times total. |
| `allow_skip` | bool | false | If true, failure marks the step as "skipped" instead of stopping the pipeline. |

## Result Objects

### StepResult

```
{
  name: string,            // "extract"
  status: "success" | "failed" | "skipped",
  started_at: string,      // ISO 8601 timestamp
  ended_at: string,
  duration_seconds: float,
  result: any,             // return value (null/undefined on failure)
  error: string | null,    // error message on failure
  attempts: int,           // total attempts (1 = no retries needed)
}
```

### PipelineResult

```
{
  pipeline_name: string,
  status: "success" | "failed",
  started_at: string,
  ended_at: string,
  duration_seconds: float,
  steps: StepResult[],
  steps_passed: int,
  steps_failed: int,
  steps_skipped: int,
}
```

Status is `"failed"` if any non-skippable step failed, `"success"` otherwise.

## Example Usage

### Python

```python
from data_platform_lab.orchestration import Pipeline, format_result

def extract(ctx):
    return [{"id": 1}, {"id": 2}]

def transform(ctx):
    records = ctx["step_results"]["extract"]
    return [{"id": r["id"], "processed": True} for r in records]

def load(ctx):
    records = ctx["step_results"]["transform"]
    print(f"Loaded {len(records)} records")

pipeline = Pipeline("my_etl")
pipeline.add_step("extract", extract)
pipeline.add_step("transform", transform)
pipeline.add_step("load", load)

result = pipeline.run()
print(format_result(result))
```

### JavaScript

```javascript
import { Pipeline, formatResult } from "./src/orchestration/index.js";

const pipeline = new Pipeline("my_etl");

pipeline
  .addStep("extract", async (ctx) => [{ id: 1 }, { id: 2 }])
  .addStep("transform", async (ctx) => {
    return ctx.step_results.extract.map((r) => ({ ...r, processed: true }));
  })
  .addStep("load", async (ctx) => {
    console.log(`Loaded ${ctx.step_results.transform.length} records`);
  });

const result = await pipeline.run();
console.log(formatResult(result));
```

### Example Output

```
=== Pipeline: my_etl ===
Status: success
Duration: 0.01s
Steps: 3 total | 3 passed | 0 failed | 0 skipped

  [PASS] extract (0.00s)
  [PASS] transform (0.00s)
  [PASS] load (0.00s)
```

## How Other Modules Can Plug In

The runner is designed as the execution backbone for future exercises:

- **Ingestion** — wrap `run_pipeline` or `run_api_pipeline` as an extract step
- **Validation** — wrap `run_validation` as a validate step; check `report.status` and raise if `"failed"`
- **Transform** — wrap `run_incremental_etl` or `compare_snapshots` as a transform step
- **Storage** — a load step that writes to the target (file, database, warehouse)
- **Observability** — a report step that emits metrics from the pipeline result

Each existing module already returns a structured result object. Wrapping them as pipeline steps is a one-line function:

```python
pipeline.add_step("validate", lambda ctx: run_validation(
    ctx["step_results"]["extract"],
    checks=[...],
    dataset_name="customers",
))
```

## Differences Between Python and JavaScript

| Aspect | Python | JavaScript |
| --- | --- | --- |
| Step functions | Synchronous callables | Async functions (awaited) |
| Data types | `@dataclass StepResult`, `PipelineResult`, `StepDefinition` | Plain objects |
| Pipeline class | `Pipeline` with `add_step()` | `Pipeline` with `addStep()` |
| Timing | `time.perf_counter()` for duration | `Date.now()` for duration |
| Timestamps | `datetime.now(UTC).isoformat()` | `new Date().toISOString()` |
| Context | `dict[str, Any]` | Plain object |
| Chaining | `add_step()` returns `self` | `addStep()` returns `this` |

## Running Tests

```bash
# Python (14 tests)
cd python && python -m pytest tests/test_runner.py -v

# JavaScript (14 tests)
cd javascript && node --test tests/runner.test.js
```

## Limitations

- **Sequential only.** Steps run one at a time. There is no support for parallel step execution or DAG-based dependency resolution.
- **No timeout per step.** A step that hangs will block the entire pipeline. A production runner would need per-step timeouts.
- **No persistent state.** The pipeline result exists only in memory. A production runner would persist run history to a database or file.
- **No conditional steps.** Every registered step runs (or is skipped on failure). There is no support for conditional execution based on context values.
- **No dependency injection.** Steps communicate through the shared context dict, which is simple but untyped and can lead to implicit coupling between steps.
- **Retry has no backoff.** Retries happen immediately with no delay. A production runner would support configurable backoff (fixed, exponential).

## See it in action

The orchestration runner is used in two repository workflows:

- **[Sensor pipeline demo](sensor-pipeline-demo.md)** — the primary showcase.
  Processes sensor events through 5 steps (ingest, validate, deduplicate,
  aggregate, output) with dead-letter routing, hourly aggregation, and
  manifest generation.
- **[Customer ETL](orchestrated-workflow.md)** — a focused tutorial example
  that wires CSV ingestion and validation modules as pipeline steps.

The sensor demo is the recommended starting point for seeing the runner in
a real workflow. See [orchestrated-workflow.md](orchestrated-workflow.md)
for a comparison of both.

---

## Future Extensions

- Add per-step timeout support with configurable duration.
- Add conditional steps that only run when a predicate on the context is true.
- Add parallel step groups (run a set of independent steps concurrently).
- Add persistent run history with a JSON or SQLite backend.
- Add hooks for before/after each step (for logging, metrics, notifications).
- Add exponential backoff for retries.
- Add a YAML-based pipeline definition format for configuration-driven pipelines.
- Integrate with the observability module to emit structured run metrics.
