# Exercise 03: Data Quality Validation Framework

## Problem Statement

Data quality checks are needed at every stage of a pipeline — after ingestion, after transformation, before warehouse loading. Writing ad-hoc validation code for each pipeline leads to inconsistency and duplication. This exercise builds a reusable validation framework that can be applied to any tabular dataset (list of records) with composable, declarative rules.

## Validation Philosophy

The framework follows three principles:

1. **Rules are small, composable functions.** Each rule checks one thing. Complex validation is achieved by combining simple rules, not by building monolithic validators.

2. **Severity separates warnings from blockers.** A failed `CRITICAL` check means the data should not proceed downstream. A failed `WARNING` check means something is off but the pipeline can continue. The runner aggregates these into a single status: `passed`, `warning`, or `failed`.

3. **Results are data, not exceptions.** Every check returns a structured result object with the check name, pass/fail status, severity, a human-readable message, and the indices of failing rows. This makes it easy to log results, build reports, or route bad rows to a dead-letter store.

## Available Rule Types

| Rule | What it checks | Default severity |
| --- | --- | --- |
| `check_required_columns` / `checkRequiredColumns` | All named columns exist in the dataset | CRITICAL |
| `check_no_nulls` / `checkNoNulls` | Specified columns contain no null, undefined, or empty-string values | CRITICAL |
| `check_unique` / `checkUnique` | A column or combination of columns is unique across all rows | CRITICAL |
| `check_numeric_range` / `checkNumericRange` | A numeric column falls within an optional `[min, max]` range | WARNING |
| `check_allowed_values` / `checkAllowedValues` | A column contains only values from a predefined set | WARNING |
| `check_date_format` / `checkDateFormat` | A column matches an expected date format (default: `YYYY-MM-DD`) | WARNING |

All rules accept a `severity` parameter so the caller can override the default.

## Result Objects

### CheckResult

Returned by each individual rule:

```
{
  name: string,           // e.g. "check_no_nulls(email)"
  passed: boolean,
  severity: "warning" | "critical",
  message: string,        // human-readable summary
  failing_rows: int[],    // 0-based row indices (empty if passed)
}
```

### ValidationReport

Returned by the runner after executing all checks:

```
{
  dataset_name: string,
  total_checks: int,
  passed: int,
  failed: int,
  warnings: int,              // failed checks with WARNING severity
  critical_failures: int,     // failed checks with CRITICAL severity
  status: "passed" | "warning" | "failed",
  checks: CheckResult[],
}
```

Status logic:
- `"failed"` — at least one CRITICAL check failed
- `"warning"` — no CRITICAL failures, but at least one WARNING check failed
- `"passed"` — all checks passed

## Example Usage

### Python

```python
from data_platform_lab.validation import (
    Severity,
    check_required_columns,
    check_no_nulls,
    check_unique,
    check_numeric_range,
    check_allowed_values,
    run_validation,
    format_report,
)

records = [
    {"id": 1, "name": "Alice", "age": 30, "status": "active"},
    {"id": 2, "name": "Bob", "age": 25, "status": "active"},
    {"id": 2, "name": "", "age": -5, "status": "unknown"},
]

report = run_validation(
    records,
    checks=[
        (check_required_columns, {"required": ["id", "name", "age", "status"]}),
        (check_no_nulls, {"columns": ["id", "name"]}),
        (check_unique, {"columns": ["id"]}),
        (check_numeric_range, {"column": "age", "min_value": 0, "max_value": 150}),
        (check_allowed_values, {"column": "status", "allowed": {"active", "inactive"}}),
    ],
    dataset_name="customers",
)

print(format_report(report))
```

### JavaScript

```javascript
import {
  Severity,
  checkRequiredColumns,
  checkNoNulls,
  checkUnique,
  checkNumericRange,
  checkAllowedValues,
  runValidation,
  formatReport,
} from "./src/validation/index.js";

const records = [
  { id: 1, name: "Alice", age: 30, status: "active" },
  { id: 2, name: "Bob", age: 25, status: "active" },
  { id: 2, name: "", age: -5, status: "unknown" },
];

const report = runValidation(
  records,
  [
    [checkRequiredColumns, { required: ["id", "name", "age", "status"] }],
    [checkNoNulls, { columns: ["id", "name"] }],
    [checkUnique, { columns: ["id"] }],
    [checkNumericRange, { column: "age", min: 0, max: 150 }],
    [checkAllowedValues, { column: "status", allowed: ["active", "inactive"] }],
  ],
  { datasetName: "customers" },
);

console.log(formatReport(report));
```

### Expected Output

```
=== Validation Report: customers ===
Status: failed
Total checks: 5 | Passed: 2 | Failed: 3
Critical failures: 2 | Warnings: 1

  [FAIL] [critical] check_no_nulls(name): 1 row(s) with null values (rows: 2)
  [FAIL] [critical] check_unique(id): 1 duplicate row(s) found (rows: 2)
  [FAIL] [warning]  check_numeric_range(age): 1 row(s) out of range (rows: 2)
  [PASS] [critical] check_required_columns: all required columns present
  [PASS] [warning]  check_allowed_values(status): ... (but would also fail in this example)
```

## Integration with Other Modules

The validation framework is designed to be called from any pipeline stage:

- **After ingestion** — validate that raw data has the expected shape before writing to bronze.
- **After transformation** — validate that silver-layer data meets quality thresholds before promotion.
- **Before warehouse loading** — gate inserts on passing critical checks.
- **In orchestration** — use `report.status` to decide whether a pipeline step should continue or halt.

The `failing_rows` field in each `CheckResult` can be used to split a dataset into valid and rejected subsets, enabling dead-letter routing without modifying the validation framework itself.

## Differences Between Python and JavaScript

| Aspect | Python | JavaScript |
| --- | --- | --- |
| Data type | `list[dict[str, Any]]` | `object[]` |
| Result types | `@dataclass CheckResult`, `@dataclass ValidationReport` | Plain objects |
| Severity | `Enum` | Frozen object |
| Date validation | `datetime.strptime` | Regex + manual month/day validation |
| Runner interface | `list[tuple[Callable, dict]]` | `Array<[Function, object]>` |

## Limitations

- **Row-level only for column checks.** Rules like `check_required_columns` operate on the column set, not individual rows. A dataset where different rows have different keys will only check against the first row's keys (or all rows' key union, depending on implementation).
- **No cross-column rules.** There is no built-in rule for "column A must be less than column B." This can be added as a custom rule function following the same `CheckResult` contract.
- **No statistical rules.** Rules like "column mean must be within 2 standard deviations of historical mean" are not included. The framework can support them — they are just functions that return `CheckResult`.

## Future Extensions

- Add cross-column comparison rules (e.g., `start_date < end_date`).
- Add statistical/distribution rules (mean, stddev, percentile bounds).
- Add a YAML/JSON rule definition format for configuration-driven validation.
- Integrate with the observability module to emit validation metrics.
- Build a CLI that validates a CSV file against a rule set.
