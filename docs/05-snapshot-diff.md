# Exercise 05: CDC Snapshot Comparison Tool

## Problem Statement

Many data systems export full table snapshots on a schedule rather than streaming individual row changes. To build incremental downstream pipelines, you need to compare consecutive snapshots and derive the change set: which rows were inserted, updated, or deleted. This is the core of snapshot-based Change Data Capture (CDC). This exercise builds a tool that compares two CSV snapshots and produces structured output files for each change type.

## CDC Concept

Change Data Capture (CDC) refers to any technique that identifies data that has changed between two points in time. There are two broad approaches:

1. **Log-based CDC** — reads the database's transaction log (binlog, WAL) to capture every write as it happens. This is real-time but requires database-level access.

2. **Snapshot-based CDC** — compares two full exports of a table taken at different times. This works with any data source that can produce a full dump, but requires diffing the entire dataset.

This exercise implements snapshot-based CDC. Given an "old" and "new" CSV representing the same table at two points in time, it identifies every row-level change.

## Comparison Algorithm

```text
old_snapshot.csv              new_snapshot.csv
      │                              │
      ▼                              ▼
  read_snapshot                 read_snapshot
      │                              │
      ▼                              ▼
  index_by_key ──────────── index_by_key
      │                              │
      └──────────┬───────────────────┘
                 ▼
          compare keys
         ┌───────┼───────┐
         ▼       ▼       ▼
     in new    in both   in old
     only      snapshots only
         │       │         │
         ▼       ▼         ▼
      INSERT  compare   DELETE
              columns
              ┌───┴───┐
              ▼       ▼
           UPDATE  UNCHANGED
```

Steps:
1. Read both CSV files, parsing into header lists and row dictionaries
2. Index rows by the configurable primary key column(s), validating uniqueness
3. Keys present in the new snapshot but absent from the old are **inserts**
4. Keys present in the old snapshot but absent from the new are **deletes**
5. Keys present in both are compared column by column (excluding key and ignored columns):
   - If any column differs, the row is an **update** and the specific column changes are recorded
   - If all columns match, the row is **unchanged**
6. All changes are sorted by primary key for deterministic output

## Configurable Options

| Option | Purpose | Default |
| --- | --- | --- |
| `key_columns` / `keyColumns` | Column(s) forming the primary key for row matching | Required, no default |
| `ignore_columns` / `ignoreColumns` | Column(s) to exclude from change detection | Empty list |

Composite keys (multiple columns) are supported. Ignored columns are useful for metadata fields like `updated_at` or `sync_id` that change on every export but are not meaningful data changes.

## Output Files

The tool writes four output files to a configurable directory:

| File | Content | When written |
| --- | --- | --- |
| `inserts.csv` | Full row data for newly added rows | Only if inserts > 0 |
| `updates.csv` | Full row data for changed rows, plus a `changed_columns` field | Only if updates > 0 |
| `deletes.csv` | Full row data for removed rows | Only if deletes > 0 |
| `summary.json` | Structured summary with counts and detailed change list | Always |

## Result Objects

### ColumnChange

Describes a single column-level difference within an updated row:

```
{
  column: string,       // e.g. "email"
  old_value: string,    // e.g. "alice@old.com"
  new_value: string,    // e.g. "alice@new.com"
}
```

### RowChange

Describes a single row-level change:

```
{
  change_type: "insert" | "update" | "delete",
  key: { column: value, ... },   // primary key columns and values
  row: { column: value, ... },   // full row (new data for insert/update, old data for delete)
  changed_columns: ColumnChange[],  // only populated for updates
}
```

### DiffSummary

The complete comparison result:

```
{
  old_row_count: int,
  new_row_count: int,
  inserts: int,
  updates: int,
  deletes: int,
  unchanged: int,
  changes: RowChange[],
}
```

## Example: Sample Data

Using the provided `old_snapshot.csv` (7 rows) and `new_snapshot.csv` (8 rows) with `key_columns=["customer_id"]`:

| Key | Change | Details |
| --- | --- | --- |
| C001 | UPDATE | city: Lisbon -> Porto |
| C002 | UPDATE | email: bob.silva@example.com -> bob.silva@newdomain.com |
| C004 | DELETE | Row removed |
| C005 | UPDATE | active: true -> false |
| C008 | INSERT | New row |
| C009 | INSERT | New row |
| C003, C006, C007 | UNCHANGED | No differences |

Summary: 2 inserts, 3 updates, 1 delete, 3 unchanged.

## Assumptions

- **Both snapshots have the same schema.** The tool compares column values positionally by name. If a column exists in one snapshot but not the other, it is ignored.
- **Primary keys are unique.** If duplicate keys are found in either snapshot, the tool raises an error rather than silently picking one.
- **All values are strings.** CSV files are inherently untyped. The comparison is string-based: `"1"` and `"1.0"` are considered different.
- **Whitespace is trimmed.** Leading and trailing whitespace in headers and values is stripped before comparison.

## Differences Between Python and JavaScript

| Aspect | Python | JavaScript |
| --- | --- | --- |
| Data types | `@dataclass ColumnChange`, `RowChange`, `DiffSummary` | Plain objects |
| CSV reading | `csv.DictReader` (stdlib) | Custom `parseCsvLine` parser |
| CSV writing | `csv.DictWriter` (stdlib) | Custom `rowToCsvLine` serializer |
| Key index | `dict[tuple[str, ...], dict]` | `Map<string, Object>` with `\0` separator |
| Error types | `ValueError` for duplicates, `KeyError` for missing columns | `Error` for both |
| Summary output | `dataclasses.asdict` for JSON | Direct object serialization |
| Updates CSV `changed_columns` field | Comma-separated column names | JSON array of change objects |

## Usage

### Python

```bash
cd python
python -m pytest tests/test_snapshot_diff.py -v
```

```python
from pathlib import Path
from data_platform_lab.transform import compare_snapshots, write_diff_files, format_summary

summary = compare_snapshots(
    old_path=Path("data/sample/old_snapshot.csv"),
    new_path=Path("data/sample/new_snapshot.csv"),
    key_columns=["customer_id"],
)

print(format_summary(summary))
write_diff_files(summary, Path("data/output/cdc"))
```

### JavaScript

```bash
cd javascript
node --test tests/snapshot-diff.test.js
```

```javascript
import { compareSnapshots, writeDiffFiles, formatSummary } from "./src/transform/index.js";

const summary = await compareSnapshots(
  "data/sample/old_snapshot.csv",
  "data/sample/new_snapshot.csv",
  ["customer_id"],
);

console.log(formatSummary(summary));
await writeDiffFiles(summary, "data/output/cdc", ["customer_id", "first_name", "last_name", "email", "city", "country", "active"]);
```

## Limitations

- **String comparison only.** Numeric values like `"100"` and `"100.00"` are treated as different. Date formats like `"2024-01-01"` and `"01/01/2024"` are treated as different. A production tool would need type-aware comparison.
- **Full snapshot required.** Both files must contain the complete table. If a row is missing from the new snapshot because of a filter (not a delete), it will be reported as a delete.
- **No ordering semantics.** Row order in the CSV does not affect the result. The tool indexes by primary key, so order is irrelevant.
- **Memory-bound.** Both snapshots are loaded entirely into memory. For very large tables, a streaming or chunk-based approach would be needed.
- **No schema evolution.** If columns are added or removed between snapshots, the tool does not flag this as a structural change.
- **Duplicate keys are an error.** The tool raises rather than handling duplicate primary keys. In practice, duplicates in a snapshot may indicate an upstream bug.

## Future Extensions

- Add type-aware comparison (numeric, date, boolean) with configurable type hints per column.
- Add schema diff detection (new columns, removed columns, renamed columns).
- Support streaming comparison for large files using sorted-merge join.
- Add a CLI entry point for comparing arbitrary CSV files from the command line.
- Integrate with the validation framework to validate snapshots before comparison.
- Support JSON and Parquet snapshot formats alongside CSV.
- Add a `--since` flag for timestamp-based filtering of changes.
