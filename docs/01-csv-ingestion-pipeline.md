# Exercise 01: Multi-File CSV Ingestion and Cleaning Pipeline

## Problem Statement

Raw data often arrives as multiple CSV files in a landing directory. Before any downstream processing can happen, these files need to be read, validated, cleaned, merged, and deduplicated into a single consistent output.

This exercise builds a pipeline that:

1. Scans an input directory for all `.csv` files.
2. Validates that each file contains the required columns.
3. Standardizes column names across files (lowercase, trimmed, spaces replaced with underscores).
4. Trims whitespace from all string fields.
5. Removes exact duplicate rows across the merged dataset.
6. Writes a single cleaned CSV to the specified output path.
7. Produces a summary report with counts for files processed, rows read, rows written, duplicates removed, and rejected files.

This is a common first step in any batch ETL pipeline and exercises several foundational data engineering skills: file I/O, schema validation, data cleaning, and pipeline reporting.

## Implementation Approach

Both implementations follow the same architecture — a set of small, composable functions orchestrated by a single `run_pipeline` / `runPipeline` entry point.

### Pipeline Stages

```text
input directory
  │
  ├── file_1.csv ──→ read ──→ validate columns ──→ standardize headers ──→ trim fields ──┐
  ├── file_2.csv ──→ read ──→ validate columns ──→ standardize headers ──→ trim fields ──┤
  └── file_3.csv ──→ read ──→ validate columns ─✗ reject (missing columns)               │
                                                                                          │
                                                                              merge all rows
                                                                                          │
                                                                              deduplicate │
                                                                                          │
                                                                              write output CSV
                                                                                          │
                                                                              return summary
```

### Core Functions

| Function | Purpose |
| --- | --- |
| `read_csv_file` / `readCsvFile` | Parse a single CSV file into headers and rows |
| `validate_columns` / `validateColumns` | Check that required columns exist in the headers |
| `standardize_headers` / `standardizeHeaders` | Normalize column names to `lowercase_with_underscores` |
| `trim_fields` / `trimFields` | Strip leading/trailing whitespace from every field |
| `deduplicate` / `deduplicate` | Remove exact duplicate rows, report how many were removed |
| `run_pipeline` / `runPipeline` | Orchestrate all stages and return a structured result |

### Design Decisions

- **Standard library only.** Both implementations use only built-in modules. Python uses `csv`, `pathlib`, `logging`, `dataclasses`, and `argparse`. JavaScript uses `node:fs/promises`, `node:path`, and `node:util`.
- **No pandas / no external CSV libraries.** The point of this exercise is to work close to the data. Using a DataFrame library would hide the mechanics.
- **Simple CSV parsing.** The JavaScript version includes basic quoted-field handling (fields wrapped in double quotes that may contain commas). Neither version handles every edge case of RFC 4180 — that is an intentional limitation.
- **Deduplication by exact row match.** Rows are compared as serialized strings. This is simple and fast for small datasets. It does not handle semantic duplicates (e.g., "Portugal" vs "portugal").
- **Rejected files, not rejected rows.** If a file is missing required columns, the entire file is skipped. Individual malformed rows within an otherwise valid file are handled gracefully but not routed to a separate dead-letter output — that is left for the validation exercise.

## Differences Between Python and JavaScript Versions

| Aspect | Python | JavaScript |
| --- | --- | --- |
| CSV parsing | `csv.reader` (standard library) | Custom string splitting with quote handling |
| File I/O | Synchronous (`pathlib.Path.read_text`) | Async (`node:fs/promises`) |
| Data structure | `list[list[str]]` | `string[][]` |
| Result type | `@dataclass PipelineResult` | Plain object |
| CLI | `argparse` | `node:util.parseArgs` |
| Logging | `logging` module with named logger | `console.info` / `console.warn` |
| Entry point | `python -m data_platform_lab.ingestion` | `node javascript/src/ingestion/cli.js` |
| Tests | pytest with `tmp_path` fixture | `node:test` with manual temp dirs |

The functional behavior is identical. The same input produces the same output in both languages.

## Usage

### Python

```bash
cd python
poetry run python -m data_platform_lab.ingestion \
  --input-dir ../data/sample \
  --output ../data/bronze/merged_customers.csv \
  --required-columns customer_id,email
```

### JavaScript

```bash
node javascript/src/ingestion/cli.js \
  --input-dir data/sample \
  --output data/bronze/merged_customers.csv \
  --required-columns customer_id,email
```

### Running Tests

```bash
# Python
cd python && poetry run pytest tests/test_csv_pipeline.py -v

# JavaScript
cd javascript && node --test tests/csv-pipeline.test.js
```

## Limitations

- **No streaming.** Files are read entirely into memory. This is fine for sample-sized data but would not scale to multi-gigabyte files.
- **No encoding detection.** Both versions assume UTF-8. Files in other encodings will produce garbled output or errors.
- **No schema evolution.** All files must share the same column set (after standardization). Files with extra columns will include those columns; files with fewer columns will have empty fields.
- **No dead-letter output.** Rejected files are reported in the summary but their contents are not written to a separate location.
- **Basic quoting.** The JavaScript CSV parser handles double-quoted fields but does not support escaped quotes within quoted fields.

## Possible Extensions

- Add encoding detection (e.g., `chardet` in Python).
- Write rejected files and rows to a dead-letter directory with error annotations.
- Support configurable deduplication keys (instead of full-row comparison).
- Add output format options (JSON, Parquet).
- Stream large files line-by-line instead of loading everything into memory.
- Add checkpointing so the pipeline can resume after partial failure.
- Integrate with the observability module for structured logging and metrics emission.
