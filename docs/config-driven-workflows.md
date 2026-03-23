# Config-Driven Workflows

Three workflows support JSON config files as an alternative to CLI flags.
Config files make runs reproducible and shareable without long command lines.

---

## Which workflows support config

| Workflow | Config keys | CLI module |
|----------|------------|------------|
| Streaming processor | `input`, `output_dir`, `pipeline_name`, `lateness_threshold` | `streaming.cli` |
| Warehouse loader | `data_dir`, `db_path`, `report_dir`, `sql_dir` | `warehouse.cli` |
| Benchmark | `work_dir`, `num_files`, `rows_per_file`, `max_workers` | `benchmark.cli` |

---

## Precedence

```
defaults  <  config file  <  CLI flags
```

- If no `--config` is provided, CLI flags and built-in defaults apply.
- If `--config` is provided, config values override defaults.
- CLI flags always override config values when explicitly provided.

---

## Config format

Plain JSON objects with `snake_case` keys (matching the platform convention).

```json
{
  "input": "data/sample/sensor_events.json",
  "output_dir": "data/silver/sensor_stream",
  "lateness_threshold": 600
}
```

- Keys must match the known set for each workflow.
- Unknown keys produce a warning but are not errors.
- Missing required keys (e.g. `input` for streaming) are an error unless
  provided via CLI flag.

---

## Example config files

The repository includes ready-to-use examples in `configs/`:

### `configs/streaming.json`

```json
{
  "input": "data/sample/sensor_events.json",
  "output_dir": "data/silver/sensor_stream",
  "pipeline_name": "sensor_stream",
  "lateness_threshold": 600
}
```

### `configs/warehouse.json`

```json
{
  "data_dir": "data/sample",
  "db_path": "data/gold/warehouse.db",
  "report_dir": "data/gold/warehouse",
  "sql_dir": "sql"
}
```

### `configs/benchmark.json`

```json
{
  "work_dir": "data/benchmark",
  "num_files": 100,
  "rows_per_file": 200,
  "max_workers": 4
}
```

---

## Running with config

### Python

```bash
cd python

# Streaming
poetry run python -m data_platform_lab.streaming.cli --config ../configs/streaming.json

# Warehouse
poetry run python -m data_platform_lab.warehouse.cli --config ../configs/warehouse.json

# Benchmark
poetry run python -m data_platform_lab.benchmark.cli --config ../configs/benchmark.json
```

### JavaScript

```bash
# Streaming
node javascript/src/streaming/cli.js --config configs/streaming.json

# Warehouse
node javascript/src/warehouse/cli.js --config configs/warehouse.json

# Benchmark
node javascript/src/benchmark/cli.js --config configs/benchmark.json
```

### Overriding config with CLI flags

```bash
# Use config but override the lateness threshold
poetry run python -m data_platform_lab.streaming.cli \
  --config ../configs/streaming.json \
  --lateness-threshold 1200
```

---

## Error handling

| Condition | Behaviour |
|-----------|-----------|
| Config file not found | Error with clear message, exit 1 |
| Invalid JSON | Error with parse details, exit 1 |
| Config is not a JSON object | Error, exit 1 |
| Unknown keys | Warning logged, execution continues |
| Missing required keys (not in CLI) | Error, exit 1 |

---

## Implementation

The config system is a shared module used by all three CLIs:

- **Python**: `data_platform_lab.config` — `load_config()`, `validate_config()`, `merge_config()`
- **JavaScript**: `src/config.js` — `loadConfig()`, `validateConfig()`, `mergeConfig()`

Both provide the same API and enforce the same precedence model.

---

## Tests

```bash
# Python (12 tests)
cd python && python -m pytest tests/test_config.py -v

# JavaScript (12 tests)
cd javascript && node --test tests/config.test.js
```

---

## Limitations

- Config files are JSON only (no YAML, TOML, or .env support).
- Config keys use `snake_case`, matching platform conventions. CLI flags
  use `--kebab-case`. The mapping is automatic (e.g. `output_dir` in
  config corresponds to `--output-dir` on the CLI).
- Config does not support environment variable interpolation.
- No config inheritance or includes — each file is self-contained.
- Workflows not listed above (CSV ingestion, API ingestion, demo) do
  not support `--config` yet. They could be added using the same pattern.
