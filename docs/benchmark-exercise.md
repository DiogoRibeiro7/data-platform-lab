# Exercise 09 — Benchmark: Ingestion Strategies

Compare sequential, threaded/concurrent, and async/pool-based file processing
to understand throughput trade-offs in I/O-heavy data ingestion.

---

## What is being measured

The benchmark runs the same logical workload — read CSV files, validate rows,
clean data, write output — using three different execution strategies. Each
strategy processes the same generated files and produces identical results.
The only difference is how the work is scheduled.

### Workload

1. Generate N synthetic CSV files (default: 50 files, 100 rows each)
2. Each file contains customer records with intentional quality issues:
   - ~10% empty email addresses
   - ~10% inconsistent country casing (UPPERCASE or lowercase)
3. Processing per file: read CSV, validate required fields, normalise country
   to title case, lowercase email, write cleaned output

### Strategies

| Strategy | Python | JavaScript |
|----------|--------|------------|
| Sequential | `for` loop | `for...of` with `await` |
| Parallel | `ThreadPoolExecutor` | `Promise.all` (unlimited) |
| Async/Pool | `asyncio` + thread executor | Worker pool with concurrency limit |

---

## Running the benchmark

### Python

```bash
cd python
poetry run python -m data_platform_lab.benchmark.cli \
  --num-files 50 --rows-per-file 100 --max-workers 4
```

### JavaScript

```bash
node javascript/src/benchmark/cli.js \
  --num-files 50 --rows-per-file 100 --max-workers 4
```

### CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--work-dir` | `../data/benchmark` | Root directory for generated files |
| `--num-files` | 50 | Number of CSV files to generate |
| `--rows-per-file` | 100 | Rows per file |
| `--max-workers` | 4 | Thread/worker pool size |

---

## Example output

```
=== Benchmark Report ===
Files: 50  |  Rows/file: 100  |  Total rows: 5000

Strategy          Time (s)    Files      Rows     Valid   Invalid
-----------------------------------------------------------------
sequential          0.1842       50      5000      5000         0
threaded            0.0731       50      5000      5000         0
async               0.0698       50      5000      5000         0

Relative to sequential:
  threaded: 2.52x
  async: 2.64x
```

The report is also saved as `benchmark_report.json` in the work directory.

---

## Report format

```json
{
  "num_files": 50,
  "rows_per_file": 100,
  "total_rows": 5000,
  "strategies": [
    {
      "strategy": "sequential",
      "total_seconds": 0.1842,
      "files_processed": 50,
      "total_rows_read": 5000,
      "total_rows_valid": 5000,
      "total_rows_invalid": 0
    }
  ]
}
```

---

## Interpreting results

### What the benchmark shows

- **I/O concurrency helps.** File reads and writes release the GIL (Python)
  or are natively async (Node.js), so parallel strategies complete faster.
- **Thread pool ≈ async pool** for this workload. Both are I/O-bound with
  light CPU work. The difference between them is scheduling overhead.
- **Speedup depends on file count.** With 5 files the overhead of pool
  creation may dominate. With 500 files the parallelism pays off clearly.

### What the benchmark does NOT show

- **Network I/O**: This benchmark uses local filesystem I/O. Network-bound
  workloads (API calls, S3 downloads) would show larger concurrency benefits.
- **CPU-bound work**: Heavy computation (e.g. Parquet encoding, compression)
  won't benefit from Python threads due to the GIL. Use `ProcessPoolExecutor`
  or a compiled library for CPU-bound parallelism.
- **Distributed scaling**: This is single-machine concurrency. Real data
  pipelines scale across workers using tools like Spark, Dask, or Ray.

### Caveats

- **Machine-dependent**: Timing varies with disk speed, CPU cores, OS
  scheduler, and background processes. Run the benchmark multiple times.
- **Warm cache**: The second and third strategies benefit from OS file cache
  warming during the first strategy's run. This is intentionally not
  controlled for — real pipelines face the same cache effects.
- **Small files**: The synthetic files are small (~5KB each). Larger files
  would shift the bottleneck from file-open overhead to actual I/O throughput.
- **No contention**: Each strategy writes to its own output directory. Shared
  output would introduce lock contention in the threaded strategy.

---

## Tests

```bash
# Python (13 tests)
cd python && python -m pytest tests/test_benchmark.py -v

# JavaScript (14 tests)
cd javascript && node --test tests/benchmark.test.js
```

Tests verify:
- File generation correctness and quality issues
- Single-file processing: row counts, country cleaning, email lowercasing
- All three strategies produce identical row counts
- Benchmark report shape and content
- Report formatting and JSON serialisation

Tests do NOT assert timing values — those are machine-dependent and would
produce flaky tests.

---

## Design decisions

**Why file I/O instead of HTTP?** File processing is fully local,
deterministic, and doesn't require a running server. HTTP benchmarks
would need a mock server or real endpoints, adding fragile dependencies.

**Why synthetic data?** Generated files ensure consistent sizing and
reproducible quality issue distribution. Using sample data would limit
the benchmark to 4 small files.

**Why threads (not processes) in Python?** File I/O releases the GIL, so
threads provide real parallelism for this workload without the overhead of
process spawning and inter-process communication.

**Why three strategies?** Sequential is the baseline. The parallel options
(unlimited vs pool-limited) show the trade-off between maximum concurrency
and controlled resource usage — a real decision in production pipelines.

---

## Extension ideas

- Add a `ProcessPoolExecutor` strategy (Python) for CPU-bound comparison
- Add a `worker_threads` strategy (Node.js) for true parallelism
- Benchmark with larger files (10K+ rows) to shift the bottleneck
- Add a simulated network delay to model API ingestion
- Compare results across different `max_workers` values
- Add a warmup run before timing to control for cache effects
