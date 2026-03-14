# Exercise 02: API Ingestion Pipeline

## Problem Statement

Production data pipelines frequently pull data from HTTP APIs. Unlike flat files, API ingestion introduces new concerns: pagination, rate limits, timeouts, retries, and response validation. This exercise builds a pipeline that fetches data from a public REST API, stores the raw response, transforms it into a canonical schema, and writes the processed output — all with proper error handling and run tracking.

## Chosen API

The pipeline uses [JSONPlaceholder](https://jsonplaceholder.typicode.com), a free, public REST API that returns fake data for testing and prototyping. No authentication is required.

**Endpoint:** `GET /posts`

Returns an array of post objects:

```json
{
  "userId": 1,
  "id": 1,
  "title": "sunt aut facere repellat provident...",
  "body": "quia et suscipit\nsuscipit recusandae..."
}
```

**Pagination:** Supported via `_start` (offset) and `_limit` query parameters.

## Data Flow

```text
API endpoint
  │
  ├── fetchPage (single request with offset + limit)
  │     └── retry on transient failure (up to 2 retries)
  │
  ├── fetchAllPages (iterate pages until exhausted or max reached)
  │
  ├── saveRaw → data/raw/api_posts/{run_id}/raw.json
  │     (untouched API response)
  │
  ├── transformPosts
  │     (schema mapping, field derivation, invalid record filtering)
  │
  └── saveProcessed → data/bronze/api_posts/{run_id}/processed.json
        (canonical schema output)
```

## Raw vs Processed Storage

| Layer | Path | Content | Purpose |
| --- | --- | --- | --- |
| Raw | `data/raw/api_posts/{run_id}/raw.json` | Exact API response, no modifications | Preserve original data for debugging and reprocessing |
| Bronze | `data/bronze/api_posts/{run_id}/processed.json` | Transformed, schema-mapped records | First cleaned representation, ready for downstream use |

Each pipeline run generates a unique `run_id` based on the current timestamp (`YYYYMMDD_HHMMSS`), creating isolated folders per run. This makes it easy to compare runs, replay transformations from raw data, and track lineage.

## Canonical Schema

The transformation maps raw posts into a normalized output:

| Field | Type | Source |
| --- | --- | --- |
| `id` | int | `raw.id` |
| `user_id` | int | `raw.userId` |
| `title` | string | `raw.title` |
| `title_length` | int | `len(raw.title)` |
| `body_preview` | string | First 100 characters of `raw.body` |
| `word_count` | int | Word count of `raw.body` |

Records missing `id`, `userId`, `title`, or `body` are skipped during transformation.

## Error Handling

| Scenario | Behavior |
| --- | --- |
| HTTP 5xx | Retry up to 2 times with 1-second delay between attempts |
| Network error | Retry up to 2 times, then record error in result |
| Timeout | Abort request after configurable timeout (default 10s) |
| Malformed JSON | Raise/throw immediately, record error in result |
| Missing fields in record | Skip record during transformation, do not fail the pipeline |
| Complete API failure | Pipeline returns a result with zero records and populated error list |

## Differences Between Python and JavaScript Versions

| Aspect | Python | JavaScript |
| --- | --- | --- |
| HTTP client | `urllib.request.urlopen` (stdlib) | Global `fetch` (Node 20+) |
| Timeout | `timeout` param on `urlopen` | `AbortSignal.timeout()` |
| Retry delay | `time.sleep(1)` | `setTimeout` via `await` |
| Result type | `@dataclass ApiRunResult` | Plain object |
| CLI | `argparse` | `node:util.parseArgs` |
| Logging | `logging` module | `console.info` / `console.warn` |
| Test mocking | `unittest.mock.patch` on `urlopen` | `mock.fn()` replacing `globalThis.fetch` |

## Usage

### Python

```bash
cd python
poetry run python -m data_platform_lab.ingestion.api_cli \
  --url https://jsonplaceholder.typicode.com/posts \
  --raw-dir ../data/raw/api_posts \
  --processed-dir ../data/bronze/api_posts \
  --page-size 10 \
  --max-pages 5
```

### JavaScript

```bash
node javascript/src/ingestion/api-cli.js \
  --url https://jsonplaceholder.typicode.com/posts \
  --raw-dir data/raw/api_posts \
  --processed-dir data/bronze/api_posts \
  --page-size 10 \
  --max-pages 5
```

### Running Tests

```bash
# Python
cd python && poetry run pytest tests/test_api_pipeline.py -v

# JavaScript
cd javascript && node --test tests/api-pipeline.test.js
```

## Limitations

- **Single endpoint only.** The pipeline is built for the `/posts` endpoint. Supporting other resources would require parameterizing the transformation step.
- **No rate limiting.** Requests are fired sequentially without enforced delays between pages. A production pipeline would need configurable rate limiting.
- **No incremental fetching.** Every run fetches from the beginning. There is no high-water mark or checkpoint to fetch only new records.
- **No authentication.** JSONPlaceholder requires none. Real APIs would need API key, OAuth, or other auth handling.
- **Sequential pagination.** Pages are fetched one at a time. Parallel fetching would improve throughput but add complexity.

## Future Improvements

- Add checkpoint support: store the last fetched ID and resume from there on subsequent runs.
- Support parallel page fetching with configurable concurrency.
- Add response caching with ETag/If-Modified-Since headers.
- Integrate with the observability module for structured run metrics.
- Write processed output as CSV in addition to JSON for warehouse loading exercises.
- Add a configurable schema mapping so the same pipeline can ingest different API resources.
