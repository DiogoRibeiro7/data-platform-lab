# Read-only, SHA-pinned ONS pay serving API

This slice makes the [ONS gross median weekly pay Silver/Gold models](ons-pay-analytics.md)
available to a future application without selecting Supabase, a cloud database,
or an S3 supplier. FastAPI provides the local HTTP contract; DuckDB reads
the existing versioned Parquet directly. **The API never writes data.**

## Start locally

Complete [registry ingestion](registry-handoff-ingestion.md) and
[ONS pay curation](ons-pay-analytics.md) first. The curation output must have
an `ons_pay_curation_<run-id>.json` receipt under `data/manifests/` and three
Silver/Gold Parquet files in their versioned directories.

```bash
cd python
poetry install --extras serving
poetry run data-platform-lab ons-pay-serve --data-root ../data
```

The server listens on **127.0.0.1:8000**, not the public network, by default.
Browse [local interactive API documentation](http://127.0.0.1:8000/docs), or:

```bash
# Discover full, explicit version/registry/source pins; no 'latest' alias.
curl -s http://127.0.0.1:8000/v1/ons-pay/vintages

# Copy registry_commit and source_sha256 from the response above.
curl -G http://127.0.0.1:8000/v1/ons-pay/geography-year \
  --data-urlencode 'registry_commit=<40-hex-char-commit>' \
  --data-urlencode 'source_sha256=<64-hex-char-source-sha>' \
  --data-urlencode 'model_version=1' \
  --data-urlencode 'geography=Local Authority District' \
  --data-urlencode 'pay_year=2024' \
  --data-urlencode 'limit=20'

curl -G http://127.0.0.1:8000/v1/ons-pay/areas/E09000001/trend \
  --data-urlencode 'registry_commit=<40-hex-char-commit>' \
  --data-urlencode 'source_sha256=<64-hex-char-source-sha>' \
  --data-urlencode 'start_year=2019' \
  --data-urlencode 'end_year=2025'
```

`<...>` means replace that placeholder with the actual response field, without
the brackets. `E09000001` is just an example area code; the path filter is exact.

## HTTP contract

| Route | Purpose | Parameters |
| --- | --- | --- |
| `GET /health` | Process liveness, not data readiness | None |
| `GET /v1/ons-pay/vintages` | Discover verified, pinned source/model combinations | None |
| `GET /v1/ons-pay/geography-year` | Unweighted geography-type × year summary | Required `registry_commit`, `source_sha256`; optional `model_version=1`, `geography`, `pay_year`, `limit`, `offset` |
| `GET /v1/ons-pay/areas/{area_code}/trend` | An area's annual reported pay and nominal YoY | Required pins and area code; optional `model_version=1`, `start_year`, `end_year`, `limit`, `offset` |

Results have deterministic ordering, `total` (before pagination), `limit`,
`offset`, `rows`, and the selected `vintage` metadata. `limit` defaults to 50
and cannot exceed 100. `offset` is zero-based. A vintage is the **exact triple**
`model_version` + 40-character registry commit + 64-character CSV SHA-256;
there is **no default source or "latest" promotion**. The two SHA pins are
required for both data endpoints. Vintage metadata includes the verified input
manifest SHA-256, upstream publisher/snapshot/retrieval date, citation and
license for attribution.

Client-visible behavior:

- `200`: the requested exact vintage was served, or an empty filtered result.
- `404`: the exact requested vintage was never curated.
- `422`: invalid or missing pins, filters, pagination, or year range.
- `503`: a malformed/conflicting receipt, missing/altered Parquet output, or
  a broken serving table contract; the response does not disclose local paths.

`/v1/ons-pay/geography-year` reports **unweighted medians among areas**.
It is *not* the UK's national median wage; geography types may overlap.
`/v1/ons-pay/areas/{area_code}/trend` preserves `null` for suppressed pay
and missing year-on-year change, and returns **nominal**, not real-wage growth.

## Integrity, scope, and migration path

Only successful `ons_pay_curation` receipts and the three **known**,
content-addressed v1 Parquet paths are accepted. Paths are reconstructed from
the curated root, model version, commit, and SHA, then compared exactly to the
receipt. The catalog rejects unexpected paths, symlinks, malformed receipts,
duplicate vintage receipts that disagree, and missing files. Before serving
data, it re-hashes all three files and checks the queried table's row count,
per-row lineage pins, and typed response schema.
DuckDB gets the fixed Parquet path and every filter as SQL parameters; clients
cannot provide a SQL expression, path, or table name.

This is a **trusted local development API**, not a production deployment:
there is no authentication, rate limiting, CORS policy for browsers, cross-
process locking, or multi-writer transaction support. Do not bind `--host
0.0.0.0` or expose its port on a public network without adding an
authenticated gateway and operational controls. A SHA-256 receipt detects
accidental or unauthorized *file drift relative to that receipt*, but is not
a signed provenance attestation if a local attacker can rewrite both receipt
and files. Like the curation pipeline, it assumes one trusted writer.

The application contract can later be implemented with PostgreSQL, hosted
Supabase, or another destination without changing the canonical source
registry or the vintage selection rule. That supplier decision remains open.

## Offline contract tests

```bash
cd python
poetry install --extras serving
poetry run pytest tests/test_ons_pay_serving.py -q
poetry run ruff check src/ tests/
poetry run mypy src/
```

The tests run the **real DuckDB curation** against synthetic ONS records,
exercise the API through an in-process HTTP client, and check pagination,
missingness, lineage, multiple versions, SQL input safety, replay semantics,
invalid requests, broken receipts, altered files, path traversal, and symlinks.
No database account, network call, API server process, or supplier credentials
are required.
