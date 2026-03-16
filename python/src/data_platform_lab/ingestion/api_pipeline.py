"""API ingestion pipeline for JSONPlaceholder posts.

Fetches paginated data from a REST API, saves raw JSON responses,
transforms records into a canonical schema, and writes processed output.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ApiRunResult:
    """Summary produced by a single API pipeline run."""

    run_id: str
    api_url: str
    pages_fetched: int
    total_records: int
    records_written: int
    raw_path: str
    processed_path: str
    errors: list[str]
    duration_seconds: float


def fetch_page(
    base_url: str,
    offset: int = 0,
    limit: int = 10,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    """Fetch a single page from the API. Returns parsed JSON list.

    Raises urllib.error.URLError on network failure, TimeoutError on timeout,
    ValueError on non-JSON response.
    """
    separator = "&" if "?" in base_url else "?"
    url = f"{base_url}{separator}_start={offset}&_limit={limit}"
    logger.debug("Fetching %s", url)

    max_retries = 2
    last_exception: BaseException | None = None

    for attempt in range(1 + max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                raw = resp.read()
            break
        except urllib.error.HTTPError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exception = exc
            if attempt < max_retries:
                logger.warning(
                    "Transient failure (attempt %d/%d): %s",
                    attempt + 1,
                    1 + max_retries,
                    exc,
                )
                time.sleep(1)
            else:
                if isinstance(exc, (TimeoutError, OSError)) and "timed out" in str(
                    exc
                ):
                    raise TimeoutError(str(exc)) from exc
                raise
    else:
        # Should not reach here, but satisfy the type checker.
        msg = "Max retries exceeded"
        raise urllib.error.URLError(msg) if last_exception is None else last_exception

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        msg = f"Response is not valid JSON: {raw[:200]!r}"
        raise ValueError(msg) from exc

    if not isinstance(data, list):
        msg = f"Expected a JSON array, got {type(data).__name__}"
        raise ValueError(msg)

    return data  # type: ignore[return-value]


def fetch_all_pages(
    base_url: str,
    page_size: int = 10,
    max_pages: int = 5,
    timeout: int = 10,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch multiple pages with basic pagination.

    Returns (all_records, pages_fetched).
    Stops when a page returns fewer records than page_size or max_pages reached.
    """
    all_records: list[dict[str, Any]] = []
    pages_fetched = 0

    for page_num in range(max_pages):
        offset = page_num * page_size
        page = fetch_page(base_url, offset=offset, limit=page_size, timeout=timeout)
        pages_fetched += 1
        all_records.extend(page)
        logger.info(
            "Page %d: fetched %d records (offset=%d)",
            page_num + 1,
            len(page),
            offset,
        )
        if len(page) < page_size:
            logger.info("Received partial page; stopping pagination.")
            break

    return all_records, pages_fetched


_REQUIRED_FIELDS = {"id", "userId", "title", "body"}


def transform_posts(
    raw_records: list[dict[str, Any]],
) -> list[dict[str, str | int]]:
    """Transform raw post records into a canonical schema.

    Output schema: id, user_id, title, title_length, body_preview (first 100
    chars), word_count.  Skips records missing required fields.
    """
    transformed: list[dict[str, str | int]] = []

    for record in raw_records:
        if not _REQUIRED_FIELDS.issubset(record):
            logger.warning("Skipping record with missing fields: %s", record)
            continue

        try:
            rid = int(record["id"])
            uid = int(record["userId"])
        except (TypeError, ValueError):
            logger.warning(
                "Skipping record with non-numeric id/userId: id=%r, userId=%r",
                record.get("id"),
                record.get("userId"),
            )
            continue

        body = str(record["body"]) if record["body"] is not None else ""
        title = str(record["title"]) if record["title"] is not None else ""

        transformed.append(
            {
                "id": rid,
                "user_id": uid,
                "title": title,
                "title_length": len(title),
                "body_preview": body[:100],
                "word_count": len(body.split()),
            }
        )

    return transformed


def save_raw(
    records: list[dict[str, Any]],
    output_dir: Path,
    run_id: str,
) -> Path:
    """Save raw API response as JSON under output_dir/run_id/raw.json."""
    dest = output_dir / run_id / "raw.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(records, indent=2), encoding="utf-8")
    logger.info("Raw data saved to %s", dest)
    return dest


def save_processed(
    records: list[dict[str, str | int]],
    output_dir: Path,
    run_id: str,
) -> Path:
    """Save processed records as JSON under output_dir/run_id/processed.json."""
    dest = output_dir / run_id / "processed.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(records, indent=2), encoding="utf-8")
    logger.info("Processed data saved to %s", dest)
    return dest


def run_api_pipeline(
    base_url: str = "https://jsonplaceholder.typicode.com/posts",
    raw_dir: Path = Path("data/raw/api_posts"),
    processed_dir: Path = Path("data/bronze/api_posts"),
    page_size: int = 10,
    max_pages: int = 5,
    timeout: int = 10,
) -> ApiRunResult:
    """Run the full API ingestion pipeline.

    1. Generate a timestamped run_id
    2. Fetch all pages
    3. Save raw response
    4. Transform records
    5. Save processed output
    6. Return summary
    """
    start = time.monotonic()
    run_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    errors: list[str] = []

    logger.info("Starting API pipeline run %s against %s", run_id, base_url)

    # --- Fetch ---
    try:
        raw_records, pages_fetched = fetch_all_pages(
            base_url,
            page_size=page_size,
            max_pages=max_pages,
            timeout=timeout,
        )
    except Exception as exc:
        error_msg = f"Fetch failed: {exc}"
        logger.error(error_msg)
        errors.append(error_msg)
        duration = time.monotonic() - start
        return ApiRunResult(
            run_id=run_id,
            api_url=base_url,
            pages_fetched=0,
            total_records=0,
            records_written=0,
            raw_path="",
            processed_path="",
            errors=errors,
            duration_seconds=round(duration, 3),
        )

    # --- Save raw ---
    raw_path = save_raw(raw_records, raw_dir, run_id)

    # --- Transform ---
    processed = transform_posts(raw_records)

    # --- Save processed ---
    processed_path = save_processed(processed, processed_dir, run_id)

    duration = time.monotonic() - start
    result = ApiRunResult(
        run_id=run_id,
        api_url=base_url,
        pages_fetched=pages_fetched,
        total_records=len(raw_records),
        records_written=len(processed),
        raw_path=str(raw_path),
        processed_path=str(processed_path),
        errors=errors,
        duration_seconds=round(duration, 3),
    )
    logger.info("Pipeline complete: %s", result)
    return result
