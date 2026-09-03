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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dataexcept import (
    ApiError,
    DataValidationError,
    FileWriteError,
    ParsingError,
    ServiceTimeoutError,
)

from data_platform_lab.manifest import write_manifest

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
    manifest_path: str = ""


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, urllib.error.URLError):
        return isinstance(exc.reason, TimeoutError) or "timed out" in str(exc.reason).lower()
    return "timed out" in str(exc).lower()


def fetch_page(
    base_url: str,
    offset: int = 0,
    limit: int = 10,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    """Fetch one page and classify external failures with DataExcept."""
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
        except urllib.error.HTTPError as exc:
            raise ApiError(url, exc.code, f"HTTP request failed with status {exc.code}") from exc
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
                continue
            if _is_timeout(exc):
                raise ServiceTimeoutError("HTTP API", float(timeout)) from exc
            raise ApiError(url, message=f"API request failed: {exc}") from exc
    else:
        fallback = RuntimeError("API retry loop ended without a result")
        if last_exception is not None:
            raise ApiError(url, message=f"API request failed: {last_exception}") from last_exception
        raise ApiError(url, message=str(fallback)) from fallback

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ParsingError(
            "API response",
            "Response is not valid JSON",
        ) from exc

    if not isinstance(data, list):
        raise DataValidationError(
            "response",
            type(data).__name__,
            f"Expected API response to be a JSON array, got {type(data).__name__}",
        )

    return data


def fetch_all_pages(
    base_url: str,
    page_size: int = 10,
    max_pages: int = 5,
    timeout: int = 10,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch multiple pages with basic pagination."""
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

    Record-level bad data remains a skip-and-log condition rather than
    exceptional control flow.
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


def _write_json(dest: Path, records: object) -> Path:
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(json.dumps(records, indent=2), encoding="utf-8")
    except OSError as exc:
        raise FileWriteError(str(dest), exc) from exc
    return dest


def save_raw(
    records: list[dict[str, Any]],
    output_dir: Path,
    run_id: str,
) -> Path:
    """Save raw API response as JSON under output_dir/run_id/raw.json."""
    dest = _write_json(output_dir / run_id / "raw.json", records)
    logger.info("Raw data saved to %s", dest)
    return dest


def save_processed(
    records: list[dict[str, str | int]],
    output_dir: Path,
    run_id: str,
) -> Path:
    """Save processed records as JSON under output_dir/run_id/processed.json."""
    dest = _write_json(output_dir / run_id / "processed.json", records)
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
    """Run the full API ingestion pipeline."""
    start = time.monotonic()
    run_id = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    errors: list[str] = []

    logger.info("Starting API pipeline run %s against %s", run_id, base_url)

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

    raw_path = save_raw(raw_records, raw_dir, run_id)
    processed = transform_posts(raw_records)
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
    manifest_path = write_manifest(
        pipeline_name="api_ingestion",
        run_id=run_id,
        source=base_url,
        output=str(processed_path),
        row_count=len(processed),
        status="success" if not errors else "failed",
        warnings=errors if errors else None,
        extras={
            "pages_fetched": pages_fetched,
            "total_records": len(raw_records),
            "raw_path": str(raw_path),
        },
    )
    result.manifest_path = str(manifest_path)

    logger.info("Pipeline complete: %s", result)
    return result
