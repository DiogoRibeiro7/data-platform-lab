"""Tests for the API ingestion pipeline.

All HTTP interactions are mocked — no real network calls are made.
"""

from __future__ import annotations

import json
import urllib.error
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from data_platform_lab.ingestion.api_pipeline import (
    ApiRunResult,
    fetch_all_pages,
    fetch_page,
    run_api_pipeline,
    save_processed,
    save_raw,
    transform_posts,
)


# ---------------------------------------------------------------------------
# Helper: mock response object for urllib.request.urlopen
# ---------------------------------------------------------------------------

class MockResponse:
    """Minimal stand-in for the object returned by ``urlopen``."""

    def __init__(self, data: bytes, status: int = 200) -> None:
        self._data = data
        self.status = status

    def read(self) -> bytes:
        return self._data

    def __enter__(self) -> MockResponse:
        return self

    def __exit__(self, *args: object) -> None:
        pass


def _json_bytes(obj: Any) -> bytes:
    return json.dumps(obj).encode()


SAMPLE_POSTS: list[dict[str, Any]] = [
    {"userId": 1, "id": 1, "title": "first", "body": "body one"},
    {"userId": 1, "id": 2, "title": "second", "body": "body two"},
    {"userId": 2, "id": 3, "title": "third", "body": "body three"},
]


# ===================================================================
# TestFetchPage
# ===================================================================

class TestFetchPage:
    """Tests for ``fetch_page``."""

    @patch("data_platform_lab.ingestion.api_pipeline.urllib.request.urlopen")
    def test_fetch_page_success(self, mock_urlopen: Any) -> None:
        """Mock a successful JSON response, verify parsing."""
        mock_urlopen.return_value = MockResponse(_json_bytes(SAMPLE_POSTS))

        result = fetch_page("https://example.com/posts", offset=0, limit=10)

        assert result == SAMPLE_POSTS
        mock_urlopen.assert_called_once()

    @patch("data_platform_lab.ingestion.api_pipeline.urllib.request.urlopen")
    def test_fetch_page_http_error(self, mock_urlopen: Any) -> None:
        """Mock a 500 error, verify it raises."""
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="https://example.com/posts",
            code=500,
            msg="Internal Server Error",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )

        with pytest.raises(urllib.error.HTTPError):
            fetch_page("https://example.com/posts")

    @patch("data_platform_lab.ingestion.api_pipeline.urllib.request.urlopen")
    def test_fetch_page_timeout(self, mock_urlopen: Any) -> None:
        """Mock a timeout, verify it raises."""
        mock_urlopen.side_effect = urllib.error.URLError(
            reason=TimeoutError("timed out")
        )

        with pytest.raises((urllib.error.URLError, TimeoutError)):
            fetch_page("https://example.com/posts")

    @patch("data_platform_lab.ingestion.api_pipeline.urllib.request.urlopen")
    def test_fetch_page_malformed_json(self, mock_urlopen: Any) -> None:
        """Mock a non-JSON response, verify ValueError."""
        mock_urlopen.return_value = MockResponse(b"<html>not json</html>")

        with pytest.raises(ValueError, match="not valid JSON"):
            fetch_page("https://example.com/posts")


# ===================================================================
# TestFetchAllPages
# ===================================================================

class TestFetchAllPages:
    """Tests for ``fetch_all_pages``."""

    @patch("data_platform_lab.ingestion.api_pipeline.fetch_page")
    def test_fetch_all_pages_pagination(self, mock_fetch: Any) -> None:
        """Mock multiple full pages, verify accumulation and stop at max."""
        page_a = [{"id": i} for i in range(10)]
        page_b = [{"id": i} for i in range(10, 20)]

        mock_fetch.side_effect = [page_a, page_b]

        records, pages = fetch_all_pages(
            "https://example.com/posts", page_size=10, max_pages=2
        )

        assert pages == 2
        assert len(records) == 20
        assert mock_fetch.call_count == 2

    @patch("data_platform_lab.ingestion.api_pipeline.fetch_page")
    def test_fetch_all_pages_stops_on_empty(self, mock_fetch: Any) -> None:
        """Mock a page returning fewer records, verify early stop."""
        full_page = [{"id": i} for i in range(10)]
        partial_page = [{"id": 10}, {"id": 11}]

        mock_fetch.side_effect = [full_page, partial_page]

        records, pages = fetch_all_pages(
            "https://example.com/posts", page_size=10, max_pages=5
        )

        assert pages == 2
        assert len(records) == 12
        # Should NOT have attempted a third page.
        assert mock_fetch.call_count == 2


# ===================================================================
# TestTransformPosts
# ===================================================================

class TestTransformPosts:
    """Tests for ``transform_posts``."""

    def test_transform_posts_valid(self) -> None:
        """Transform valid posts, verify output schema."""
        result = transform_posts(SAMPLE_POSTS)

        assert len(result) == 3
        first = result[0]
        assert first["id"] == 1
        assert first["user_id"] == 1
        assert first["title"] == "first"
        assert first["title_length"] == 5
        assert first["body_preview"] == "body one"
        assert first["word_count"] == 2

    def test_transform_posts_skips_invalid(self) -> None:
        """Records missing required fields are skipped."""
        bad_records: list[dict[str, Any]] = [
            {"userId": 1, "id": 1},  # missing title & body
            {"title": "hi", "body": "there"},  # missing id & userId
            {"userId": 1, "id": 2, "title": "ok", "body": "fine"},  # valid
        ]

        result = transform_posts(bad_records)
        assert len(result) == 1
        assert result[0]["id"] == 2

    def test_transform_posts_skips_null_id(self) -> None:
        """Records with null or non-numeric id/userId are skipped."""
        records: list[dict[str, Any]] = [
            {"id": None, "userId": 1, "title": "t", "body": "b"},
            {"id": 1, "userId": "not_a_number", "title": "t", "body": "b"},
            {"id": 2, "userId": 2, "title": "t", "body": "b"},
        ]
        result = transform_posts(records)
        assert len(result) == 1
        assert result[0]["id"] == 2

    def test_transform_posts_null_body(self) -> None:
        """Records with None body get empty string instead of crashing."""
        records: list[dict[str, Any]] = [
            {"id": 1, "userId": 1, "title": "t", "body": None},
        ]
        result = transform_posts(records)
        assert len(result) == 1
        assert result[0]["body_preview"] == ""
        assert result[0]["word_count"] == 0

    def test_transform_posts_body_preview(self) -> None:
        """Body preview is truncated to 100 chars."""
        long_body = "a" * 200
        records: list[dict[str, Any]] = [
            {"userId": 1, "id": 1, "title": "t", "body": long_body},
        ]

        result = transform_posts(records)
        assert len(result[0]["body_preview"]) == 100  # type: ignore[arg-type]


# ===================================================================
# TestSaveRawAndProcessed
# ===================================================================

class TestSaveFiles:
    """Tests for ``save_raw`` and ``save_processed``."""

    def test_save_raw_creates_file(self, tmp_path: Path) -> None:
        """Verify save_raw writes JSON correctly."""
        path = save_raw(SAMPLE_POSTS, tmp_path, "20260101_120000")
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data) == 3

    def test_save_processed_creates_file(self, tmp_path: Path) -> None:
        """Verify save_processed writes JSON correctly."""
        processed = transform_posts(SAMPLE_POSTS)
        path = save_processed(processed, tmp_path, "20260101_120000")
        assert path.exists()
        data = json.loads(path.read_text())
        assert len(data) == 3
        assert "user_id" in data[0]


# ===================================================================
# TestRunApiPipeline
# ===================================================================

class TestRunApiPipeline:
    """Tests for ``run_api_pipeline``."""

    @patch("data_platform_lab.ingestion.api_pipeline.fetch_all_pages")
    def test_run_api_pipeline_success(
        self, mock_fetch_all: Any, tmp_path: Path
    ) -> None:
        """Mock fetch, run full pipeline, verify files created and summary."""
        mock_fetch_all.return_value = (SAMPLE_POSTS, 1)

        raw_dir = tmp_path / "raw"
        processed_dir = tmp_path / "bronze"

        result = run_api_pipeline(
            base_url="https://example.com/posts",
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            page_size=10,
            max_pages=1,
        )

        assert isinstance(result, ApiRunResult)
        assert result.pages_fetched == 1
        assert result.total_records == 3
        assert result.records_written == 3
        assert result.errors == []
        assert Path(result.raw_path).exists()
        assert Path(result.processed_path).exists()

        raw_data = json.loads(Path(result.raw_path).read_text())
        assert len(raw_data) == 3

        processed_data = json.loads(Path(result.processed_path).read_text())
        assert len(processed_data) == 3

    @patch("data_platform_lab.ingestion.api_pipeline.fetch_all_pages")
    def test_run_api_pipeline_api_failure(
        self, mock_fetch_all: Any, tmp_path: Path
    ) -> None:
        """Mock complete API failure, verify graceful handling."""
        mock_fetch_all.side_effect = urllib.error.URLError("Connection refused")

        raw_dir = tmp_path / "raw"
        processed_dir = tmp_path / "bronze"

        result = run_api_pipeline(
            base_url="https://example.com/posts",
            raw_dir=raw_dir,
            processed_dir=processed_dir,
        )

        assert isinstance(result, ApiRunResult)
        assert result.pages_fetched == 0
        assert result.total_records == 0
        assert result.records_written == 0
        assert len(result.errors) == 1
        assert "Fetch failed" in result.errors[0]
        assert result.raw_path == ""
        assert result.processed_path == ""
