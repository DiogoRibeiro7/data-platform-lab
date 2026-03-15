"""CLI entry point for the API ingestion pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from data_platform_lab.ingestion.api_pipeline import run_api_pipeline


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the API CLI."""
    parser = argparse.ArgumentParser(
        description="Ingest posts from a JSON API and save raw + processed output.",
    )
    parser.add_argument(
        "--url",
        default="https://jsonplaceholder.typicode.com/posts",
        help="Base URL for the API endpoint (default: JSONPlaceholder /posts).",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw/api_posts"),
        help="Directory for raw JSON output (default: data/raw/api_posts).",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=Path("data/bronze/api_posts"),
        help="Directory for processed JSON output (default: data/bronze/api_posts).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=10,
        help="Number of records per API page (default: 10).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum number of pages to fetch (default: 5).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="HTTP request timeout in seconds (default: 10).",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug-level logging.",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Parse arguments, run the pipeline, and print the summary."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )

    result = run_api_pipeline(
        base_url=args.url,
        raw_dir=args.raw_dir,
        processed_dir=args.processed_dir,
        page_size=args.page_size,
        max_pages=args.max_pages,
        timeout=args.timeout,
    )

    print("\n=== API Pipeline Run Summary ===")
    print(f"  Run ID:          {result.run_id}")
    print(f"  API URL:         {result.api_url}")
    print(f"  Pages fetched:   {result.pages_fetched}")
    print(f"  Total records:   {result.total_records}")
    print(f"  Records written: {result.records_written}")
    print(f"  Raw path:        {result.raw_path}")
    print(f"  Processed path:  {result.processed_path}")
    print(f"  Duration (s):    {result.duration_seconds}")
    if result.errors:
        print(f"  Errors:          {result.errors}")
    print("================================\n")

    if result.errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
