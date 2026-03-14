"""Ingestion — read data from files, APIs, archives, and external sources.

Covers flat-file parsing (CSV, JSON), HTTP API consumption with pagination
and retries, compressed archive extraction, and log file readers.
"""

from data_platform_lab.ingestion.csv_pipeline import PipelineResult, run_pipeline

__all__ = ["PipelineResult", "run_pipeline"]
