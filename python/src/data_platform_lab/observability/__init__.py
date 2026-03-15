"""Observability — instrument pipelines with logging, metrics, and lineage.

Covers structured logging, execution timing, row-count metrics, data
lineage tracking, and pipeline health monitoring.
"""

from data_platform_lab.observability.tracker import (
    RunMetadata,
    RunTracker,
    Timer,
    format_run_metadata,
    generate_run_id,
    metadata_to_dict,
)

__all__ = [
    "RunMetadata",
    "RunTracker",
    "Timer",
    "format_run_metadata",
    "generate_run_id",
    "metadata_to_dict",
]
