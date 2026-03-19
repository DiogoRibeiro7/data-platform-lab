"""Streaming — process event data with validation, deduplication, and aggregation.

Simulates near-real-time event processing locally using JSONL input files.
Events are validated, deduplicated, and routed to accepted or dead-letter
outputs with per-sensor aggregate statistics.
"""

from data_platform_lab.streaming.processor import (
    EventResult,
    StreamSummary,
    compute_aggregates,
    deduplicate_key,
    process_stream,
    validate_event,
)

__all__ = [
    "EventResult",
    "StreamSummary",
    "compute_aggregates",
    "deduplicate_key",
    "process_stream",
    "validate_event",
]
