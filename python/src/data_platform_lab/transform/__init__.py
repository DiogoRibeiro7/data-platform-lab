"""Transform — clean, reshape, enrich, and aggregate datasets.

Covers column mapping, type casting, deduplication, filtering, derived
fields, and bronze-to-silver-to-gold promotion logic.
"""

from data_platform_lab.transform.incremental_etl import (
    Checkpoint,
    RunSummary,
    load_checkpoint,
    read_events,
    run_incremental_etl,
    save_checkpoint,
    transform_event,
)
from data_platform_lab.transform.snapshot_diff import (
    ColumnChange,
    DiffSummary,
    RowChange,
    compare_rows,
    compare_snapshots,
    format_summary,
    index_by_key,
    read_snapshot,
    write_diff_files,
)

__all__ = [
    "Checkpoint",
    "ColumnChange",
    "DiffSummary",
    "RowChange",
    "RunSummary",
    "compare_rows",
    "compare_snapshots",
    "format_summary",
    "index_by_key",
    "load_checkpoint",
    "read_events",
    "read_snapshot",
    "run_incremental_etl",
    "save_checkpoint",
    "transform_event",
    "write_diff_files",
]
