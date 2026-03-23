"""Warehouse — load data into analytical stores and run warehouse-style queries.

Covers SQLite and DuckDB loading, analytical query patterns (CTEs, window
functions, slowly changing dimensions), and gold-layer dataset production.
"""

from __future__ import annotations

from data_platform_lab.warehouse.loader import (
    load_raw_csv,
    load_raw_events_json,
    run_sql_file,
    run_warehouse_pipeline,
)

__all__ = [
    "load_raw_csv",
    "load_raw_events_json",
    "run_sql_file",
    "run_warehouse_pipeline",
]
