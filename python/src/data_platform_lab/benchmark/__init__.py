"""Benchmark — compare ingestion strategies for file-processing workloads."""

from data_platform_lab.benchmark.runner import (
    generate_test_files,
    process_file,
    run_async,
    run_benchmark,
    run_sequential,
    run_threaded,
)

__all__ = [
    "generate_test_files",
    "process_file",
    "run_async",
    "run_benchmark",
    "run_sequential",
    "run_threaded",
]
