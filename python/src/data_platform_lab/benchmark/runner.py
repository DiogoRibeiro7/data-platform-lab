"""Benchmark runner — compares sequential, threaded, and async file processing."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Synthetic data generation ────────────────────────────────────────────

SAMPLE_NAMES = [
    ("Alice", "Martins"), ("Bob", "Silva"), ("Carol", "Santos"),
    ("David", "Costa"), ("Eva", "Ferreira"), ("Frank", "Oliveira"),
    ("Grace", "Rodrigues"), ("Hugo", "Almeida"), ("Iris", "Pereira"),
    ("Jack", "Sousa"),
]

COUNTRIES = ["Portugal", "Spain", "France", "Italy", "Germany"]
CITIES = ["Lisbon", "Madrid", "Paris", "Rome", "Berlin"]


def generate_test_files(
    output_dir: str | Path,
    num_files: int = 50,
    rows_per_file: int = 100,
) -> list[Path]:
    """Generate synthetic CSV files for benchmarking.

    Each file contains customer rows with fields:
    customer_id, first_name, last_name, email, city, country, created_at

    Some rows have intentional quality issues:
    - ~10% empty email
    - ~10% inconsistent country casing
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []

    for f_idx in range(num_files):
        file_path = output_dir / f"batch_{f_idx:04d}.csv"
        with file_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow([
                "customer_id", "first_name", "last_name",
                "email", "city", "country", "created_at",
            ])
            for r_idx in range(rows_per_file):
                row_id = f_idx * rows_per_file + r_idx
                first, last = SAMPLE_NAMES[row_id % len(SAMPLE_NAMES)]
                country = COUNTRIES[row_id % len(COUNTRIES)]
                city = CITIES[row_id % len(CITIES)]

                # Intentional quality issues
                email = ""
                if row_id % 10 != 0:  # ~10% empty
                    email = f"{first.lower()}.{last.lower()}.{row_id}@example.com"
                if row_id % 10 == 3:
                    country = country.upper()  # casing issue
                if row_id % 10 == 7:
                    country = country.lower()

                writer.writerow([
                    f"C{row_id:06d}", first, last, email, city, country,
                    "2024-01-15",
                ])
        files.append(file_path)

    return files


# ── Single-file processing ───────────────────────────────────────────────

@dataclass
class FileResult:
    """Result of processing a single file."""
    file_name: str
    rows_read: int = 0
    rows_valid: int = 0
    rows_invalid: int = 0
    duration_seconds: float = 0.0


def process_file(input_path: Path, output_dir: Path) -> FileResult:
    """Process a single CSV file: read, validate, clean, write output.

    Validation rules:
    - customer_id must be non-empty
    - first_name and last_name must be non-empty
    - country is normalised to title case
    - email is lowercased if present

    Returns a FileResult with counts and timing.
    """
    start = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / input_path.name

    rows_read = 0
    rows_valid = 0
    rows_invalid = 0
    cleaned_rows: list[dict[str, str]] = []

    with input_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames is not None
        fieldnames = list(reader.fieldnames)

        for row in reader:
            rows_read += 1
            # Validate required fields
            if not row.get("customer_id") or not row.get("first_name") or not row.get("last_name"):
                rows_invalid += 1
                continue
            # Clean
            row["country"] = row.get("country", "").strip().title()
            if row.get("email"):
                row["email"] = row["email"].strip().lower()
            rows_valid += 1
            cleaned_rows.append(row)

    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    duration = time.perf_counter() - start
    return FileResult(
        file_name=input_path.name,
        rows_read=rows_read,
        rows_valid=rows_valid,
        rows_invalid=rows_invalid,
        duration_seconds=round(duration, 6),
    )


# ── Strategy implementations ─────────────────────────────────────────────

def run_sequential(files: list[Path], output_dir: Path) -> list[FileResult]:
    """Process all files sequentially in a for-loop."""
    return [process_file(f, output_dir) for f in files]


def run_threaded(
    files: list[Path],
    output_dir: Path,
    max_workers: int = 4,
) -> list[FileResult]:
    """Process all files using a thread pool."""
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(process_file, f, output_dir) for f in files]
        return [fut.result() for fut in futures]


async def _run_async_inner(
    files: list[Path],
    output_dir: Path,
    max_workers: int = 4,
) -> list[FileResult]:
    """Async wrapper: delegates to thread executor for file I/O."""
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        tasks = [
            loop.run_in_executor(pool, process_file, f, output_dir)
            for f in files
        ]
        return list(await asyncio.gather(*tasks))


def run_async(
    files: list[Path],
    output_dir: Path,
    max_workers: int = 4,
) -> list[FileResult]:
    """Process all files using asyncio with a thread executor."""
    return asyncio.run(_run_async_inner(files, output_dir, max_workers))


# ── Benchmark orchestrator ───────────────────────────────────────────────

@dataclass
class StrategyResult:
    """Result for one benchmark strategy run."""
    strategy: str
    total_seconds: float = 0.0
    files_processed: int = 0
    total_rows_read: int = 0
    total_rows_valid: int = 0
    total_rows_invalid: int = 0


@dataclass
class BenchmarkReport:
    """Complete benchmark report."""
    num_files: int = 0
    rows_per_file: int = 0
    total_rows: int = 0
    strategies: list[StrategyResult] = field(default_factory=list)


def run_benchmark(
    work_dir: str | Path,
    num_files: int = 50,
    rows_per_file: int = 100,
    max_workers: int = 4,
) -> BenchmarkReport:
    """Run the full benchmark: generate files, then time each strategy.

    Parameters
    ----------
    work_dir : path
        Root directory for generated input/output files.
    num_files : int
        Number of CSV files to generate.
    rows_per_file : int
        Rows per generated file.
    max_workers : int
        Thread/worker count for parallel strategies.

    Returns
    -------
    BenchmarkReport
        Structured report with timing for each strategy.
    """
    work_dir = Path(work_dir)
    input_dir = work_dir / "input"
    report = BenchmarkReport(
        num_files=num_files,
        rows_per_file=rows_per_file,
        total_rows=num_files * rows_per_file,
    )

    logger.info("Generating %d files (%d rows each)...", num_files, rows_per_file)
    files = generate_test_files(input_dir, num_files, rows_per_file)

    strategies: list[tuple[str, Any]] = [
        ("sequential", lambda fs, od: run_sequential(fs, od)),
        ("threaded", lambda fs, od: run_threaded(fs, od, max_workers)),
        ("async", lambda fs, od: run_async(fs, od, max_workers)),
    ]

    for name, fn in strategies:
        output_dir = work_dir / f"output_{name}"
        logger.info("Running strategy: %s", name)
        start = time.perf_counter()
        results = fn(files, output_dir)
        elapsed = time.perf_counter() - start

        strategy_result = StrategyResult(
            strategy=name,
            total_seconds=round(elapsed, 4),
            files_processed=len(results),
            total_rows_read=sum(r.rows_read for r in results),
            total_rows_valid=sum(r.rows_valid for r in results),
            total_rows_invalid=sum(r.rows_invalid for r in results),
        )
        report.strategies.append(strategy_result)
        logger.info("  %s: %.4fs (%d files)", name, elapsed, len(results))

    return report


def format_report(report: BenchmarkReport) -> str:
    """Format a benchmark report as a human-readable string."""
    lines = [
        "=== Benchmark Report ===",
        (
            f"Files: {report.num_files}  |  Rows/file: {report.rows_per_file}"
            f"  |  Total rows: {report.total_rows}"
        ),
        "",
        (
            f"{'Strategy':<15} {'Time (s)':>10} {'Files':>8}"
            f" {'Rows':>10} {'Valid':>10} {'Invalid':>10}"
        ),
        "-" * 65,
    ]
    for s in report.strategies:
        lines.append(
            f"{s.strategy:<15} {s.total_seconds:>10.4f} {s.files_processed:>8} "
            f"{s.total_rows_read:>10} {s.total_rows_valid:>10} {s.total_rows_invalid:>10}"
        )

    # Relative speedup vs sequential
    if len(report.strategies) >= 2:
        seq_time = report.strategies[0].total_seconds
        if seq_time > 0:
            lines.append("")
            lines.append("Relative to sequential:")
            for s in report.strategies[1:]:
                if s.total_seconds > 0:
                    speedup = seq_time / s.total_seconds
                    lines.append(f"  {s.strategy}: {speedup:.2f}x")

    return "\n".join(lines)


def save_report(report: BenchmarkReport, output_path: str | Path) -> None:
    """Save a benchmark report as JSON."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(asdict(report), fh, indent=2)
