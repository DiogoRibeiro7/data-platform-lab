"""Machine-readable, read-only release readiness gates for two pinned ONS vintages."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Exit 0 for passing policies, 1 for breaches and 2 for invalid evidence."""
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-compare",
        description="Compare two SHA-pinned Silver vintages before candidate publication.",
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--model-version", type=int, choices=(1,), default=1)
    parser.add_argument("--baseline-registry-commit", required=True)
    parser.add_argument("--baseline-source-sha256", required=True)
    parser.add_argument("--candidate-registry-commit", required=True)
    parser.add_argument("--candidate-source-sha256", required=True)
    parser.add_argument("--max-added-rows", type=int)
    parser.add_argument("--max-removed-rows", type=int)
    parser.add_argument("--max-revised-rows", type=int)
    parser.add_argument("--max-newly-missing-pay-rows", type=int)
    parser.add_argument("--max-missing-pay-increase-pp", type=float)
    parser.add_argument("--max-absolute-pay-revision-gbp", type=float)
    parser.add_argument("--examples-per-change", type=int, default=5)
    args = parser.parse_args(argv)

    # The base CLI remains usable without the optional DuckDB serving extra.
    try:
        from data_platform_lab.serving.catalog import VintageKey
        from data_platform_lab.serving.ons_pay_compare import (
            ComparisonError,
            compare_ons_pay_vintages,
        )
    except ModuleNotFoundError:
        parser.error("install the serving extra first: poetry install --extras serving")

    try:
        report = compare_ons_pay_vintages(
            data_root=args.data_root,
            baseline=VintageKey(
                args.model_version, args.baseline_registry_commit, args.baseline_source_sha256
            ),
            candidate=VintageKey(
                args.model_version, args.candidate_registry_commit, args.candidate_source_sha256
            ),
            max_added_rows=args.max_added_rows,
            max_removed_rows=args.max_removed_rows,
            max_revised_rows=args.max_revised_rows,
            max_newly_missing_pay_rows=args.max_newly_missing_pay_rows,
            max_missing_pay_increase_pp=args.max_missing_pay_increase_pp,
            max_absolute_pay_revision_gbp=args.max_absolute_pay_revision_gbp,
            examples_per_change=args.examples_per_change,
        )
    except ComparisonError as exc:
        parser.error(str(exc))

    print(json.dumps(report.to_dict(), indent=2))
    if report.status == "breach":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
