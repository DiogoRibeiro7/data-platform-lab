"""Read-only, exit-code-driven ONS pay operational monitoring CLI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-monitor",
        description="Read-only integrity, freshness and audit checks for a pinned ONS vintage.",
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--registry-commit", required=True, help="Exact 40-hex Git commit")
    parser.add_argument("--source-sha256", required=True, help="Exact 64-hex canonical CSV hash")
    parser.add_argument("--model-version", type=int, choices=(1,), default=1)
    parser.add_argument("--max-source-age-days", type=int, default=None)
    parser.add_argument("--max-refresh-age-hours", type=float, default=None)
    parser.add_argument("--max-missing-pay-percent", type=float, default=None)
    parser.add_argument("--format", choices=("json", "prometheus"), default="json")
    args = parser.parse_args(argv)
    # Keep the base platform CLI importable without the optional DuckDB extra.
    try:
        from data_platform_lab.observability.ons_pay_monitor import (
            MonitorError,
            format_prometheus,
            monitor_ons_pay,
        )
        from data_platform_lab.serving.catalog import VintageKey
    except ModuleNotFoundError:
        parser.error("install the serving extra first: poetry install --extras serving")
    try:
        report = monitor_ons_pay(
            data_root=args.data_root,
            key=VintageKey(args.model_version, args.registry_commit, args.source_sha256),
            max_source_age_days=args.max_source_age_days,
            max_refresh_age_hours=args.max_refresh_age_hours,
            max_missing_pay_percent=args.max_missing_pay_percent,
        )
    except MonitorError as exc:
        parser.error(str(exc))
    if args.format == "prometheus":
        print(format_prometheus(report), end="")
    else:
        print(json.dumps(report.to_dict(), indent=2))
    if report.status != "healthy":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
