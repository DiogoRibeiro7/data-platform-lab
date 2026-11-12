"""Read-only status and success-age checks for a saved ONS ingestion job."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.observability.ons_pay_job_status import JobStatusError, inspect_ons_pay_job
from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser


def main(argv: list[str] | None = None) -> None:
    """Print completed-run status; return 1 for missing, failed or stale outcomes."""
    parser = _Parser(
        prog="data-platform-lab ons-pay-job-status",
        description="Read completed audits for the exact saved job; no ingestion or data checks.",
        allow_abbrev=False,
    )
    parser.add_argument("--job", type=Path, required=True)
    parser.add_argument(
        "--max-success-age-hours",
        type=float,
        help="Optional non-negative limit on hours since the last complete success.",
    )
    try:
        args = parser.parse_args(argv)
        report = inspect_ons_pay_job(
            job_path=args.job, max_success_age_hours=args.max_success_age_hours
        )
    except (_ArgumentError, JobStatusError) as exc:
        parser.exit(2, f"{exc}\n")
    print(json.dumps(report.to_dict(), indent=2))
    if report.status != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
