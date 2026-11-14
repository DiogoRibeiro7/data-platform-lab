"""Verify local raw-load integrity without downloading or transforming data."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.observability.ons_pay_load_verification import (
    LoadVerificationError,
    verify_ons_pay_load,
)
from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser


def main(argv: list[str] | None = None) -> None:
    """Return 0 for healthy raw data, 1 for failed checks, and 2 for invalid input."""
    parser = _Parser(
        prog="data-platform-lab ons-pay-load-verify",
        allow_abbrev=False,
        description="Read-only integrity and optional source-age checks for local raw loads.",
    )
    parser.add_argument("--load-audit", type=Path, required=True)
    parser.add_argument("--object-root", type=Path, required=True)
    parser.add_argument("--load-sha256", help="Optional exact SHA-256 of the selected load audit.")
    parser.add_argument(
        "--max-source-age-days", type=int, help="Optional non-negative source age limit."
    )
    parser.add_argument("--max-object-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--max-total-bytes", type=int, default=64 * 1024 * 1024)
    try:
        args = parser.parse_args(argv)
        report = verify_ons_pay_load(
            load_audit=args.load_audit,
            object_root=args.object_root,
            expected_load_sha256=args.load_sha256,
            max_source_age_days=args.max_source_age_days,
            max_object_bytes=args.max_object_bytes,
            max_total_bytes=args.max_total_bytes,
        )
    except (_ArgumentError, LoadVerificationError):
        parser.exit(2, "Invalid verification arguments or load evidence.\n")
    print(json.dumps(report.to_dict(), indent=2))
    if report.status != "healthy":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
