"""Exit-code-driven publication history checks with no source or database access."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.observability.ons_pay_publication_status import (
    PublicationStatusError,
    publication_status,
)
from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser
from data_platform_lab.serving.publish_evidence import PublicationEvidenceError


def main(argv: list[str] | None = None) -> None:
    """Print safe JSON; exit 0/1/2/5 for OK/attention/configuration/archive failure."""
    parser = _Parser(prog="data-platform-lab ons-pay-publication-status", allow_abbrev=False)
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--model-version", type=int, choices=(1,), required=True)
    parser.add_argument("--registry-commit", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--sink", choices=("postgresql", "mongodb"), required=True)
    parser.add_argument("--table", choices=("geography_year", "local_authority_yoy"), required=True)
    parser.add_argument(
        "--target-id", help="Exact target label; omit to select only unlabeled records."
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        help="Maximum age of the latest accepted attempt; positive finite hours.",
    )
    try:
        args = parser.parse_args(argv)
        report = publication_status(
            data_root=args.data_root,
            sink=args.sink,
            table=args.table,
            vintage={
                "model_version": args.model_version,
                "registry_commit": args.registry_commit,
                "source_sha256": args.source_sha256,
            },
            target_id=args.target_id,
            max_age_hours=args.max_age_hours,
        )
    except (_ArgumentError, PublicationStatusError) as exc:
        print(json.dumps({"error": {"code": "invalid_arguments", "message": str(exc)}}))
        raise SystemExit(2) from None
    except PublicationEvidenceError:
        print(
            json.dumps(
                {
                    "error": {
                        "code": "evidence_error",
                        "message": "Cannot inspect valid publication evidence.",
                    }
                }
            )
        )
        raise SystemExit(5) from None
    print(json.dumps(report.to_dict(), indent=2, allow_nan=False))
    if report.status != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
