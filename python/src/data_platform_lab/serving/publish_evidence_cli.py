"""Read-only inspection of durable publication attempts, without source or sink I/O."""

from __future__ import annotations

import json
from pathlib import Path

from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser
from data_platform_lab.serving.publish_evidence import PublicationArchive, PublicationEvidenceError


def main(argv: list[str] | None = None) -> None:
    """Validate the full archive and print recent or explicitly selected evidence."""
    parser = _Parser(prog="data-platform-lab ons-pay-publication-history", allow_abbrev=False)
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--last", type=int, default=50, help="Latest records (1-10000).")
    parser.add_argument(
        "--attempt-id", help="Inspect one canonical UUID, after validating all history."
    )
    try:
        args = parser.parse_args(argv)
        archive = PublicationArchive(args.data_root)
        records = archive.history(last=args.last)
        if args.attempt_id is not None:
            selected = archive.get_attempt(args.attempt_id)
            records = (selected,) if selected is not None else ()
    except _ArgumentError as exc:
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
    print(
        json.dumps(
            {
                "schema_version": 1,
                "operation": "history",
                "records": [
                    {"record_sha256": record.record_sha256, "manifest": record.to_dict()}
                    for record in records
                ],
            },
            indent=2,
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    main()
