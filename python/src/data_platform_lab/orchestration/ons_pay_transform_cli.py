"""Preview or execute offline curation of a completed ONS load batch."""

from __future__ import annotations

import json
import re
from pathlib import Path

from data_platform_lab.curation.ons_pay import MODEL_VERSION
from data_platform_lab.orchestration.ons_pay_transform import (
    TransformBusyError,
    TransformError,
    plan_ons_pay_transform,
    transform_ons_pay,
)
from data_platform_lab.serving.publish_cli import _ArgumentError, _Parser
from data_platform_lab.storage import BlobStore, LocalBlobStore
from data_platform_lab.storage.config import add_s3_arguments, s3_from_arguments


def main(argv: list[str] | None = None) -> None:
    """Print JSON plans/results; use exit 1 for failure, 2 for invalid input, 3 for busy."""
    parser = _Parser(
        prog="data-platform-lab ons-pay-transform",
        allow_abbrev=False,
        description="Transform a successful raw-load batch without downloading sources.",
    )
    parser.add_argument("--load-audit", type=Path, required=True)
    parser.add_argument("--load-sha256", help="Optional exact SHA-256 from a previous preview.")
    parser.add_argument("--object-root", type=Path, help="Required for local raw storage.")
    add_s3_arguments(parser)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate receipt links only; do not read stored data or write files.",
    )
    try:
        args = parser.parse_args(argv)
        if args.load_sha256 is not None and re.fullmatch(r"[0-9a-f]{64}", args.load_sha256) is None:
            raise _ArgumentError("load-sha256 must be a lowercase SHA-256 digest.")
        object_store = s3_from_arguments(args)
        if object_store is None and args.object_root is None:
            raise _ArgumentError("Local transformation requires --object-root.")
        object_root = (
            args.object_root.expanduser().resolve() if args.object_root is not None else None
        )
        output_root = args.output_root.expanduser().resolve()
        if args.dry_run:
            plan = plan_ons_pay_transform(
                load_audit=args.load_audit, expected_load_sha256=args.load_sha256
            )
            print(
                json.dumps(
                    {
                        "status": "planned",
                        "writes": False,
                        "model_version": MODEL_VERSION,
                        "load_audit_sha256": plan.load_audit_sha256,
                        "load_run_id": plan.load_run_id,
                        "load_job_sha256": plan.load_job_sha256,
                        **(
                            {"object_store": object_store.to_dict()}
                            if object_store is not None
                            else {"object_root": str(object_root)}
                        ),
                        "output_root": str(output_root),
                        "sources": [
                            {
                                "registry_commit": item.source.registry_commit,
                                "source_sha256": item.source.source_sha256,
                                "ingestion_receipt_path": str(item.receipt_path),
                                "ingestion_receipt_sha256": item.receipt_sha256,
                            }
                            for item in plan.sources
                        ],
                    },
                    indent=2,
                )
            )
            return
    except (_ArgumentError, TransformError, OSError, RuntimeError, ValueError):
        parser.exit(2, "Invalid transformation arguments or load evidence.\n")
    try:
        store: BlobStore
        if object_store is not None:
            store = object_store.open()
        else:
            assert object_root is not None
            store = LocalBlobStore(object_root)
        result = transform_ons_pay(
            load_audit=args.load_audit,
            store=store,
            output_root=output_root,
            expected_load_sha256=args.load_sha256,
        )
    except TransformBusyError as exc:
        parser.exit(3, f"{exc}\n")
    except TransformError as exc:
        parser.exit(1, f"{exc}\n")
    except Exception:
        parser.exit(1, "Unable to access ONS transformation destinations.\n")
    print(
        json.dumps(
            {
                "status": "success",
                "run_id": result.run_id,
                "receipt_path": str(result.receipt_path),
                "load_audit_sha256": result.plan.load_audit_sha256,
                "vintages_completed": len(result.sources),
                "source_rows": sum(item.result.source_rows for item in result.sources),
                "parquet_published": sum(
                    not output.reused for item in result.sources for output in item.result.outputs
                ),
                "parquet_reused": sum(
                    output.reused for item in result.sources for output in item.result.outputs
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
