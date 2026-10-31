"""Run the offline ONS pay analytical model from a verified registry receipt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from data_platform_lab.curation.ons_pay import curate_ons_pay
from data_platform_lab.storage import LocalBlobStore


def main(argv: list[str] | None = None) -> None:
    """Materialize audited Silver/Gold Parquet tables without cloud credentials."""
    parser = argparse.ArgumentParser(description="Model ONS pay as Silver/Gold Parquet tables")
    parser.add_argument(
        "--receipt", type=Path, required=True, help="Successful registry ingestion JSON"
    )
    parser.add_argument("--object-root", type=Path, default=Path("../data/object-store"))
    parser.add_argument("--output-root", type=Path, default=Path("../data"))
    args = parser.parse_args(argv)
    result = curate_ons_pay(
        input_receipt=args.receipt,
        store=LocalBlobStore(args.object_root),
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "run_id": result.run_id,
                "source_rows": result.source_rows,
                "rows_with_pay": result.rows_with_pay,
                "rows_without_pay": result.rows_without_pay,
                "outputs": [
                    {
                        "layer": artifact.layer,
                        "path": str(artifact.path),
                        "rows": artifact.rows,
                        "status": "reused" if artifact.reused else "published",
                    }
                    for artifact in result.outputs
                ],
                "receipt_path": str(result.receipt_path),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
