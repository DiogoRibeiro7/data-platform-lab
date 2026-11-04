"""Read-only CLI for relational/document representations of verified Gold data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Print a supplier-neutral, exact-vintage publishing preview as JSON."""
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-publish-plan",
        description="Preview pinned ONS pay Gold rows for a future SQL or document sink.",
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--model-version", type=int, choices=(1,), default=1)
    parser.add_argument("--registry-commit", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--table", choices=("geography_year", "local_authority_yoy"), required=True)
    parser.add_argument("--shape", choices=("relational", "document"), required=True)
    parser.add_argument(
        "--max-rows", type=int, default=100_000, help="Fail above this row count (1-100000)"
    )
    args = parser.parse_args(argv)

    try:
        from data_platform_lab.serving.catalog import (
            CatalogError,
            VintageKey,
            VintageNotFound,
        )
        from data_platform_lab.serving.publish_plan import (
            PublishPlanError,
            plan_ons_pay_publishing,
        )
    except ModuleNotFoundError:
        parser.error("install the serving extra first: poetry install --extras serving")

    try:
        plan = plan_ons_pay_publishing(
            data_root=args.data_root,
            vintage=VintageKey(
                args.model_version,
                args.registry_commit,
                args.source_sha256,
            ),
            table=args.table,
            shape=args.shape,
            max_rows=args.max_rows,
        )
    except (CatalogError, VintageNotFound, PublishPlanError) as exc:
        parser.error(str(exc))
    print(json.dumps(plan.to_dict(), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
