"""Manual, opt-in ONS pay activation/rollback with default read-only previews."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    """Report active status or preview/apply one explicitly versioned decision."""
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-release",
        description="Review and optionally apply a pinned local ONS pay release decision.",
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("status", help="Validate and show active release history (read-only)")

    activate = actions.add_parser("activate", help="Preview or apply an exact candidate")
    activate.add_argument("--candidate-registry-commit", required=True)
    activate.add_argument("--candidate-source-sha256", required=True)
    activate.add_argument("--model-version", type=int, choices=(1,), default=1)
    activate.add_argument("--expected-generation", type=int, required=True)
    activate.add_argument("--reason", required=True)
    activate.add_argument("--bootstrap", action="store_true", help="Explicit first activation")
    activate.add_argument("--max-added-rows", type=int)
    activate.add_argument("--max-removed-rows", type=int)
    activate.add_argument("--max-revised-rows", type=int)
    activate.add_argument("--max-newly-missing-pay-rows", type=int)
    activate.add_argument("--max-missing-pay-increase-pp", type=float)
    activate.add_argument("--max-absolute-pay-revision-gbp", type=float)
    activate.add_argument("--apply", action="store_true", help="Write a reviewed decision")

    rollback = actions.add_parser("rollback", help="Preview or apply a prior active version")
    rollback.add_argument("--to-generation", type=int, required=True)
    rollback.add_argument("--expected-generation", type=int, required=True)
    rollback.add_argument("--reason", required=True)
    rollback.add_argument("--apply", action="store_true", help="Write a reviewed rollback")
    args = parser.parse_args(argv)

    # The optional serving dependency is loaded only when this command runs.
    try:
        from data_platform_lab.serving.catalog import VintageKey
        from data_platform_lab.serving.release_store import ReleaseError, ReleaseStore
    except ModuleNotFoundError:
        parser.error("install the serving extra first: poetry install --extras serving")

    store = ReleaseStore(args.data_root)
    try:
        if args.action == "status":
            state = store.status()
            print(json.dumps({"active": state.to_dict() if state is not None else None}, indent=2))
            return
        if args.action == "activate":
            candidate = VintageKey(
                args.model_version, args.candidate_registry_commit, args.candidate_source_sha256
            )
            limits = {
                "max_added_rows": args.max_added_rows,
                "max_removed_rows": args.max_removed_rows,
                "max_revised_rows": args.max_revised_rows,
                "max_newly_missing_pay_rows": args.max_newly_missing_pay_rows,
                "max_missing_pay_increase_pp": args.max_missing_pay_increase_pp,
                "max_absolute_pay_revision_gbp": args.max_absolute_pay_revision_gbp,
            }
            if args.apply:
                plan = store.activate(
                    candidate=candidate,
                    expected_generation=args.expected_generation,
                    reason=args.reason,
                    bootstrap=args.bootstrap,
                    **limits,
                )
            else:
                plan = store.plan_activate(
                    candidate=candidate,
                    expected_generation=args.expected_generation,
                    bootstrap=args.bootstrap,
                    **limits,
                )
        elif args.apply:
            plan = store.rollback(
                to_generation=args.to_generation,
                expected_generation=args.expected_generation,
                reason=args.reason,
            )
        else:
            plan = store.plan_rollback(
                to_generation=args.to_generation, expected_generation=args.expected_generation
            )
    except ReleaseError as exc:
        parser.error(str(exc))

    print(json.dumps(plan.to_dict(), indent=2))
    if not plan.eligible:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
