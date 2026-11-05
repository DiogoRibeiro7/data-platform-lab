"""Explicit ONS Gold preview, publication and read-only sink verification."""

from __future__ import annotations

import argparse
import importlib
import json
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn, cast

if TYPE_CHECKING:
    from data_platform_lab.serving.publish_plan import PublishPlan
    from data_platform_lab.serving.publish_reconcile import MongoReader, PostgresReader
    from data_platform_lab.serving.publish_sinks import MongoCollection, PostgresConnection


class _ArgumentError(ValueError):
    """Invalid CLI syntax, whose raw arguments must not be echoed."""


class _ConfigurationError(ValueError):
    """Missing operator configuration or an optional driver."""


class _DatabaseError(RuntimeError):
    """A connection could not be opened, configured or closed."""


class _Parser(argparse.ArgumentParser):
    """Keep argument failures machine-readable without echoing secret input."""

    def error(self, message: str) -> NoReturn:
        raise _ArgumentError("Invalid or missing arguments; use ons-pay-publish --help.")


def _parser() -> argparse.ArgumentParser:
    """Require every source pin and make sink writes an explicit operation."""
    parser = _Parser(
        prog="data-platform-lab ons-pay-publish",
        description="Preview, explicitly publish, or verify one pinned ONS pay Gold vintage.",
        allow_abbrev=False,
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--model-version", type=int, choices=(1,), required=True)
    parser.add_argument("--registry-commit", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--table", choices=("geography_year", "local_authority_yoy"), required=True)
    parser.add_argument("--sink", choices=("postgresql", "mongodb"), required=True)
    parser.add_argument("--max-rows", type=int, default=100_000, help="Source row cap (1-100000).")
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--apply", action="store_true", help="Publish and require acceptance.")
    operation.add_argument("--verify", action="store_true", help="Read and reconcile; never write.")
    return parser


def _empty_result() -> dict[str, Any]:
    """Use the same JSON envelope for success and failure, with unknowns null."""
    return {
        "schema_version": 1,
        "operation": None,
        "status": "failed",
        "sink": None,
        "table": None,
        "vintage": None,
        "counts": {"source": None, "applied": None, "observed": None},
        "fingerprints": {
            "source_artifact_sha256": None,
            "source_plan_sha256": None,
            "expected_sink_sha256": None,
            "observed_sink_sha256": None,
        },
        "reconciliation": None,
        "acceptance": None,
        "error": None,
    }


def _failure(result: dict[str, Any], code: str, message: str, exit_code: int) -> int:
    """Withhold acceptance and expose only a fixed, operator-safe diagnostic."""
    result["status"] = "rejected" if exit_code == 3 else "failed"
    result["acceptance"] = None
    result["error"] = {"code": code, "message": message}
    return exit_code


def _required(environ: Mapping[str, str], name: str) -> str:
    """Read an explicit connection setting without including its value in errors."""
    value = environ.get(name)
    if not value or not value.strip():
        raise _ConfigurationError(f"Set {name} before --apply or --verify.")
    return value


@contextmanager
def _open_sink(
    args: argparse.Namespace,
    environ: Mapping[str, str],
    *,
    postgres_connection: PostgresConnection | PostgresReader | None,
    mongo_collection: MongoCollection | MongoReader | None,
) -> Iterator[object]:
    """Lazily open an owned client, or borrow an injected handle without closing it."""
    if args.sink == "postgresql":
        if mongo_collection is not None:
            raise _ConfigurationError("A MongoDB handle cannot serve a PostgreSQL operation.")
        if postgres_connection is not None:
            yield postgres_connection
            return
        address = _required(environ, "ONS_PAY_POSTGRES_DSN")
        driver_name = "psycopg"
    else:
        if postgres_connection is not None:
            raise _ConfigurationError("A PostgreSQL handle cannot serve a MongoDB operation.")
        if mongo_collection is not None:
            yield mongo_collection
            return
        address = _required(environ, "ONS_PAY_MONGODB_URI")
        database = _required(environ, "ONS_PAY_MONGODB_DATABASE")
        driver_name = "pymongo"
    try:
        driver = importlib.import_module(driver_name)
    except ImportError:
        raise _ConfigurationError(f"Install the {driver_name} driver for this sink.") from None
    try:
        client = (
            driver.connect(address, autocommit=False, connect_timeout=10)
            if args.sink == "postgresql"
            else driver.MongoClient(
                address,
                serverSelectionTimeoutMS=10_000,
                connectTimeoutMS=10_000,
                socketTimeoutMS=30_000,
            )
        )
    except Exception:
        raise _DatabaseError("Could not open the configured sink connection.") from None
    try:
        try:
            if args.sink == "postgresql":
                if args.verify:
                    client.read_only = True
                handle = client
            else:
                handle = client[database][f"ons_pay_{args.table}"]
        except Exception:
            raise _DatabaseError("Could not configure the selected sink.") from None
        yield handle
    finally:
        try:
            client.close()
        except Exception:
            raise _DatabaseError(
                "Could not close the sink connection; inspect its state."
            ) from None


def _source_plan(args: argparse.Namespace) -> PublishPlan:
    """Rebuild a plan from the existing verified curation receipt and Parquet files."""
    from data_platform_lab.serving.catalog import VintageKey
    from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing

    return plan_ons_pay_publishing(
        data_root=args.data_root,
        vintage=VintageKey(args.model_version, args.registry_commit, args.source_sha256),
        table=args.table,
        shape="relational" if args.sink == "postgresql" else "document",
        max_rows=args.max_rows,
    )


def _execute(
    args: argparse.Namespace,
    result: dict[str, Any],
    environ: Mapping[str, str],
    *,
    postgres_connection: PostgresConnection | PostgresReader | None,
    mongo_collection: MongoCollection | MongoReader | None,
) -> int:
    """Compose existing planner, adapter, reconciler and gate with stable exit codes."""
    from data_platform_lab.serving.catalog import CatalogError, VintageNotFound
    from data_platform_lab.serving.publish_gate import (
        PublicationAcceptanceError,
        accept_publication,
    )
    from data_platform_lab.serving.publish_plan import PublishPlanError
    from data_platform_lab.serving.publish_reconcile import (
        ReconciliationError,
        ReconciliationReadError,
        expected_sink_fingerprint,
        reconcile_mongodb,
        reconcile_postgres,
    )
    from data_platform_lab.serving.publish_sinks import (
        PublishSinkError,
        publish_mongodb,
        publish_postgres,
    )

    try:
        plan = _source_plan(args)
        result["vintage"] = asdict(plan.vintage)
        result["counts"]["source"] = plan.source_rows
        result["fingerprints"].update(
            source_artifact_sha256=plan.source_artifact_sha256,
            source_plan_sha256=plan.records_sha256,
            expected_sink_sha256=expected_sink_fingerprint(plan, sink=args.sink),
        )
        if not args.apply and not args.verify:
            result["status"] = "previewed"
            return 0

        with _open_sink(
            args,
            environ,
            postgres_connection=postgres_connection,
            mongo_collection=mongo_collection,
        ) as handle:
            # Connections can take time. Recheck actual source files after opening,
            # immediately before I/O; adapters also validate and snapshot the plan.
            current = _source_plan(args)
            if current != plan:
                raise PublishPlanError("source changed since the initial plan")
            receipt = None
            if args.apply:
                receipt = (
                    publish_postgres(current, cast("PostgresConnection", handle))
                    if args.sink == "postgresql"
                    else publish_mongodb(current, cast("MongoCollection", handle))
                )
                result["counts"]["applied"] = receipt.records_applied
            report = (
                reconcile_postgres(current, cast("PostgresReader", handle))
                if args.sink == "postgresql"
                else reconcile_mongodb(current, cast("MongoReader", handle))
            )
            result["counts"]["observed"] = report.observed_count
            result["fingerprints"]["observed_sink_sha256"] = report.observed_sink_sha256
            # Summaries omit IDs, arbitrary sink field names, and all record values.
            result["reconciliation"] = {
                "passed": report.passed,
                "missing_count": len(report.missing_ids),
                "unexpected_count": len(report.unexpected_ids),
                "changed_record_count": len(report.changed_fields),
                "changed_field_count": sum(
                    len(fields) for fields in report.changed_fields.values()
                ),
            }
            if receipt is not None:
                result["acceptance"] = accept_publication(current, receipt, report).to_dict()
                result["status"] = "accepted"
            elif report.passed:
                result["status"] = "verified"
            else:
                return _failure(
                    result, "reconciliation_failed", "Sink vintage differs from the source plan.", 3
                )
    except (CatalogError, VintageNotFound, PublishPlanError, OSError):
        return _failure(
            result,
            "source_validation_failed",
            "Source pins, row budget or curated files are invalid.",
            2,
        )
    except _ConfigurationError as exc:
        return _failure(result, "configuration_error", str(exc), 2)
    except (PublishSinkError, ReconciliationReadError, _DatabaseError):
        return _failure(
            result, "database_error", "Sink operation failed; writes may already be visible.", 4
        )
    except ReconciliationError:
        return _failure(
            result, "reconciliation_failed", "Sink data could not be safely reconciled.", 3
        )
    except PublicationAcceptanceError:
        return _failure(
            result,
            "acceptance_rejected",
            "Publication evidence did not pass the acceptance gate.",
            3,
        )
    return 0


def main(
    argv: list[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    postgres_connection: PostgresConnection | PostgresReader | None = None,
    mongo_collection: MongoCollection | MongoReader | None = None,
) -> None:
    """Print one safe JSON result; exit 0/2/3/4 for success/validation/drift/database.

    Embedders may inject connection settings or caller-owned handles. An apply
    handle must support both the existing write and read protocols; verification
    only needs the reader protocol. Preview never accesses any injected handle.
    """
    result = _empty_result()
    try:
        args = _parser().parse_args(argv)
    except _ArgumentError as exc:
        code = _failure(result, "invalid_arguments", str(exc), 2)
    else:
        result.update(
            operation="apply" if args.apply else "verify" if args.verify else "preview",
            sink=args.sink,
            table=args.table,
        )
        try:
            code = _execute(
                args,
                result,
                os.environ if environ is None else environ,
                postgres_connection=postgres_connection,
                mongo_collection=mongo_collection,
            )
        except ImportError:
            code = _failure(
                result, "configuration_error", "Install the analytics or serving extra first.", 2
            )
    print(json.dumps(result, indent=2, allow_nan=False))
    if code:
        raise SystemExit(code)


if __name__ == "__main__":
    main()
