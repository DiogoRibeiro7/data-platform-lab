"""Explicit, pinned ONS ingestion through accepted persistent PostgreSQL publication."""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid5

from data_platform_lab.curation.ons_pay import DATASET_ID
from data_platform_lab.ingestion.registry_fetch import RegistryFetchOptions
from data_platform_lab.orchestration.ons_pay_refresh import (
    RefreshInput,
    RefreshSource,
    RemoteRefreshSource,
    plan_ons_pay_refresh,
    refresh_ons_pay,
)
from data_platform_lab.serving.publish_cli import _Parser, run_publication
from data_platform_lab.serving.publish_evidence import (
    PublicationArchive,
    validate_attempt_id,
    validate_target_id,
)
from data_platform_lab.storage import LocalBlobStore
from data_platform_lab.storage.config import S3Storage, add_s3_arguments, s3_from_arguments

if TYPE_CHECKING:
    from data_platform_lab.serving.publish_plan import TableName

TABLES: tuple[TableName, TableName] = ("geography_year", "local_authority_yoy")


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(
        prog="data-platform-lab ons-pay-postgres",
        description="Preview or explicitly ingest, curate and publish both pinned ONS Gold tables.",
        allow_abbrev=False,
    )
    parser.add_argument("--handoff", type=Path, required=True)
    parser.add_argument(
        "--registry-root", type=Path, help="Pinned checkout; omit for pinned HTTPS."
    )
    parser.add_argument("--registry-commit", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--object-root", type=Path)
    add_s3_arguments(parser)
    parser.add_argument("--reuse-verified", action="store_true", help="HTTPS raw cache reuse.")
    parser.add_argument("--target-id", required=True)
    parser.add_argument(
        "--attempt-id", required=True, help="UUID namespace for two table attempts."
    )
    parser.add_argument("--statement-timeout-ms", type=int, default=30_000)
    parser.add_argument("--lock-timeout-ms", type=int, default=5_000)
    parser.add_argument("--apply", action="store_true", help="Enable storage and database writes.")
    return parser


def _preflight(args: argparse.Namespace) -> tuple[RefreshInput, S3Storage | None, dict[str, Any]]:
    """Validate metadata and independent pins without clients, source reads or writes."""
    if (
        args.dataset_id != DATASET_ID
        or re.fullmatch(r"[0-9a-f]{40}", args.registry_commit) is None
        or re.fullmatch(r"[0-9a-f]{64}", args.source_sha256) is None
        or any(
            not 1 <= value <= 2_147_483_647
            for value in (args.statement_timeout_ms, args.lock_timeout_ms)
        )
    ):
        raise ValueError("Invalid source selection or timeouts.")
    validate_target_id(args.target_id)
    validate_attempt_id(args.attempt_id)
    storage = s3_from_arguments(args)
    if storage is None and args.object_root is None:
        raise ValueError("An explicit raw storage location is required.")
    source: RefreshInput
    if args.registry_root is not None:
        if args.reuse_verified:
            raise ValueError("HTTPS cache reuse cannot select a checkout.")
        source = RefreshSource(args.handoff, args.registry_root)
        for root in (args.data_root, args.object_root):
            if root is not None and root.resolve().is_relative_to(args.registry_root.resolve()):
                raise ValueError("Destinations must be outside the registry checkout.")
    else:
        source = RemoteRefreshSource(
            args.handoff,
            RegistryFetchOptions(
                repository="DiogoRibeiro7/data",
                commit=args.registry_commit,
                dataset_id=args.dataset_id,
                reuse_verified=args.reuse_verified,
            ),
        )
    planned = plan_ons_pay_refresh([source])[0]
    if (planned.registry_commit, planned.source_sha256) != (
        args.registry_commit,
        args.source_sha256,
    ):
        raise ValueError("Handoff differs from the independent source selection.")
    source = replace(source, expected_manifest_sha256=planned.manifest_sha256)
    result: dict[str, Any] = {
        "schema_version": 1,
        "operation": "apply" if args.apply else "preview",
        "status": "previewed",
        "dataset_id": DATASET_ID,
        "vintage": {
            "model_version": 1,
            "registry_commit": args.registry_commit,
            "source_sha256": args.source_sha256,
        },
        "target_id": args.target_id,
        "handoff_sha256": planned.manifest_sha256,
        "source_mode": "checkout" if args.registry_root is not None else "https",
        "handoff": str(args.handoff),
        "registry_root": str(args.registry_root) if args.registry_root is not None else None,
        "data_root": str(args.data_root),
        "raw_storage": storage.to_dict()
        if storage is not None
        else {"backend": "local", "object_root": str(args.object_root)},
        "reuse_verified": args.reuse_verified,
        "timeouts_ms": {"statement": args.statement_timeout_ms, "lock": args.lock_timeout_ms},
        "attempts": {table: str(uuid5(UUID(args.attempt_id), table)) for table in TABLES},
        "actions": ["verify_service", "ingest", "curate", "publish_both", "reconcile", "index"],
        "publications": [],
    }
    return source, storage, result


def _publish_args(args: argparse.Namespace, table: str) -> list[str]:
    return [
        "--data-root",
        str(args.data_root),
        "--model-version",
        "1",
        "--registry-commit",
        args.registry_commit,
        "--source-sha256",
        args.source_sha256,
        "--table",
        table,
        "--sink",
        "postgresql",
        "--target-id",
        args.target_id,
    ]


def _index_verified(args: argparse.Namespace, result: dict[str, Any], connection: Any) -> None:
    """Recheck both accepted baselines under table locks, then atomically index them."""
    from data_platform_lab.serving.catalog import VintageKey
    from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing
    from data_platform_lab.serving.publish_reconcile import reconcile_postgres

    connection.rollback()
    # Fixed public tables and SHARE locks prevent writes between reconciliation and indexing.
    connection.execute(
        "LOCK TABLE public.ons_pay_geography_year, public.ons_pay_local_authority_yoy IN SHARE MODE"
    ).close()
    archive = PublicationArchive(args.data_root)
    columns = (
        "evidence_sha256,attempt_id,dataset_id,table_name,target_id,registry_commit,"
        "source_sha256,model_version,records_accepted,source_plan_sha256,"
        "sink_snapshot_sha256,published_at,evidence_path"
    )
    for table in TABLES:
        plan = plan_ons_pay_publishing(
            data_root=args.data_root,
            vintage=VintageKey(1, args.registry_commit, args.source_sha256),
            table=table,
            shape="relational",
        )
        evidence = archive.accepted_baseline(
            plan, sink="postgresql", attempt_id=result["attempts"][table], target_id=args.target_id
        )
        report = reconcile_postgres(plan, connection)
        if not report.passed:
            raise ValueError("Persisted rows differ from an accepted baseline.")
        record = evidence.to_dict()
        values = (
            evidence.record_sha256,
            record["run_id"],
            DATASET_ID,
            table,
            args.target_id,
            args.registry_commit,
            args.source_sha256,
            1,
            plan.source_rows,
            plan.records_sha256,
            report.observed_sink_sha256,
            datetime.fromisoformat(record["ended_at"]),
            f"manifests/ons_pay_publish/{evidence.record_sha256}.json",
        )
        connection.execute(
            f"INSERT INTO dpl_curated.publications ({columns}) "
            f"VALUES ({','.join(['%s'] * len(values))}) ON CONFLICT DO NOTHING",
            values,
        ).close()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM dpl_curated.publications WHERE "
                + " AND ".join(f"{column} = %s" for column in columns.split(",")),
                values,
            )
            if cursor.fetchone() != (1,):
                raise ValueError("Publication index conflicts with accepted evidence.")
    connection.commit()


def _apply(
    args: argparse.Namespace,
    source: RefreshInput,
    storage: S3Storage | None,
    result: dict[str, Any],
) -> int:
    """Reuse refresh and publication; retain partial outcomes without claiming batch success."""
    if os.name != "posix":
        raise ValueError("Durable publication evidence requires a POSIX filesystem.")
    dsn = os.environ.get("ONS_PAY_POSTGRES_DSN")
    if not dsn or not dsn.strip():
        raise ValueError("Set ONS_PAY_POSTGRES_DSN for apply.")
    import psycopg

    from data_platform_lab.serving.postgres_service import verify_service

    with psycopg.connect(dsn, autocommit=False, connect_timeout=10) as connection:
        for setting, value in (
            ("search_path", "public, pg_catalog"),
            ("statement_timeout", f"{args.statement_timeout_ms}ms"),
            ("lock_timeout", f"{args.lock_timeout_ms}ms"),
        ):
            connection.execute(
                "SELECT pg_catalog.set_config(%s, %s, false)", (setting, value)
            ).close()
        connection.commit()
        if verify_service(connection)["status"] != "ready":
            raise ValueError("Persistent PostgreSQL service is not ready.")
        store = storage.open() if storage is not None else LocalBlobStore(args.object_root)
        refreshed = refresh_ons_pay(sources=[source], store=store, output_root=args.data_root)
        result["refresh_receipt"] = str(refreshed.receipt_path)
        # Validate both curated plans before the first database publication.
        for table in TABLES:
            code, preview = run_publication(_publish_args(args, table))
            if code:
                result["publications"].append(preview)
                return code
        for table in TABLES:
            code, publication = run_publication(
                [
                    *_publish_args(args, table),
                    "--apply",
                    "--record-evidence",
                    "--attempt-id",
                    result["attempts"][table],
                ],
                postgres_connection=connection,
            )
            result["publications"].append(publication)
            if code:
                return code
        _index_verified(args, result, connection)
    result["status"] = "published"
    return 0


def main(argv: list[str] | None = None) -> None:
    """Print bounded metadata only; keep driver errors, source rows and credentials private."""
    result: dict[str, Any] = {"schema_version": 1, "status": "failed", "publications": []}
    code = 2
    try:
        args = _parser().parse_args(argv)
        source, storage, result = _preflight(args)
        if args.apply:
            result["status"] = "failed"
            code = 1
            code = _apply(args, source, storage, result)
        else:
            code = 0
    except Exception:
        # Nested transports/drivers may include secrets or row values in exception text.
        result["status"] = "failed"
        result["error"] = (
            "Preflight or execution failed; inspect configuration and durable receipts."
        )
    print(json.dumps(result, indent=2, allow_nan=False))
    if code:
        raise SystemExit(code)
