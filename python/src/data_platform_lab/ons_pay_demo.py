"""Synthetic ONS publishing walkthrough for the isolated Docker demo runner.

All data processing, publication, reconciliation and evidence use the existing
platform contracts. The fixed service names belong to the launcher's private
Compose network; this module never loads an operator's database credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any
from uuid import uuid4

from data_platform_lab.curation.ons_pay import DATASET_ID, MODEL_VERSION, curate_ons_pay
from data_platform_lab.ingestion.registry_handoff import ingest_handoff
from data_platform_lab.serving.catalog import CatalogError, VintageKey, VintageNotFound
from data_platform_lab.serving.publish_evidence import PublicationArchive
from data_platform_lab.serving.publish_plan import (
    PublishPlan,
    PublishPlanError,
    TableName,
    plan_ons_pay_publishing,
)
from data_platform_lab.serving.publish_reconcile import (
    ReconciliationReport,
    reconcile_mongodb,
    reconcile_postgres,
)
from data_platform_lab.serving.publish_sinks import (
    PublishReceipt,
    SinkName,
    publish_mongodb,
    publish_postgres,
)
from data_platform_lab.storage import LocalBlobStore

_SOURCE_PATH = f"datasets/{DATASET_ID}/raw/gross-median-weekly-pay.csv"
_TABLES: tuple[TableName, ...] = ("geography_year", "local_authority_yoy")
_SINKS: tuple[SinkName, ...] = ("postgresql", "mongodb")


class DemoError(RuntimeError):
    """The walkthrough failed an invariant or cannot use its isolated workspace."""


@dataclass(frozen=True)
class DemoSource:
    """A real local Git checkout and its synthetic, checksum-pinned handoff."""

    registry_root: Path
    manifest_path: Path
    registry_commit: str
    source_sha256: str


def _json_file(path: Path, value: object) -> None:
    """Create a readable JSON artifact without replacing any existing file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, ensure_ascii=False, allow_nan=False)
        handle.write("\n")


def _git(root: Path, *args: str) -> str:
    """Create reproducible local history without inheriting user identity or Git hooks."""
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(
        GIT_CONFIG_NOSYSTEM="1",
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_AUTHOR_NAME="Data Platform Lab Synthetic Demo",
        GIT_AUTHOR_EMAIL="synthetic@example.invalid",
        GIT_COMMITTER_NAME="Data Platform Lab Synthetic Demo",
        GIT_COMMITTER_EMAIL="synthetic@example.invalid",
        GIT_AUTHOR_DATE="2026-01-01T00:00:00+00:00",
        GIT_COMMITTER_DATE="2026-01-01T00:00:00+00:00",
    )
    return subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "core.autocrlf=false",
            "-c",
            "core.filemode=false",
            "-c",
            f"core.hooksPath={os.devnull}",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()


def prepare_source(workspace: Path) -> DemoSource:
    """Commit the bundled synthetic CSV and emit a handoff for production verification.

    The registry name is required by the existing ONS contract. Its commit is
    local synthetic history, not an upstream ONS/data-registry release. The
    corresponding source URI is only a lineage label and is never downloaded.
    """
    payload = files("data_platform_lab").joinpath("demo_data/ons_pay.csv").read_bytes()
    payload = payload.replace(b"\r\n", b"\n")
    root = workspace / "registry"
    root.mkdir(parents=True, exist_ok=False)
    source = root / _SOURCE_PATH
    source.parent.mkdir(parents=True)
    source.write_bytes(payload)
    _git(root, "init", "-q", "--initial-branch=main", "--object-format=sha1", "--template=")
    _git(root, "add", "--", _SOURCE_PATH)
    _git(root, "commit", "-qm", "Synthetic ONS-compatible publishing fixture")
    commit = _git(root, "rev-parse", "HEAD")
    digest = hashlib.sha256(payload).hexdigest()
    manifest = workspace / "handoff.json"
    _json_file(
        manifest,
        {
            "manifest_version": 1,
            "dataset_id": DATASET_ID,
            "title": "Synthetic pay demonstration; not official ONS statistics",
            "lifecycle_status": "active",
            "registry": {"repository": "DiogoRibeiro7/data", "commit": commit},
            "source": {
                "publisher": "Data Platform Lab synthetic demo",
                "url": "https://example.invalid/synthetic-ons-pay",
                "snapshot": "synthetic-demo-v1",
                "retrieved_at": "2026-01-01",
            },
            "license": {
                "name": "MIT",
                "url": "https://opensource.org/license/mit/",
                "redistribution": "allowed",
            },
            "citation": "Data Platform Lab synthetic fixture; not official ONS statistics",
            "local_bytes_verified": True,
            "synthetic": True,
            "objects": [
                {
                    "source_path": _SOURCE_PATH,
                    "source_uri": f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{commit}/{_SOURCE_PATH}",
                    "sha256": digest,
                    "role": "raw",
                    "format": "csv",
                    "storage_key": (
                        f"canonical/{DATASET_ID}/{digest}/raw/gross-median-weekly-pay.csv"
                    ),
                    "size_bytes": len(payload),
                }
            ],
        },
    )
    return DemoSource(root, manifest, commit, digest)


@contextmanager
def _stage(report: dict[str, Any], name: str, **identity: object) -> Iterator[dict[str, Any]]:
    """Retain ordered stage outcomes and measured timings, including the failed stage."""
    entry: dict[str, Any] = {"stage": name, "status": "running", **identity}
    report["stages"].append(entry)
    started = time.perf_counter()
    try:
        yield entry
        if entry["status"] == "running":
            entry["status"] = "success"
    except Exception:
        entry["status"] = "failed"
        raise
    finally:
        entry["elapsed_seconds"] = round(time.perf_counter() - started, 6)


@contextmanager
def _open_demo_sink(sink: SinkName, table: TableName) -> Iterator[Any]:
    """Connect only to the dedicated services created by the demo Compose project."""
    if sink == "postgresql":
        import psycopg

        with psycopg.connect(
            "host=postgres dbname=ons_pay_demo user=ons_pay_demo password=local_demo_only",
            connect_timeout=10,
            autocommit=False,
        ) as connection:
            yield connection
    else:
        driver = importlib.import_module("pymongo")
        with driver.MongoClient(
            "mongodb://mongodb:27017",
            directConnection=True,
            retryReads=False,
            retryWrites=False,
            readPreference="primary",
            w=1,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=30_000,
        ) as client:
            yield client["ons_pay_demo"][f"ons_pay_{table}"]


def _mutate_one(plan: PublishPlan, sink: SinkName, handle: Any) -> None:
    """Change exactly one synthetic record between its write and read-back."""
    field = {"geography_year": "geography", "local_authority_yoy": "area_name"}[plan.table]
    if sink == "postgresql":
        result = handle.execute(
            f"UPDATE ons_pay_{plan.table} SET {field} = %s "
            "WHERE record_id = %s AND model_version = %s AND registry_commit = %s "
            "AND source_sha256 = %s",
            (
                "controlled synthetic drift",
                plan.records[0]["record_id"],
                plan.vintage.model_version,
                plan.vintage.registry_commit,
                plan.vintage.source_sha256,
            ),
        )
        if result.rowcount != 1:
            raise DemoError("controlled mutation did not affect exactly one record")
        handle.commit()
    else:
        result = handle.update_one(
            {"_id": plan.records[0]["_id"], "vintage": asdict(plan.vintage)},
            {"$set": {f"data.{field}": "controlled synthetic drift"}},
        )
        if result.modified_count != 1:
            raise DemoError("controlled mutation did not affect exactly one document")


def _publication_attempt(
    data_root: Path, plan: PublishPlan, sink: SinkName, *, mutate: bool = False
) -> dict[str, Any]:
    """Use production adapters and archive/gate for one explicitly requested attempt."""
    archive = PublicationArchive(data_root)
    attempt_id = str(uuid4())
    archive.prepare(plan, sink=sink, attempt_id=attempt_id)
    started = datetime.now(UTC)
    receipt: PublishReceipt | None = None
    reconciliation: ReconciliationReport | None = None
    try:
        with _open_demo_sink(sink, plan.table) as handle:
            # Re-read every source artifact after connection establishment, as the CLI does.
            current = plan_ons_pay_publishing(
                data_root=data_root, vintage=plan.vintage, table=plan.table, shape=plan.shape
            )
            if current != plan:
                raise PublishPlanError("source changed before publication")
            receipt = (
                publish_postgres(current, handle)
                if sink == "postgresql"
                else publish_mongodb(current, handle)
            )
            if mutate:
                _mutate_one(current, sink, handle)
            reconciliation = (
                reconcile_postgres(current, handle)
                if sink == "postgresql"
                else reconcile_mongodb(current, handle)
            )
    except Exception as exc:
        archive.record(
            plan,
            sink=sink,
            attempt_id=attempt_id,
            started_at=started,
            ended_at=datetime.now(UTC),
            receipt=receipt,
            report=reconciliation,
            failure_code="source_validation_failed"
            if isinstance(exc, (PublishPlanError, CatalogError, VintageNotFound))
            else "database_error",
        )
        raise
    # The archive independently invokes accept_publication. A successful write
    # alone can never turn a mismatched read-back into accepted evidence.
    evidence = archive.record(
        plan,
        sink=sink,
        attempt_id=attempt_id,
        started_at=started,
        ended_at=datetime.now(UTC),
        receipt=receipt,
        report=reconciliation,
    )
    return evidence.cli_result(replayed=False)


def _require_accepted(result: dict[str, Any]) -> None:
    """Stop the demo if a supposedly successful publication was not accepted."""
    if result["status"] != "accepted" or result["acceptance"] is None:
        raise DemoError("publication was not accepted")


def run_demo(output_root: Path, *, exercise_recovery: bool = False) -> dict[str, Any]:
    """Run the synthetic walkthrough once, retaining all artifacts and a JSON report.

    The optional recovery exercise is explicitly authorized by the launcher
    flag. Unexpected errors always stop the run; they never trigger retries.
    """
    if os.name != "posix":
        raise DemoError("run the walkthrough in its Linux Docker container")
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    if (output_root / "report.json").exists():
        raise DemoError("the output directory already contains a report")
    workspace = output_root / "work"
    workspace.mkdir(exist_ok=False)
    data_root = workspace / "curated"
    report: dict[str, Any] = {
        "schema_version": 1,
        "demo": "ons_pay_publishing",
        "synthetic": True,
        "run_id": str(uuid4()),
        "started_at": datetime.now(UTC).isoformat(),
        "status": "running",
        "exercise_recovery": exercise_recovery,
        "stages": [],
        "release_activation": "not_requested",
    }
    started = time.perf_counter()
    try:
        with _stage(report, "prepare_synthetic_source") as stage:
            source = prepare_source(workspace)
            vintage = VintageKey(MODEL_VERSION, source.registry_commit, source.source_sha256)
            report["source"] = {
                "dataset_id": DATASET_ID,
                "vintage": asdict(vintage),
                "provenance": "local synthetic Git history; not an official ONS release",
            }
            stage.update(
                registry="work/registry",
                handoff="work/handoff.json",
                source_sha256=source.source_sha256,
                registry_commit=source.registry_commit,
            )
        store = LocalBlobStore(workspace / "object-store")
        with _stage(report, "verify_and_ingest") as stage:
            ingested = ingest_handoff(
                manifest_path=source.manifest_path,
                registry_root=source.registry_root,
                store=store,
                receipt_dir=workspace / "ingestion-receipts",
            )
            stage.update(
                committed_blob_verified=True,
                source_checksum_verified=True,
                destination_checksum_verified=True,
                bytes_verified=ingested.bytes_verified,
                files_published=ingested.files_published,
                files_reused=ingested.files_reused,
                receipt=str(ingested.receipt_path.relative_to(output_root)),
            )
        with _stage(report, "curate_silver_and_gold") as stage:
            curated = curate_ons_pay(
                input_receipt=ingested.receipt_path, store=store, output_root=data_root
            )
            stage.update(
                source_rows=curated.source_rows,
                rows_with_pay=curated.rows_with_pay,
                rows_without_pay=curated.rows_without_pay,
                receipt=str(curated.receipt_path.relative_to(output_root)),
                outputs=[
                    {
                        "path": str(item.path.relative_to(output_root)),
                        "layer": item.layer,
                        "rows": item.rows,
                        "sha256": item.sha256,
                    }
                    for item in curated.outputs
                ],
            )
        for table in _TABLES:
            for sink in _SINKS:
                with _stage(report, "plan", table=table, sink=sink) as stage:
                    plan = plan_ons_pay_publishing(
                        data_root=data_root,
                        vintage=vintage,
                        table=table,
                        shape="relational" if sink == "postgresql" else "document",
                    )
                    path = workspace / "plans" / f"{table}_{plan.shape}.json"
                    _json_file(path, plan.to_dict())
                    stage.update(
                        source_rows=plan.source_rows,
                        source_artifacts_verified=True,
                        source_artifact_sha256=plan.source_artifact_sha256,
                        source_plan_sha256=plan.records_sha256,
                        plan=str(path.relative_to(output_root)),
                    )
                with _stage(report, "publish", table=table, sink=sink) as stage:
                    first = _publication_attempt(data_root, plan, sink)
                    stage["publication"] = first
                    _require_accepted(first)
                with _stage(report, "repeat_publication", table=table, sink=sink) as stage:
                    repeated = _publication_attempt(data_root, plan, sink)
                    stage["publication"] = repeated
                    _require_accepted(repeated)
                    if repeated["fingerprints"] != first["fingerprints"]:
                        raise DemoError("repeated publication changed a fingerprint")
                    stage["idempotency_verified"] = repeated["counts"] == first["counts"]
                    if not stage["idempotency_verified"]:
                        raise DemoError("repeated publication changed population counts")
                if exercise_recovery:
                    with _stage(report, "controlled_drift", table=table, sink=sink) as stage:
                        rejected = _publication_attempt(data_root, plan, sink, mutate=True)
                        stage["publication"] = rejected
                        drift = rejected["reconciliation"]
                        if (
                            rejected["status"] != "rejected"
                            or rejected["acceptance"] is not None
                            or drift is None
                            or drift["passed"]
                            or drift["changed_record_count"] != 1
                            or drift["missing_count"]
                            or drift["unexpected_count"]
                        ):
                            raise DemoError(
                                "controlled drift did not produce the expected rejection"
                            )
                        stage["expected_rejection_verified"] = True
                    with _stage(report, "explicit_recovery", table=table, sink=sink) as stage:
                        recovered = _publication_attempt(data_root, plan, sink)
                        stage["publication"] = recovered
                        _require_accepted(recovered)
                        if recovered["fingerprints"] != first["fingerprints"]:
                            raise DemoError("recovery did not restore the original fingerprints")
                        stage["recovery_verified"] = True
        with _stage(report, "inspect_durable_history") as stage:
            history = PublicationArchive(data_root).history()
            outcomes = [item.to_dict()["outcome"] for item in history]
            if (
                len(history) != (16 if exercise_recovery else 8)
                or outcomes.count("accepted") != (12 if exercise_recovery else 8)
                or outcomes.count("rejected") != (4 if exercise_recovery else 0)
            ):
                raise DemoError("durable history does not contain every expected outcome")
            stage["outcomes"] = outcomes
            stage["records"] = len(history)
            stage["archive"] = "work/curated/manifests/ons_pay_publication"
            stage["integrity_verified"] = True
        report["status"] = "success"
    except Exception as exc:
        # Driver messages can contain connection details or row values. Keep the
        # public report diagnostic but value-free; stage evidence supplies counts.
        report["status"] = "failed"
        report["error"] = {"code": "demo_stage_failed", "type": type(exc).__name__}
    report["ended_at"] = datetime.now(UTC).isoformat()
    report["elapsed_seconds"] = round(time.perf_counter() - started, 6)
    _json_file(output_root / "report.json", report)
    return report


def main(argv: list[str] | None = None) -> None:
    """Internal container entry point; use scripts/ons_pay_publishing_demo.py on the host."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path("/output"))
    parser.add_argument(
        "--exercise-recovery",
        action="store_true",
        help="Explicitly authorize a controlled mutation, gate rejection and pinned replay.",
    )
    args = parser.parse_args(argv)
    try:
        report = run_demo(args.output_root, exercise_recovery=args.exercise_recovery)
    except (OSError, DemoError):
        report = {"status": "failed", "error": {"code": "demo_workspace_unavailable"}}
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    if report["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
