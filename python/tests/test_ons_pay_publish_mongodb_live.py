"""Opt-in PyMongo integration over disposable databases and synthetic pinned Gold."""

from __future__ import annotations

import copy
import json
import os
import re
from collections.abc import Iterator
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from test_ons_pay_serving import COMMIT_B, PAY_CSV, _curate, _pin

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_cli import main as publish_main
from data_platform_lab.serving.publish_evidence import PublicationArchive
from data_platform_lab.serving.publish_gate import (
    PublicationAcceptanceError,
    accept_publication,
    publish_and_accept_mongodb,
)
from data_platform_lab.serving.publish_plan import PublishPlan, TableName, plan_ons_pay_publishing
from data_platform_lab.serving.publish_reconcile import reconcile_mongodb
from data_platform_lab.serving.publish_sinks import PublishSinkError, publish_mongodb

if TYPE_CHECKING:
    from pymongo.collection import Collection
    from pymongo.database import Database

pytestmark = pytest.mark.skipif(
    os.environ.get("ONS_PAY_LIVE_MONGODB") != "1",
    reason="requires explicit opt-in and a running disposable local MongoDB instance",
)


@pytest.fixture
def mongo_database() -> Iterator[Database[dict[str, Any]]]:
    """Own a random database; reject hosted addresses and ignore any configured DB name."""
    # Lazy import keeps the ordinary suite independent of the optional driver.
    from pymongo import MongoClient

    uri = os.environ.get("ONS_PAY_MONGODB_URI", "")
    parsed = urlsplit(uri)
    if (
        parsed.scheme != "mongodb"
        or parsed.hostname not in {"127.0.0.1", "localhost", "mongodb"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        pytest.fail("use a credential-free loopback or Compose mongodb service URI")
    with MongoClient(
        uri,
        directConnection=True,
        retryReads=False,
        retryWrites=False,
        readPreference="primary",
        w=1,
        serverSelectionTimeoutMS=10_000,
        connectTimeoutMS=10_000,
        socketTimeoutMS=30_000,
    ) as client:
        client.admin.command("ping")
        name = f"ons_pay_test_{uuid4().hex}"
        try:
            yield client[name]
        finally:
            # Never drop an environment-supplied database or another test's state.
            client.drop_database(name)


def _plan(root: Path, receipt: dict[str, object], table: TableName) -> PublishPlan:
    """Use production validation and real Parquet artifacts to create a document plan."""
    pins = _pin(receipt)
    return plan_ons_pay_publishing(
        data_root=root,
        vintage=VintageKey(1, pins["registry_commit"], pins["source_sha256"]),
        table=table,
        shape="document",
    )


@pytest.fixture(params=["geography_year", "local_authority_yoy"])
def publication(request: pytest.FixtureRequest, tmp_path: Path) -> tuple[Path, PublishPlan]:
    """Exercise every scenario against both supported Gold contracts."""
    root, receipt = _curate(tmp_path)
    return root, _plan(root, receipt, request.param)


def _snapshot(collection: Collection[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Read all documents, including foreign vintages, to detect unintended mutations."""
    return {document["_id"]: document for document in collection.find({})}


def _args(root: Path, plan: PublishPlan) -> list[str]:
    """Build an operator invocation with the complete verified source pin."""
    return [
        "--data-root",
        str(root),
        "--model-version",
        str(plan.vintage.model_version),
        "--registry-commit",
        plan.vintage.registry_commit,
        "--source-sha256",
        plan.vintage.source_sha256,
        "--table",
        plan.table,
        "--sink",
        "mongodb",
    ]


def _invoke(
    capsys: pytest.CaptureFixture[str],
    database: Database[dict[str, Any]],
    args: list[str],
    *,
    exit_code: int = 0,
) -> dict[str, Any]:
    """Exercise CLI-owned PyMongo clients and validate their machine-readable output."""
    settings = {
        "ONS_PAY_MONGODB_URI": os.environ["ONS_PAY_MONGODB_URI"],
        "ONS_PAY_MONGODB_DATABASE": database.name,
    }
    if exit_code:
        with pytest.raises(SystemExit) as error:
            publish_main(args, environ=settings)
        assert error.value.code == exit_code
    else:
        publish_main(args, environ=settings)
    captured = capsys.readouterr()
    assert captured.err == ""
    assert settings["ONS_PAY_MONGODB_URI"] not in captured.out
    return json.loads(captured.out)


def test_live_document_identity_pins_types_and_idempotent_replay(
    publication: tuple[Path, PublishPlan], mongo_database: Database[dict[str, Any]]
) -> None:
    """Round-trip complete BSON documents through the actual writer, reader and gate."""
    root, plan = publication
    repeated = plan_ons_pay_publishing(
        data_root=root, vintage=plan.vintage, table=plan.table, shape="document"
    )
    assert repeated == plan
    collection = mongo_database[f"ons_pay_{plan.table}"]
    first = publish_mongodb(plan, collection)
    expected = {str(record["_id"]): record for record in plan.records}
    assert len(expected) == plan.source_rows
    assert _snapshot(collection) == expected
    for document in collection.find({}):
        assert re.fullmatch(r"[0-9a-f]{64}", document["_id"])
        assert document["vintage"] == asdict(plan.vintage)
        assert document["data"]["registry_commit"] == plan.vintage.registry_commit
        assert document["data"]["source_sha256"] == plan.vintage.source_sha256
        assert document["schema_version"] == 1
        assert document["dataset_id"] == plan.dataset_id
        assert document["table"] == plan.table
    if plan.table == "local_authority_yoy":
        suppressed = collection.find_one({"data.area_code": "A001", "data.pay_year": 2024})
        assert suppressed is not None
        assert suppressed["data"]["weekly_pay_gbp"] is None
        assert suppressed["data"]["reference_date"] == "2024-04-01"
        assert suppressed["data"]["status_code"] == "x"
    second = publish_mongodb(repeated, collection)
    assert first == second
    report = reconcile_mongodb(plan, collection)
    assert report.passed
    assert report.observed_count == plan.source_rows
    assert report.expected_sink_sha256 == report.observed_sink_sha256
    accepted = accept_publication(plan, second, report)
    assert accepted.records_accepted == plan.source_rows
    assert accepted == publish_and_accept_mongodb(plan, writer=collection, reader=collection)
    assert _snapshot(collection) == expected


@pytest.mark.parametrize("mutation", ["missing", "unexpected", "altered", "lineage"])
def test_live_drift_fails_gate_and_verification_never_repairs(
    publication: tuple[Path, PublishPlan],
    mongo_database: Database[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
    mutation: str,
) -> None:
    """Detect identity/field drift, including a record moved outside the vintage filter."""
    root, plan = publication
    collection = mongo_database[f"ons_pay_{plan.table}"]
    receipt = publish_mongodb(plan, collection)
    identifier = str(plan.records[0]["_id"])
    extra_id = "f" * 64
    assert extra_id not in {record["_id"] for record in plan.records}
    if mutation == "missing":
        collection.delete_one({"_id": identifier})
    elif mutation == "unexpected":
        extra = copy.deepcopy(plan.records[0])
        extra["_id"] = extra_id
        collection.insert_one(extra)
    elif mutation == "altered":
        collection.update_one({"_id": identifier}, {"$set": {"data.source_sha256": "0" * 64}})
    else:
        collection.update_one({"_id": identifier}, {"$set": {"vintage.registry_commit": COMMIT_B}})
    before = _snapshot(collection)
    report = reconcile_mongodb(plan, collection)
    assert not report.passed
    assert report.expected_sink_sha256 != report.observed_sink_sha256
    assert report.missing_ids == ((identifier,) if mutation in {"missing", "lineage"} else ())
    assert report.unexpected_ids == ((extra_id,) if mutation == "unexpected" else ())
    assert report.changed_fields == (
        {identifier: ("data.source_sha256",)} if mutation == "altered" else {}
    )
    with pytest.raises(PublicationAcceptanceError):
        accept_publication(plan, receipt, report)
    result = _invoke(capsys, mongo_database, [*_args(root, plan), "--verify"], exit_code=3)
    assert result["acceptance"] is None
    assert result["counts"]["applied"] is None
    assert _snapshot(collection) == before
    assert PublicationArchive(root).history() == ()
    # A replay repairs expected IDs but deliberately cannot delete an unexpected ID.
    publish_mongodb(plan, collection)
    if mutation == "unexpected":
        assert not reconcile_mongodb(plan, collection).passed
        assert collection.find_one({"_id": extra_id}) == before[extra_id]
        collection.delete_one({"_id": extra_id})  # Explicit operator cleanup in our own test DB.
    assert reconcile_mongodb(plan, collection).passed


@pytest.mark.parametrize("changed_pin", ["registry_commit", "source_sha256"])
def test_live_vintages_coexist_and_recovery_preserves_other_vintage(
    publication: tuple[Path, PublishPlan],
    mongo_database: Database[dict[str, Any]],
    tmp_path: Path,
    changed_pin: str,
) -> None:
    """Changing either independent source pin produces disjoint identities in one collection."""
    root, first = publication
    if changed_pin == "registry_commit":
        _, receipt = _curate(tmp_path, commit=COMMIT_B)
    else:
        payload = PAY_CSV.replace(",100,Pounds,", ",101,Pounds,").encode("utf-8-sig")
        _, receipt = _curate(tmp_path, payload=payload)
    second = _plan(root, receipt, first.table)
    first_ids = {str(record["_id"]) for record in first.records}
    second_ids = {str(record["_id"]) for record in second.records}
    assert first_ids.isdisjoint(second_ids)
    assert getattr(first.vintage, changed_pin) != getattr(second.vintage, changed_pin)
    collection = mongo_database[f"ons_pay_{first.table}"]
    publish_mongodb(second, collection)
    other_vintage = _snapshot(collection)
    publish_mongodb(first, collection)
    collection.delete_one({"_id": first.records[0]["_id"]})
    assert not reconcile_mongodb(first, collection).passed
    assert reconcile_mongodb(second, collection).passed
    publish_and_accept_mongodb(first, writer=collection, reader=collection)
    after = _snapshot(collection)
    assert {key: after[key] for key in second_ids} == other_vintage
    assert set(after) == first_ids | second_ids
    assert reconcile_mongodb(first, collection).passed
    assert reconcile_mongodb(second, collection).passed


def test_live_server_rejection_leaves_partial_writes_and_explicit_replay_recovers(
    publication: tuple[Path, PublishPlan], mongo_database: Database[dict[str, Any]]
) -> None:
    """A real server validation error stops publication without returning a complete receipt."""
    from pymongo.errors import WriteError

    _, plan = publication
    assert plan.source_rows > 2
    collection = mongo_database.create_collection(
        f"ons_pay_{plan.table}", validator={"_id": {"$ne": plan.records[1]["_id"]}}
    )
    with pytest.raises(PublishSinkError, match="incomplete") as error:
        publish_and_accept_mongodb(plan, writer=collection, reader=collection)
    assert isinstance(error.value.__cause__, WriteError)
    assert error.value.__cause__.code == 121  # MongoDB DocumentValidationFailure.
    assert _snapshot(collection) == {str(plan.records[0]["_id"]): plan.records[0]}
    partial = reconcile_mongodb(plan, collection)
    assert not partial.passed
    assert partial.observed_count == 1
    assert len(partial.missing_ids) == plan.source_rows - 1
    # Removing our injected failure does not itself resume or repair publication.
    mongo_database.command("collMod", collection.name, validator={})
    assert collection.count_documents({}) == 1
    accepted = publish_and_accept_mongodb(plan, writer=collection, reader=collection)
    assert accepted.records_accepted == plan.source_rows
    assert _snapshot(collection) == {str(record["_id"]): record for record in plan.records}


@pytest.mark.skipif(os.name != "posix", reason="durable recording requires POSIX locks/fsync")
def test_live_cli_failed_rejected_and_accepted_evidence_are_immutable(
    publication: tuple[Path, PublishPlan],
    mongo_database: Database[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Real partial writes and drift cannot create acceptance; a new attempt can recover."""
    root, plan = publication
    collection = mongo_database.create_collection(
        f"ons_pay_{plan.table}", validator={"_id": {"$ne": plan.records[1]["_id"]}}
    )
    args = [*_args(root, plan), "--apply", "--record-evidence"]
    failed = _invoke(capsys, mongo_database, args, exit_code=4)
    assert failed["status"] == "failed" and failed["acceptance"] is None
    assert failed["counts"]["applied"] is None
    assert collection.count_documents({}) == 1
    archive = PublicationArchive(root)
    failed_record = archive.get_attempt(failed["evidence"]["attempt_id"])
    assert failed_record is not None
    assert failed_record.to_dict()["outcome"] == "failed"
    assert failed_record.to_dict()["receipt"] is None
    assert failed_record.to_dict()["acceptance"] is None
    mongo_database.command("collMod", collection.name, validator={})
    repeated = _invoke(
        capsys,
        mongo_database,
        [*args, "--attempt-id", failed["evidence"]["attempt_id"]],
        exit_code=4,
    )
    assert repeated["evidence"]["replayed"]
    assert collection.count_documents({}) == 1
    accepted = _invoke(capsys, mongo_database, args)
    assert accepted["status"] == "accepted"
    assert accepted["counts"]["applied"] == accepted["counts"]["observed"] == plan.source_rows
    accepted_record = archive.get_attempt(accepted["evidence"]["attempt_id"])
    assert accepted_record is not None
    assert accepted_record.to_dict()["acceptance"] == accepted["acceptance"]
    verified = _invoke(capsys, mongo_database, [*_args(root, plan), "--verify"])
    assert verified["status"] == "verified" and verified["acceptance"] is None
    extra = copy.deepcopy(plan.records[0])
    extra["_id"] = "f" * 64
    collection.insert_one(extra)
    rejected = _invoke(capsys, mongo_database, args, exit_code=3)
    assert rejected["status"] == "rejected" and rejected["acceptance"] is None
    rejected_record = archive.get_attempt(rejected["evidence"]["attempt_id"])
    assert rejected_record is not None
    assert rejected_record.to_dict()["outcome"] == "rejected"
    assert rejected_record.to_dict()["acceptance"] is None
    assert rejected["reconciliation"]["unexpected_count"] == 1
    collection.delete_one({"_id": extra["_id"]})
    recovered = _invoke(capsys, mongo_database, args)
    assert recovered["status"] == "accepted"
    # All old outcomes remain byte-identical, including historical acceptance after drift.
    assert archive.get_attempt(failed["evidence"]["attempt_id"]) == failed_record
    assert archive.get_attempt(accepted["evidence"]["attempt_id"]) == accepted_record
    assert archive.get_attempt(rejected["evidence"]["attempt_id"]) == rejected_record
    assert len(archive.history()) == 4
    assert _snapshot(collection) == {str(record["_id"]): record for record in plan.records}
