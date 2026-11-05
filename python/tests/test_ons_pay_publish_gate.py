"""Acceptance-gate tests using real curated Gold plans and injected sinks."""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest
from test_ons_pay_publish_reconcile import (
    FakeMongoReader,
    FakePostgresReader,
    _pg_values,
)
from test_ons_pay_publish_sinks import FakeMongoCollection, FakePgConnection
from test_ons_pay_serving import _curate, _pin

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_gate import (
    AcceptedPublication,
    PublicationAcceptanceError,
    accept_publication,
    publish_and_accept_mongodb,
    publish_and_accept_postgres,
)
from data_platform_lab.serving.publish_plan import (
    PublishPlan,
    SinkShape,
    TableName,
    plan_ons_pay_publishing,
)
from data_platform_lab.serving.publish_reconcile import (
    ReconciliationError,
    ReconciliationReport,
    expected_sink_fingerprint,
    reconcile_mongodb,
    reconcile_postgres,
)
from data_platform_lab.serving.publish_sinks import (
    PublishReceipt,
    PublishSinkError,
    SinkName,
)


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, VintageKey]:
    """Build the real local Parquet and integrity receipts without any DB."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    return root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])


def _plan(curated: tuple[Path, VintageKey], table: TableName, shape: SinkShape) -> PublishPlan:
    """Project one exact-vintage Gold table into a validated sink contract."""
    root, vintage = curated
    return plan_ons_pay_publishing(data_root=root, vintage=vintage, table=table, shape=shape)


def _receipt(plan: PublishPlan, *, sink: SinkName) -> PublishReceipt:
    """Construct the exact evidence a successful adapter would return."""
    return PublishReceipt(sink, plan.table, plan.vintage, plan.source_rows, plan.records_sha256)


def _reconcile(plan: PublishPlan, *, sink: SinkName) -> ReconciliationReport:
    """Perform an independent, successful read-back with driver-shaped data."""
    if sink == "postgresql":
        return reconcile_postgres(plan, FakePostgresReader(_pg_values(plan), plan.table))
    return reconcile_mongodb(plan, FakeMongoReader(copy.deepcopy(list(plan.records))))


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
@pytest.mark.parametrize(("sink", "shape"), [("postgresql", "relational"), ("mongodb", "document")])
def test_accepts_complete_pinned_publication_without_io(
    curated: tuple[Path, VintageKey], sink: SinkName, shape: SinkShape, table: TableName
) -> None:
    plan = _plan(curated, table, shape)
    receipt = _receipt(plan, sink=sink)
    report = _reconcile(plan, sink=sink)
    accepted = accept_publication(plan, receipt, report)
    assert isinstance(accepted, AcceptedPublication)
    assert accepted.status == "accepted"
    assert accepted.sink == sink
    assert accepted.table == table
    assert accepted.vintage == plan.vintage
    assert accepted.records_accepted == plan.source_rows
    assert accepted.source_plan_sha256 == plan.records_sha256
    assert accepted.sink_snapshot_sha256 == report.expected_sink_sha256
    assert expected_sink_fingerprint(plan, sink=sink) == report.expected_sink_sha256
    assert json.loads(json.dumps(accepted.to_dict()))["status"] == "accepted"


@pytest.mark.parametrize("tamper", ["sink", "table", "vintage", "count", "hash", "boolean_count"])
def test_rejects_mismatched_or_fabricated_write_receipt(
    curated: tuple[Path, VintageKey], tamper: str
) -> None:
    plan = _plan(curated, "local_authority_yoy", "relational")
    receipt = _receipt(plan, sink="postgresql")
    if tamper == "sink":
        receipt = replace(receipt, sink="mongodb")
    elif tamper == "table":
        receipt = replace(receipt, table="geography_year")
    elif tamper == "vintage":
        receipt = replace(receipt, vintage=replace(plan.vintage, source_sha256="a" * 64))
    elif tamper == "count":
        receipt = replace(receipt, records_applied=plan.source_rows - 1)
    elif tamper == "boolean_count":
        receipt = replace(receipt, records_applied=True)
    else:
        receipt = replace(receipt, records_sha256="f" * 64)

    with pytest.raises(PublicationAcceptanceError):
        accept_publication(plan, receipt, _reconcile(plan, sink="postgresql"))


@pytest.mark.parametrize(
    "tamper",
    [
        "sink",
        "table",
        "vintage",
        "source_hash",
        "expected_count",
        "observed_count",
        "bool_count",
        "passed_false",
        "missing",
        "unexpected",
        "changed",
        "wrong_equal_hashes",
    ],
)
def test_rejects_inconsistent_readback_evidence(
    curated: tuple[Path, VintageKey], tamper: str
) -> None:
    plan = _plan(curated, "local_authority_yoy", "relational")
    report = _reconcile(plan, sink="postgresql")
    if tamper == "sink":
        report = replace(report, sink="mongodb")
    elif tamper == "table":
        report = replace(report, table="geography_year")
    elif tamper == "vintage":
        report = replace(report, vintage=replace(plan.vintage, registry_commit="b" * 40))
    elif tamper == "source_hash":
        report = replace(report, source_plan_sha256="a" * 64)
    elif tamper == "expected_count":
        report = replace(report, expected_count=plan.source_rows - 1)
    elif tamper == "observed_count":
        report = replace(report, observed_count=plan.source_rows + 1)
    elif tamper == "bool_count":
        report = replace(report, expected_count=True)
    elif tamper == "passed_false":
        report = replace(report, passed=False)
    elif tamper == "missing":
        report = replace(report, missing_ids=("f" * 64,))
    elif tamper == "unexpected":
        report = replace(report, unexpected_ids=("f" * 64,))
    elif tamper == "changed":
        report = replace(report, changed_fields={"f" * 64: ("status_code",)})
    else:
        # Two identical hashes still fail if neither agrees with the plan.
        report = replace(report, expected_sink_sha256="f" * 64, observed_sink_sha256="f" * 64)

    with pytest.raises(PublicationAcceptanceError):
        accept_publication(plan, _receipt(plan, sink="postgresql"), report)


@pytest.mark.parametrize("sink", ["postgresql", "mongodb"])
def test_normal_reconciliation_drift_never_produces_acceptance(
    curated: tuple[Path, VintageKey], sink: SinkName
) -> None:
    shape: SinkShape = "relational" if sink == "postgresql" else "document"
    plan = _plan(curated, "geography_year", shape)
    if sink == "postgresql":
        report = reconcile_postgres(plan, FakePostgresReader(_pg_values(plan)[:-1], plan.table))
    else:
        report = reconcile_mongodb(plan, FakeMongoReader(list(plan.records)[:-1]))
    assert not report.passed
    with pytest.raises(PublicationAcceptanceError, match="read-back"):
        accept_publication(plan, _receipt(plan, sink=sink), report)


def test_tampered_source_plan_cannot_be_accepted(curated: tuple[Path, VintageKey]) -> None:
    plan = _plan(curated, "geography_year", "relational")
    with pytest.raises(PublicationAcceptanceError, match="source plan"):
        accept_publication(
            replace(plan, records_sha256="f" * 64),
            _receipt(plan, sink="postgresql"),
            _reconcile(plan, sink="postgresql"),
        )


class CommittedPostgresReader:
    """Read only what an injected writer has already committed."""

    def __init__(self, writer: FakePgConnection, table: TableName) -> None:
        self.writer = writer
        self.table = table
        self.reads = 0

    def execute(self, query: str, params: tuple[object, ...]) -> object:
        self.reads += 1
        assert self.writer.commits == 1
        return FakePostgresReader(list(self.writer.persisted.values()), self.table).execute(
            query, params
        )


class PublishedMongoReader:
    """Read only acknowledged replacements already stored by the writer."""

    def __init__(self, writer: FakeMongoCollection, expected_writes: int) -> None:
        self.writer = writer
        self.expected_writes = expected_writes
        self.reads = 0

    def find(self, filter: dict[str, object]) -> object:
        self.reads += 1
        assert len(self.writer.calls) == self.expected_writes
        return FakeMongoReader(list(self.writer.documents.values())).find(filter)


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_postgres_wrapper_reads_committed_rows_before_accepting(
    curated: tuple[Path, VintageKey], table: TableName
) -> None:
    plan = _plan(curated, table, "relational")
    writer = FakePgConnection()
    reader = CommittedPostgresReader(writer, table)
    accepted = publish_and_accept_postgres(plan, writer=writer, reader=reader, batch_size=2)
    assert accepted.records_accepted == len(writer.persisted) == plan.source_rows
    assert writer.commits == 1
    assert reader.reads == 1


@pytest.mark.parametrize("table", ["geography_year", "local_authority_yoy"])
def test_mongodb_wrapper_reads_all_replacements_before_accepting(
    curated: tuple[Path, VintageKey], table: TableName
) -> None:
    plan = _plan(curated, table, "document")
    writer = FakeMongoCollection()
    reader = PublishedMongoReader(writer, plan.source_rows)
    accepted = publish_and_accept_mongodb(plan, writer=writer, reader=reader)
    assert accepted.records_accepted == len(writer.documents) == plan.source_rows
    assert reader.reads == 1


def test_postgres_rejected_readback_does_not_disguise_prior_commit(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, "local_authority_yoy", "relational")
    writer = FakePgConnection()
    missing = FakePostgresReader([], plan.table)
    with pytest.raises(PublicationAcceptanceError, match="read-back"):
        publish_and_accept_postgres(plan, writer=writer, reader=missing)
    assert writer.commits == 1
    assert len(writer.persisted) == plan.source_rows
    assert len(missing.calls) == 1


def test_mongodb_rejected_readback_does_not_disguise_prior_replacements(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, "local_authority_yoy", "document")
    writer = FakeMongoCollection()
    missing = FakeMongoReader([])
    with pytest.raises(PublicationAcceptanceError, match="read-back"):
        publish_and_accept_mongodb(plan, writer=writer, reader=missing)
    assert len(writer.documents) == plan.source_rows
    assert len(missing.calls) == 1


def test_read_error_after_commit_is_reported_without_acceptance(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, "geography_year", "relational")
    writer = FakePgConnection()
    with pytest.raises(ReconciliationError, match="SELECT failed"):
        publish_and_accept_postgres(
            plan, writer=writer, reader=FakePostgresReader([], plan.table, fail=True)
        )
    assert writer.commits == 1


def test_publish_error_does_not_attempt_any_read(
    curated: tuple[Path, VintageKey],
) -> None:
    plan = _plan(curated, "geography_year", "document")
    writer = FakeMongoCollection(fail_at=2)
    reader = FakeMongoReader([])
    with pytest.raises(PublishSinkError, match="incomplete"):
        publish_and_accept_mongodb(plan, writer=writer, reader=reader)
    assert not reader.calls
    assert len(writer.documents) == 1
