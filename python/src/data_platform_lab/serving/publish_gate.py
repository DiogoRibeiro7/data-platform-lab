"""Fail-closed acceptance of exact-vintage ONS Gold sink publications.

The gate is an in-memory evidence check: it does not publish records, advance
an active release pointer, write an audit event, or attest external identity.
Convenience wrappers explicitly publish *then* reconcile using injected IO.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import PublishPlan, PublishPlanError, TableName
from data_platform_lab.serving.publish_reconcile import (
    MongoReader,
    PostgresReader,
    ReconciliationError,
    ReconciliationReport,
    expected_sink_fingerprint,
    reconcile_mongodb,
    reconcile_postgres,
)
from data_platform_lab.serving.publish_sinks import (
    MongoCollection,
    PostgresConnection,
    PublishReceipt,
    SinkName,
    publish_mongodb,
    publish_postgres,
)


class PublicationAcceptanceError(RuntimeError):
    """The publication evidence is inconsistent or the sink has drifted."""


@dataclass(frozen=True, slots=True)
class AcceptedPublication:
    """Portable, positive evidence for one checked sink population.

    This is not a durable release marker, digital signature, or guarantee
    against a later concurrent modification of the sink.
    """

    sink: SinkName
    table: TableName
    vintage: VintageKey
    records_accepted: int
    source_plan_sha256: str
    sink_snapshot_sha256: str
    status: Literal["accepted"] = "accepted"

    def to_dict(self) -> dict[str, Any]:
        """Provide portable, JSON-ready acceptance evidence without credentials."""
        return asdict(self)


def accept_publication(
    plan: PublishPlan,
    receipt: PublishReceipt,
    report: ReconciliationReport,
) -> AcceptedPublication:
    """Require an exact, complete match between source, write, and read-back.

    A passing reconciliation alone is insufficient: the write receipt must
    identify the same sink, table, full vintage, record count and source-plan
    hash. The expected sink hash is recomputed independently from the plan,
    to reject a report carrying two mutually consistent but wrong hashes.

    No database calls or writes occur in this function. All three evidence
    objects must refer to the same publication attempt/snapshot; the caller
    remains responsible for trusted handles and read isolation.
    """
    if receipt.sink not in ("postgresql", "mongodb"):
        raise PublicationAcceptanceError("unsupported publication sink")
    try:
        expected_sink_sha256 = expected_sink_fingerprint(plan, sink=receipt.sink)
    except (PublishPlanError, ReconciliationError) as exc:
        raise PublicationAcceptanceError("source plan cannot be accepted") from exc

    if (
        receipt.table != plan.table
        or receipt.vintage != plan.vintage
        or type(receipt.records_applied) is not int
        or receipt.records_applied != plan.source_rows
        or receipt.records_sha256 != plan.records_sha256
    ):
        raise PublicationAcceptanceError("publication receipt does not match the source plan")

    if (
        report.sink != receipt.sink
        or report.table != plan.table
        or report.vintage != plan.vintage
        or report.source_plan_sha256 != plan.records_sha256
        or type(report.expected_count) is not int
        or report.expected_count != plan.source_rows
        or type(report.observed_count) is not int
        or report.observed_count != plan.source_rows
    ):
        raise PublicationAcceptanceError("read-back report does not match the publication")

    if (
        report.passed is not True
        or report.missing_ids
        or report.unexpected_ids
        or report.changed_fields
        or report.expected_sink_sha256 != expected_sink_sha256
        or report.observed_sink_sha256 != expected_sink_sha256
    ):
        raise PublicationAcceptanceError("sink read-back does not match the complete source")

    return AcceptedPublication(
        sink=receipt.sink,
        table=plan.table,
        vintage=plan.vintage,
        records_accepted=plan.source_rows,
        source_plan_sha256=plan.records_sha256,
        sink_snapshot_sha256=expected_sink_sha256,
    )


def publish_and_accept_postgres(
    plan: PublishPlan,
    *,
    writer: PostgresConnection,
    reader: PostgresReader,
    batch_size: int = 500,
) -> AcceptedPublication:
    """Commit a PostgreSQL batch, then accept only a matching SELECT snapshot.

    Writer and reader may be the same psycopg connection. A rejected read-back
    does NOT roll back the already committed write; no acceptance is returned.
    """
    receipt = publish_postgres(plan, writer, batch_size=batch_size)
    report = reconcile_postgres(plan, reader)
    return accept_publication(plan, receipt, report)


def publish_and_accept_mongodb(
    plan: PublishPlan, *, writer: MongoCollection, reader: MongoReader
) -> AcceptedPublication:
    """Upsert MongoDB documents, then accept only a full matching read-back.

    The writer and reader may be the same PyMongo collection. The document
    batch is not atomic; failures or rejected read-backs may leave writes.
    """
    receipt = publish_mongodb(plan, writer)
    report = reconcile_mongodb(plan, reader)
    return accept_publication(plan, receipt, report)
