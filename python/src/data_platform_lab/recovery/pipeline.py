"""Recoverable broker-to-object-storage-to-Iceberg ingestion path."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from typing import Any

from data_platform_lab.broker import BrokerMessage, EventBroker
from data_platform_lab.iceberg import IcebergTableStore
from data_platform_lab.metadata import RunStore
from data_platform_lab.observability import RunMetadata
from data_platform_lab.storage import BlobStore
from data_platform_lab.streaming.processor import parse_event_time, validate_event

_PIPELINE_NAME = "broker_to_iceberg"


@dataclass(frozen=True, slots=True)
class RecoveryResult:
    """Outcome of processing one broker position."""

    ingestion_id: str
    raw_object_key: str
    replayed: bool
    appended: bool


class RecoverableIngestionPipeline:
    """Persist one broker message durably before acknowledging its offset."""

    def __init__(
        self,
        *,
        broker: EventBroker,
        run_store: RunStore,
        blob_store: BlobStore,
        iceberg_store: IcebergTableStore,
        table_identifier: str = "analytics.sensor_events",
    ) -> None:
        self._broker = broker
        self._run_store = run_store
        self._blob_store = blob_store
        self._iceberg_store = iceberg_store
        self._table_identifier = table_identifier

    @staticmethod
    def ingestion_id(message: BrokerMessage) -> str:
        """Return the deterministic identity for one Kafka position."""
        return f"{message.topic}:{message.partition}:{message.offset}"

    @staticmethod
    def raw_object_key(message: BrokerMessage) -> str:
        """Return the deterministic object key for one broker payload."""
        return f"ingestion/{message.topic}/{message.partition}/{message.offset}.json"

    def process(
        self,
        message: BrokerMessage,
        *,
        fail_after_iceberg: bool = False,
    ) -> RecoveryResult:
        """Process one message and acknowledge only after durable success.

        ``fail_after_iceberg`` exists solely for the recovery integration test.
        It simulates a crash after the analytical commit but before final run
        metadata and Kafka acknowledgement.
        """
        ingestion_id = self.ingestion_id(message)
        raw_key = self.raw_object_key(message)
        if not self._run_store.acquire_claim(_PIPELINE_NAME, ingestion_id):
            raise RuntimeError(f"broker position is already being processed: {ingestion_id}")

        try:
            previous = self._run_store.get(_PIPELINE_NAME, ingestion_id)
            if previous is not None and previous.status == "success":
                self._broker.acknowledge(message)
                return RecoveryResult(ingestion_id, raw_key, replayed=True, appended=False)

            started_at = datetime.now(UTC)
            self._run_store.save(
                RunMetadata(
                    pipeline_name=_PIPELINE_NAME,
                    run_id=ingestion_id,
                    status="running",
                    started_at=started_at.isoformat(),
                    rows_read=1,
                    extra={"raw_object_key": raw_key},
                )
            )

            appended = False
            raw_stored = False
            try:
                self._blob_store.put_bytes(raw_key, message.value)
                raw_stored = True
                event = self._decode_event(message.value)
                schema, batch = self._arrow_batch(ingestion_id, message, event)
                self._iceberg_store.ensure_table(self._table_identifier, schema)

                if not self._iceberg_store.contains_value(
                    self._table_identifier, "ingestion_id", ingestion_id
                ):
                    self._iceberg_store.append(self._table_identifier, batch)
                    appended = True

                if fail_after_iceberg:
                    raise RuntimeError("injected failure after Iceberg commit")

                ended_at = datetime.now(UTC)
                self._run_store.save(
                    RunMetadata(
                        pipeline_name=_PIPELINE_NAME,
                        run_id=ingestion_id,
                        status="success",
                        started_at=started_at.isoformat(),
                        ended_at=ended_at.isoformat(),
                        duration_seconds=(ended_at - started_at).total_seconds(),
                        rows_read=1,
                        rows_written=1,
                        files_processed=1,
                        extra={
                            "raw_object_key": raw_key,
                            "table": self._table_identifier,
                            "reconciled": not appended,
                        },
                    )
                )
            except Exception as exc:
                ended_at = datetime.now(UTC)
                self._run_store.save(
                    RunMetadata(
                        pipeline_name=_PIPELINE_NAME,
                        run_id=ingestion_id,
                        status="failed",
                        started_at=started_at.isoformat(),
                        ended_at=ended_at.isoformat(),
                        duration_seconds=(ended_at - started_at).total_seconds(),
                        rows_read=1,
                        rows_written=1 if appended else 0,
                        files_processed=1 if raw_stored else 0,
                        errors=[str(exc)],
                        extra={"raw_object_key": raw_key, "table": self._table_identifier},
                    )
                )
                raise

            self._broker.acknowledge(message)
            return RecoveryResult(
                ingestion_id,
                raw_key,
                replayed=previous is not None,
                appended=appended,
            )
        finally:
            self._run_store.release_claim(_PIPELINE_NAME, ingestion_id)

    @staticmethod
    def _decode_event(payload: bytes) -> dict[str, Any]:
        """Decode and validate one sensor event payload."""
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("broker payload must contain valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("broker payload must decode to a JSON object")

        result = validate_event(decoded)
        if result.status != "accepted":
            raise ValueError(result.reason or "invalid sensor event")
        return decoded

    @staticmethod
    def _arrow_batch(
        ingestion_id: str,
        message: BrokerMessage,
        event: dict[str, Any],
    ) -> tuple[Any, Any]:
        """Build the canonical analytical schema and one-row Arrow batch."""
        try:
            pa = import_module("pyarrow")
        except ModuleNotFoundError as exc:
            raise RuntimeError("PyArrow is required for recovery ingestion") from exc

        schema = pa.schema(
            [
                ("ingestion_id", pa.string()),
                ("topic", pa.string()),
                ("partition", pa.int32()),
                ("offset", pa.int64()),
                ("sensor_id", pa.string()),
                ("type", pa.string()),
                ("value", pa.float64()),
                ("unit", pa.string()),
                ("location", pa.string()),
                ("event_time", pa.timestamp("us", tz="UTC")),
            ]
        )
        row = {
            "ingestion_id": ingestion_id,
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
            "sensor_id": event["sensor_id"],
            "type": event["type"],
            "value": float(event["value"]),
            "unit": event["unit"],
            "location": event["location"],
            "event_time": parse_event_time(event["timestamp"]),
        }
        return schema, pa.Table.from_pylist([row], schema=schema)
