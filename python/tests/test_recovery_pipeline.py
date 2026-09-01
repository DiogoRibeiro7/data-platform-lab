"""Tests for end-to-end recovery orchestration."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from data_platform_lab.broker import BrokerMessage
from data_platform_lab.observability import RunMetadata
from data_platform_lab.recovery import RecoverableIngestionPipeline
from data_platform_lab.storage import LocalBlobStore


class FakeBroker:
    def __init__(self) -> None:
        self.acknowledged: list[BrokerMessage] = []

    def acknowledge(self, message: BrokerMessage) -> None:
        self.acknowledged.append(message)


class FakeRunStore:
    def __init__(self) -> None:
        self.runs: dict[tuple[str, str], RunMetadata] = {}

    def save(self, metadata: RunMetadata) -> None:
        self.runs[(metadata.pipeline_name, metadata.run_id)] = metadata

    def get(self, pipeline_name: str, run_id: str) -> RunMetadata | None:
        return self.runs.get((pipeline_name, run_id))

    def list_recent(self, limit: int = 20) -> list[RunMetadata]:
        return list(self.runs.values())[-limit:]


class FakeIcebergStore:
    def __init__(self) -> None:
        self.ingestion_ids: set[str] = set()
        self.append_count = 0

    def ensure_table(self, identifier: str, schema: Any) -> None:
        del identifier, schema

    def contains_value(self, identifier: str, field_name: str, value: object) -> bool:
        del identifier
        assert field_name == "ingestion_id"
        return str(value) in self.ingestion_ids

    def append(self, identifier: str, table_data: Any) -> None:
        del identifier
        ingestion_id = str(table_data.column("ingestion_id").to_pylist()[0])
        self.ingestion_ids.add(ingestion_id)
        self.append_count += 1


def _message() -> BrokerMessage:
    payload = json.dumps(
        {
            "sensor_id": "sensor-1",
            "type": "temperature",
            "value": 21.5,
            "unit": "C",
            "location": "lab",
            "timestamp": "2026-09-01T12:00:00+00:00",
        }
    ).encode()
    return BrokerMessage("sensor-events", 0, 7, b"sensor-1", payload)


def _pipeline(tmp_path: Path) -> tuple[RecoverableIngestionPipeline, FakeBroker, FakeRunStore, FakeIcebergStore]:
    broker = FakeBroker()
    run_store = FakeRunStore()
    iceberg_store = FakeIcebergStore()
    pipeline = RecoverableIngestionPipeline(
        broker=broker,  # type: ignore[arg-type]
        run_store=run_store,
        blob_store=LocalBlobStore(tmp_path),
        iceberg_store=iceberg_store,  # type: ignore[arg-type]
    )
    return pipeline, broker, run_store, iceberg_store


def test_recovery_pipeline_acknowledges_after_durable_success(tmp_path: Path) -> None:
    pipeline, broker, run_store, iceberg_store = _pipeline(tmp_path)
    message = _message()

    result = pipeline.process(message)

    assert result.ingestion_id == "sensor-events:0:7"
    assert result.appended is True
    assert iceberg_store.append_count == 1
    assert broker.acknowledged == [message]
    assert run_store.get("broker_to_iceberg", result.ingestion_id).status == "success"  # type: ignore[union-attr]
    assert (tmp_path / result.raw_object_key).read_bytes() == message.value


def test_replay_after_iceberg_commit_does_not_duplicate(tmp_path: Path) -> None:
    pipeline, broker, run_store, iceberg_store = _pipeline(tmp_path)
    message = _message()

    with pytest.raises(RuntimeError, match="injected failure"):
        pipeline.process(message, fail_after_iceberg=True)

    assert iceberg_store.append_count == 1
    assert broker.acknowledged == []
    failed = run_store.get("broker_to_iceberg", "sensor-events:0:7")
    assert failed is not None and failed.status == "failed"

    result = pipeline.process(message)

    assert result.replayed is True
    assert result.appended is False
    assert iceberg_store.append_count == 1
    assert broker.acknowledged == [message]
    recovered = run_store.get("broker_to_iceberg", "sensor-events:0:7")
    assert recovered is not None and recovered.status == "success"
