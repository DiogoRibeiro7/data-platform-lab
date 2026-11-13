"""Bounded, read-only verification of offline transformation audit lineage."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
from datetime import datetime
from pathlib import Path
from typing import Any

from data_platform_lab.curation.ons_pay import MODEL_VERSION, parse_ingestion_receipt
from data_platform_lab.observability.ons_pay_monitor import BatchAudit, MonitorError, _aware_utc
from data_platform_lab.orchestration.ons_pay_refresh_job import _reject_constant, _unique_object
from data_platform_lab.serving.catalog import VintageKey

_MAX_FILES = 10_000
_MAX_FILE_BYTES = 4 * 1024 * 1024
_MAX_TOTAL_BYTES = 64 * 1024 * 1024
_COUNTS = (
    "source_rows",
    "rows_with_pay",
    "rows_without_pay",
    "parquet_published",
    "parquet_reused",
)


def _object(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise MonitorError("invalid transformation evidence object")
    return value


def _count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise MonitorError("invalid transformation evidence count")
    return value


def _hex(value: object, length: int) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{" + str(length) + r"}", value) is None:
        raise MonitorError("invalid transformation evidence identity")
    return value


def _path(value: object) -> Path:
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise MonitorError("transformation evidence must use absolute paths")
    return Path(value)


class _Evidence:
    """Read each linked file once, bounding total bytes as well as audit count."""

    def __init__(self) -> None:
        self.bytes_read = 0
        self.cache: dict[Path, tuple[dict[str, Any], str]] = {}

    def read(self, path: Path) -> tuple[dict[str, Any], str]:
        if path in self.cache:
            return self.cache[path]
        if len(self.cache) >= _MAX_FILES:
            raise MonitorError("transformation evidence exceeds the file limit")
        if not path.is_file() or path.resolve(strict=True) != path:
            raise MonitorError("unsafe transformation evidence path")
        allowance = min(_MAX_FILE_BYTES, _MAX_TOTAL_BYTES - self.bytes_read)
        with path.open("rb") as stream:
            raw = stream.read(allowance + 1)
        if len(raw) > allowance:
            raise MonitorError("transformation evidence exceeds the byte limit")
        self.bytes_read += len(raw)
        result = (
            _object(
                json.loads(raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
            ),
            hashlib.sha256(raw).hexdigest(),
        )
        self.cache[path] = result
        return result


def _completed_source(
    item: dict[str, Any],
    loaded: dict[str, Any],
    evidence: _Evidence,
    directory: Path,
    started: datetime,
    finished: datetime,
) -> VintageKey:
    key = VintageKey(
        MODEL_VERSION, _hex(item.get("registry_commit"), 40), _hex(item.get("source_sha256"), 64)
    )
    ingestion_path = _path(item.get("ingestion_receipt_path"))
    ingestion, digest = evidence.read(ingestion_path)
    source = parse_ingestion_receipt(ingestion)
    if (
        digest != _hex(item.get("ingestion_receipt_sha256"), 64)
        or source.registry_commit != key.registry_commit
        or source.source_sha256 != key.source_sha256
        or loaded.get("registry_commit") != key.registry_commit
        or loaded.get("source_sha256") != key.source_sha256
        or _path(loaded.get("ingestion_receipt_path")) != ingestion_path
    ):
        raise MonitorError("transformation source differs from its load/ingestion evidence")
    curation_path = _path(item.get("curation_receipt_path"))
    if curation_path.parent != directory:
        raise MonitorError("curation evidence must be in the selected data root")
    curation, _ = evidence.read(curation_path)
    if (
        curation.get("pipeline_name") != "ons_pay_curation"
        or curation.get("status") != "success"
        or type(curation.get("model_version")) is not int
        or curation["model_version"] != MODEL_VERSION
        or curation_path.name != f"ons_pay_curation_{_hex(curation.get('run_id'), 32)}.json"
        or not started <= _aware_utc(curation.get("created_at")) <= finished
    ):
        raise MonitorError("invalid linked curation evidence")
    provenance = _object(curation.get("input"))
    if (
        provenance.get("registry_commit") != key.registry_commit
        or provenance.get("source_sha256") != key.source_sha256
        or provenance.get("ingestion_receipt_sha256") != digest
        or provenance.get("ingestion_run_id") != source.run_id
        or provenance.get("manifest_sha256") != source.manifest_sha256
    ):
        raise MonitorError("curation lineage differs from the transformation input")
    quality = _object(curation.get("quality"))
    for field in _COUNTS[:3]:
        if _count(quality.get(field)) != _count(item.get(field)):
            raise MonitorError("transformation row counts differ from curation")
    outputs = curation.get("outputs")
    if (
        not isinstance(outputs, list)
        or len(outputs) != 3
        or any(_object(output).get("status") not in {"published", "reused"} for output in outputs)
        or sum(output["status"] == "published" for output in outputs)
        != _count(item.get("parquet_published"))
        or sum(output["status"] == "reused" for output in outputs)
        != _count(item.get("parquet_reused"))
        or _count(item.get("source_rows")) < 1
        or _count(item.get("rows_with_pay")) < 1
        or item["rows_with_pay"] + _count(item.get("rows_without_pay")) != item["source_rows"]
    ):
        raise MonitorError("inconsistent transformation output counts")
    return key


def _validate(path: Path, evidence: _Evidence, checked_at: datetime) -> BatchAudit:
    audit, _ = evidence.read(path)
    if (
        audit.get("pipeline_name") != "ons_pay_transform"
        or type(audit.get("schema_version")) is not int
        or audit["schema_version"] != 1
        or type(audit.get("model_version")) is not int
        or audit["model_version"] != MODEL_VERSION
    ):
        raise MonitorError("unknown transformation audit schema")
    run_id = _hex(audit.get("run_id"), 32)
    if path.name != f"ons_pay_transform_{run_id}.json":
        raise MonitorError("transformation audit filename and run identity disagree")
    started, finished = _aware_utc(audit.get("started_at")), _aware_utc(audit.get("created_at"))
    duration = audit.get("duration_seconds")
    if (
        started > finished
        or finished > checked_at
        or type(duration) not in {int, float}
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
        or abs(duration - (finished - started).total_seconds()) > 0.000001
    ):
        raise MonitorError("inconsistent transformation audit timing")
    status = audit.get("status")
    if status not in {"success", "failed"}:
        raise MonitorError("unknown transformation outcome")
    sources = audit.get("sources")
    if not isinstance(sources, list) or len(sources) > 256:
        raise MonitorError("invalid completed transformation sources")
    inputs = _object(audit.get("input"))
    load_path = _path(inputs.get("load_audit_path"))
    expected = inputs.get("expected_load_sha256")
    if expected is not None:
        _hex(expected, 64)
    planned = audit.get("sources_planned")
    failure = audit.get("failure")
    preflight_failure = status == "failed" and _object(failure).get("phase") == "preflight"
    if preflight_failure:
        failure = _object(failure)
        if (
            sources
            or planned is not None
            or failure.get("source_index") is not None
            or any(
                inputs.get(field) is not None
                for field in ("load_audit_sha256", "load_run_id", "load_job_sha256")
            )
        ):
            raise MonitorError("preflight failure claims completed transformation progress")
        loaded_sources: list[Any] = []
    else:
        planned_count = _count(planned)
        if not 1 <= planned_count <= 256 or len(sources) > planned_count:
            raise MonitorError("invalid transformation plan count")
        load, load_sha = evidence.read(load_path)
        if (
            load_sha != _hex(inputs.get("load_audit_sha256"), 64)
            or (expected is not None and expected != load_sha)
            or _hex(inputs.get("load_run_id"), 32) != load.get("run_id")
            or inputs.get("load_job_sha256") != load.get("job_sha256")
            or load.get("pipeline_name") != "ons_pay_load"
            or load.get("status") != "success"
            or type(load.get("schema_version")) is not int
            or load["schema_version"] != 1
            or load.get("failure") is not None
            or "model_version" in load
        ):
            raise MonitorError("transformation input differs from its successful load audit")
        if inputs.get("load_job_sha256") is not None:
            _hex(inputs["load_job_sha256"], 64)
        selected_sources = load.get("sources")
        if not isinstance(selected_sources, list) or len(selected_sources) != planned_count:
            raise MonitorError("transformation plan differs from its load source count")
        loaded_sources = selected_sources
        if status == "success":
            if failure is not None or len(sources) != planned:
                raise MonitorError("successful transformation does not cover the whole batch")
        else:
            failure = _object(failure)
            if (
                failure.get("phase") != "curation"
                or _count(failure.get("source_index")) != len(sources)
                or len(sources) >= planned_count
            ):
                raise MonitorError("transformation failure and completed progress disagree")
    completed: set[VintageKey] = set()
    totals = dict.fromkeys(_COUNTS, 0)
    for index, source in enumerate(sources):
        item = _object(source)
        key = _completed_source(
            item, _object(loaded_sources[index]), evidence, path.parent, started, finished
        )
        if key in completed:
            raise MonitorError("duplicate completed transformation vintage")
        completed.add(key)
        for field in _COUNTS:
            totals[field] += _count(item.get(field))
    metrics = _object(audit.get("metrics"))
    if _count(metrics.get("vintages_completed")) != len(completed) or any(
        _count(metrics.get(field)) != count for field, count in totals.items()
    ):
        raise MonitorError("transformation aggregate counts disagree")
    return BatchAudit(
        run_id, finished, "success" if status == "success" else "failed", frozenset(completed)
    )


def read_transform_audits(data_root: Path, checked_at: datetime) -> tuple[BatchAudit, ...]:
    """Verify retained batch and completed-source metadata; never open a source/store.

    A preflight failure may have no valid load evidence. Failed curation batches
    validate only their completed prefix, so a missing failed input stays a run
    failure instead of erasing its diagnostic identity. Current selected-vintage
    Parquet integrity is checked separately by the common monitor.
    """
    try:
        directory = data_root / "manifests"
        try:
            mode = directory.lstat().st_mode
        except FileNotFoundError:
            return ()
        if not stat.S_ISDIR(mode) or directory.resolve() != directory:
            raise MonitorError("unsafe transformation audit directory")
        evidence = _Evidence()
        audits: list[BatchAudit] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.name.startswith("ons_pay_transform_") and entry.name.endswith(".json"):
                    audits.append(_validate(Path(entry.path), evidence, checked_at))
        return tuple(sorted(audits, key=lambda audit: (audit.created_at, audit.run_id)))
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, OverflowError):
        raise MonitorError("cannot verify transformation audits and linked evidence") from None
