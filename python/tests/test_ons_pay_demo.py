"""Offline checks for the demo's real source verification and failure reporting."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from data_platform_lab import ons_pay_demo as demo
from data_platform_lab.curation.ons_pay import curate_ons_pay
from data_platform_lab.ingestion.registry_handoff import HandoffError, ingest_handoff
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_evidence import PublicationArchive
from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing
from data_platform_lab.storage import LocalBlobStore


def test_synthetic_source_is_deterministic_and_verifies_real_committed_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Host identity/date settings cannot alter the fixture's commit or content hash."""
    first = demo.prepare_source(tmp_path / "first")
    monkeypatch.setenv("GIT_AUTHOR_NAME", "Unrelated user")
    monkeypatch.setenv("GIT_COMMITTER_DATE", "2030-01-01T00:00:00+00:00")
    second = demo.prepare_source(tmp_path / "second")
    assert first.registry_commit == second.registry_commit
    assert first.source_sha256 == second.source_sha256
    assert first.manifest_path.read_bytes() == second.manifest_path.read_bytes()
    store = LocalBlobStore(tmp_path / "objects")
    initial = ingest_handoff(
        manifest_path=first.manifest_path,
        registry_root=first.registry_root,
        store=store,
        receipt_dir=tmp_path / "receipts",
    )
    repeated = ingest_handoff(
        manifest_path=first.manifest_path,
        registry_root=first.registry_root,
        store=store,
        receipt_dir=tmp_path / "receipts",
    )
    assert initial.files_published == repeated.files_reused == 1
    assert initial.files_reused == repeated.files_published == 0
    receipt = json.loads(initial.receipt_path.read_text())
    assert "synthetic" in receipt["source"]["publisher"]
    assert "not official ONS statistics" in receipt["citation"]
    curated_root = tmp_path / "curated"
    curated = curate_ons_pay(
        input_receipt=initial.receipt_path, store=store, output_root=curated_root
    )
    assert (curated.source_rows, curated.rows_with_pay, curated.rows_without_pay) == (8, 7, 1)
    assert {item.path.name: item.rows for item in curated.outputs} == {
        "observations.parquet": 8,
        "geography_year.parquet": 5,
        "local_authority_yoy.parquet": 7,
    }
    vintage = VintageKey(1, first.registry_commit, first.source_sha256)
    for table in ("geography_year", "local_authority_yoy"):
        for shape in ("relational", "document"):
            plan = plan_ons_pay_publishing(
                data_root=curated_root, vintage=vintage, table=table, shape=shape
            )
            assert plan == plan_ons_pay_publishing(
                data_root=curated_root, vintage=vintage, table=table, shape=shape
            )
            assert plan.source_rows == (5 if table == "geography_year" else 7)


@pytest.mark.parametrize("rewrite_checksum", [False, True])
def test_modified_fixture_cannot_bypass_production_ingestion(
    tmp_path: Path, rewrite_checksum: bool
) -> None:
    """Reject both checksum drift and a handoff rewritten to match an uncommitted CSV."""
    source = demo.prepare_source(tmp_path / "work")
    manifest = json.loads(source.manifest_path.read_text())
    item = manifest["objects"][0]
    path = source.registry_root / item["source_path"]
    payload = path.read_bytes().replace(b",400,Pounds,", b",405,Pounds,")
    path.write_bytes(payload)
    if rewrite_checksum:
        digest = hashlib.sha256(payload).hexdigest()
        item["storage_key"] = item["storage_key"].replace(item["sha256"], digest)
        item["sha256"] = digest
        item["size_bytes"] = len(payload)
        source.manifest_path.write_text(json.dumps(manifest))
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(HandoffError, match="pinned Git blob" if rewrite_checksum else "checksum"):
        ingest_handoff(
            manifest_path=source.manifest_path,
            registry_root=source.registry_root,
            store=store,
            receipt_dir=tmp_path / "receipts",
        )
    assert not (tmp_path / "receipts").exists()
    assert not store.exists(item["storage_key"])


@pytest.mark.skipif(os.name != "posix", reason="the demo runner uses Linux evidence persistence")
def test_database_failure_records_failure_and_never_attempts_automatic_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve diagnostics and failed evidence without leaking a driver exception."""
    open_sink = Mock(side_effect=OSError("secret connection string and record contents"))
    monkeypatch.setattr(demo, "_open_demo_sink", open_sink)
    report = demo.run_demo(tmp_path, exercise_recovery=True)
    assert report["status"] == "failed"
    assert report["stages"][-1]["stage"] == "publish"
    assert report["stages"][-1]["status"] == "failed"
    assert open_sink.call_count == 1
    assert not any(stage["stage"] == "explicit_recovery" for stage in report["stages"])
    assert "secret" not in (tmp_path / "report.json").read_text()
    history = PublicationArchive(tmp_path / "work/curated").history()
    assert len(history) == 1
    assert history[0].to_dict()["outcome"] == "failed"
    assert history[0].to_dict()["acceptance"] is None


@pytest.mark.skipif(os.name != "posix", reason="the demo runner uses Linux evidence persistence")
def test_existing_report_and_workspace_are_never_replaced(tmp_path: Path) -> None:
    """A direct invocation cannot overwrite a previous run or unrelated artifacts."""
    report = tmp_path / "report.json"
    report.write_text("prior report")
    with pytest.raises(demo.DemoError, match="already contains"):
        demo.run_demo(tmp_path)
    assert report.read_text() == "prior report"
    other = tmp_path / "other"
    (other / "work").mkdir(parents=True)
    sentinel = other / "work/keep.txt"
    sentinel.write_text("unrelated data")
    with pytest.raises(FileExistsError):
        demo.run_demo(other)
    assert sentinel.read_text() == "unrelated data"
