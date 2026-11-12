"""Raw-only ONS batches, deferred curation and separate operational evidence."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import URLError

import pytest
from test_local_run_guard import _child_options, _holding_roots, _probe
from test_ons_pay_refresh import _source as local_source
from test_ons_pay_refresh_job import _entry, _job, _write
from test_ons_pay_refresh_job import _run as run_job
from test_ons_pay_remote_refresh import _args, _run, _source
from test_registry_fetch import PAYLOAD, SECRET, Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.curation.ons_pay import curate_ons_pay
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability.ons_pay_monitor import monitor_ons_pay
from data_platform_lab.orchestration import ons_pay_refresh as refresh
from data_platform_lab.orchestration.ons_pay_refresh_job import load_refresh_job
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.storage import LocalBlobStore


def _audit(path: Path) -> Any:
    return json.loads(path.read_bytes())


@pytest.mark.parametrize("transport", ["checkout", "https"])
def test_raw_load_preserves_bytes_lineage_and_replay_without_curation(
    tmp_path: Path, transport: str
) -> None:
    if transport == "checkout":
        source = local_source(tmp_path)
        manifest = _audit(source.manifest_path)
        payload = (source.registry_root / manifest["objects"][0]["source_path"]).read_bytes()
    else:
        source, manifest = _source(tmp_path)
        payload = PAYLOAD
    item = manifest["objects"][0]
    with patch.object(refresh, "curate_ons_pay", side_effect=AssertionError("curation called")):
        first = _run(tmp_path, [source], [Response(item, payload)], load_only=True)
        store = LocalBlobStore(tmp_path / "objects")
        path = store.root / item["storage_key"]
        before = path.stat().st_mtime_ns
        replay = _run(tmp_path, [source], [Response(item, payload)], load_only=True)
    assert isinstance(first, refresh.LoadResult)
    assert isinstance(first.sources[0], refresh.LoadedSource)
    assert first.sources[0].objects_published == replay.sources[0].objects_reused == 1
    assert replay.sources[0].objects_published == 0
    assert store.get_bytes(item["storage_key"]) == payload
    assert path.stat().st_mtime_ns == before
    assert first.receipt_path != replay.receipt_path
    audit = _audit(replay.receipt_path)
    assert replay.receipt_path.name.startswith("ons_pay_load_")
    assert audit["pipeline_name"] == "ons_pay_load" and audit["schema_version"] == 1
    assert audit["status"] == "success" and audit["failure"] is None
    assert "model_version" not in audit
    assert audit["metrics"]["vintages_completed"] == 1
    assert audit["metrics"]["bytes_verified"] == len(payload)
    assert not {"source_rows", "parquet_published", "rows_with_pay"} & audit["metrics"].keys()
    record = audit["sources"][0]
    assert record["registry_commit"] == manifest["registry"]["commit"]
    assert record["source_sha256"] == item["sha256"]
    assert "curation_receipt_path" not in record and "silver_rows" not in record
    assert Path(record["ingestion_receipt_path"]).is_file()
    output = tmp_path / "curated"
    assert not (output / "silver").exists() and not (output / "gold").exists()
    assert not list(output.rglob("*.parquet"))
    assert not list(output.rglob("ons_pay_refresh_*.json"))


def test_saved_mixed_job_loads_raw_and_can_later_run_full_refresh(
    tmp_path: Path, capsys: Any
) -> None:
    job, document, remote = _job(tmp_path)
    local = local_source(tmp_path)
    document["sources"].append(_entry(local))
    document["load_only"] = True
    _write(job, document)
    assert load_refresh_job(job).load_only is True
    opener = Mock()
    opener.open.return_value = Response(remote["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        loaded = run_job(job, capsys)
    assert loaded["load_only"] is True and loaded["vintages_completed"] == 2
    assert "parquet_published" not in loaded and "parquet_reused" not in loaded
    load_audit = Path(loaded["receipt_path"])
    saved = load_audit.read_bytes()
    assert _audit(load_audit)["job_sha256"] == hashlib.sha256(job.read_bytes()).hexdigest()
    assert not CuratedCatalog(tmp_path / "curated").list_vintages()
    raw_times = {path: path.stat().st_mtime_ns for path in (tmp_path / "objects").rglob("*.csv")}
    document["load_only"] = False
    _write(job, document)
    opener.open.return_value = Response(remote["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        curated = run_job(job, capsys)
    assert "load_only" not in curated and curated["parquet_published"] == 6
    assert curated["objects_reused"] == 2 and curated["objects_published"] == 0
    assert curated["job_sha256"] != loaded["job_sha256"]
    assert {path: path.stat().st_mtime_ns for path in raw_times} == raw_times
    assert load_audit.read_bytes() == saved
    assert len(CuratedCatalog(tmp_path / "curated").list_vintages()) == 2


def test_loaded_ingestion_receipt_supports_separate_offline_curation(tmp_path: Path) -> None:
    source, manifest = _source(tmp_path)
    loaded = _run(tmp_path, [source], [Response(manifest["objects"][0])], load_only=True)
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError("network opened")):
        curated = curate_ons_pay(
            input_receipt=loaded.sources[0].ingestion_receipt_path,
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
        )
    assert curated.source_rows == 7 and len(curated.outputs) == 3
    assert not list((tmp_path / "curated").rglob("ons_pay_refresh_*.json"))


def test_load_only_does_not_claim_csv_quality_checks(tmp_path: Path) -> None:
    payload = b"unsupported,columns\nraw,data\n"
    source, manifest = _source(tmp_path, payload=payload)
    loaded = _run(tmp_path, [source], [Response(manifest["objects"][0], payload)], load_only=True)
    assert loaded.sources[0].bytes_verified == len(payload)
    with pytest.raises(refresh.RefreshError) as caught:
        _run(tmp_path, [source], [Response(manifest["objects"][0], payload)])
    assert _audit(caught.value.receipt_path)["failure"]["phase"] == "curation"
    assert _audit(loaded.receipt_path)["status"] == "success"


def test_partial_load_failure_is_audited_and_replay_resumes(tmp_path: Path) -> None:
    first, one = _source(tmp_path)
    payload = PAYLOAD.replace(b"200,Pounds", b"205,Pounds")
    second, two = _source(tmp_path, "b", payload)
    with pytest.raises(refresh.RefreshError) as caught:
        _run(
            tmp_path,
            [first, second],
            [Response(one["objects"][0]), URLError(SECRET)],
            load_only=True,
        )
    failed = caught.value.receipt_path
    saved = failed.read_bytes()
    audit = _audit(failed)
    assert audit["pipeline_name"] == "ons_pay_load" and audit["status"] == "failed"
    assert audit["failure"]["phase"] == "ingestion" and audit["failure"]["source_index"] == 1
    assert audit["metrics"]["vintages_completed"] == 1 and SECRET not in saved.decode()
    assert "parquet_published" not in audit["metrics"]
    replay = _run(
        tmp_path,
        [first, second],
        [Response(one["objects"][0]), Response(two["objects"][0], payload)],
        load_only=True,
    )
    assert replay.sources[0].objects_reused == replay.sources[1].objects_published == 1
    assert failed.read_bytes() == saved


@pytest.mark.parametrize("fault", ["digest", "duplicate", "stale", "checksum"])
def test_load_only_preserves_preflight_and_source_integrity_gates(
    tmp_path: Path, fault: str
) -> None:
    source, manifest = _source(tmp_path)
    sources = [source]
    policy: dict[str, Any] = {}
    if fault == "digest":
        sources = [replace(source, expected_manifest_sha256="0" * 64)]
    elif fault == "duplicate":
        sources *= 2
    elif fault == "stale":
        policy["max_source_age_days"] = 0
    opener = Mock()
    opener.open.side_effect = (
        [Response(manifest["objects"][0], b"wrong bytes")]
        if fault == "checksum"
        else AssertionError("network opened before preflight")
    )
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        pytest.raises(refresh.RefreshError) as caught,
    ):
        refresh.refresh_ons_pay(
            sources=sources,
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
            as_of=datetime(2026, 11, 10, tzinfo=UTC).date(),
            load_only=True,
            **policy,
        )
    audit = _audit(caught.value.receipt_path)
    assert audit["pipeline_name"] == "ons_pay_load" and audit["sources"] == []
    assert audit["failure"]["phase"] == ("ingestion" if fault == "checksum" else "preflight")
    assert list(LocalBlobStore(tmp_path / "objects").list_objects()) == []
    assert opener.open.call_count == int(fault == "checksum")


@pytest.mark.parametrize("outcome", ["success", "failed", "after_failed_refresh"])
def test_raw_batches_cannot_reset_curated_refresh_health(tmp_path: Path, outcome: str) -> None:
    source, manifest = _source(tmp_path)
    result = _run(tmp_path, [source], [Response(manifest["objects"][0])])
    key = VintageKey(1, source.fetch_options.commit, manifest["objects"][0]["sha256"])
    last_batch = result.run_id
    if outcome == "after_failed_refresh":
        with pytest.raises(refresh.RefreshError) as caught:
            _run(tmp_path, [source], [URLError(SECRET)])
        last_batch = _audit(caught.value.receipt_path)["run_id"]
    if outcome != "failed":
        _run(tmp_path, [source], [Response(manifest["objects"][0])], load_only=True)
    else:
        with pytest.raises(refresh.RefreshError):
            _run(tmp_path, [source], [URLError(SECRET)], load_only=True)
    report = monitor_ons_pay(
        data_root=tmp_path / "curated",
        key=key,
        max_refresh_age_hours=1,
        now=datetime.now(UTC) + timedelta(hours=3),
    )
    assert report.status == "unhealthy"
    assert report.latest_batch_run_id == last_batch
    assert report.latest_vintage_success_run_id == result.run_id
    assert report.metrics["last_batch_success"] == int(outcome != "after_failed_refresh")
    assert report.metrics["last_vintage_refresh_age_hours"] >= 3


def test_raw_only_vintage_is_not_a_healthy_curated_vintage(tmp_path: Path) -> None:
    source, manifest = _source(tmp_path)
    _run(tmp_path, [source], [Response(manifest["objects"][0])], load_only=True)
    report = monitor_ons_pay(
        data_root=tmp_path / "curated",
        key=VintageKey(1, source.fetch_options.commit, manifest["objects"][0]["sha256"]),
    )
    assert report.status == "unhealthy" and report.metrics["curation_integrity"] == 0
    assert report.latest_batch_run_id is None and report.latest_vintage_success_run_id is None


def test_raw_job_retains_configured_fetch_retries_and_transfer_statistics(
    tmp_path: Path, capsys: Any
) -> None:
    job, document, manifest = _job(tmp_path)
    document["load_only"] = True
    document["fetch"] = {"max_attempts": 2, "retry_delay_seconds": 0.01}
    _write(job, document)
    opener = Mock()
    opener.open.side_effect = [URLError(TimeoutError(SECRET)), Response(manifest["objects"][0])]
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        result = run_job(job, capsys)
    audit = _audit(Path(result["receipt_path"]))
    ingestion = _audit(Path(audit["sources"][0]["ingestion_receipt_path"]))
    assert ingestion["source_transport"] == "https"
    assert ingestion["fetch_statistics"] == {
        "attempts": 2,
        "retries": 1,
        "downloaded_bytes": len(PAYLOAD),
    }
    assert audit["job_sha256"] == load_refresh_job(job).sha256


@pytest.mark.parametrize("mode", ["direct", "job"])
def test_load_only_dry_run_is_read_only_and_busy_execution_exits_3(
    tmp_path: Path, capsys: Any, mode: str
) -> None:
    job, document, _ = _job(tmp_path)
    document["load_only"] = True
    _write(job, document)
    source, _ = _source(tmp_path)
    args = (
        ["ons-pay-refresh", "--job", str(job)]
        if mode == "job"
        else [*_args(tmp_path, source), "--load-only"]
    )
    with _holding_roots(tmp_path / "objects"):
        before = set(tmp_path.rglob("*"))
        platform_main([*args, "--dry-run"])
        preview = json.loads(capsys.readouterr().out)
        assert preview["load_only"] is True and preview["writes"] is False
        assert set(tmp_path.rglob("*")) == before
        with pytest.raises(SystemExit) as caught:
            platform_main(args)
        assert caught.value.code == 3
    assert "audit" not in capsys.readouterr().err
    assert not (tmp_path / "curated").exists()


def test_load_audit_is_written_before_destination_guards_release(tmp_path: Path) -> None:
    source, manifest = _source(tmp_path)
    original = refresh._write_audit

    def inspect(path: Path, document: Any) -> None:
        assert _probe(tmp_path / "objects").returncode == 3
        assert _probe(tmp_path / "curated").returncode == 3
        assert document["pipeline_name"] == "ons_pay_load"
        original(path, document)

    with patch.object(refresh, "_write_audit", side_effect=inspect):
        _run(tmp_path, [source], [Response(manifest["objects"][0])], load_only=True)
    assert _probe(tmp_path / "objects", tmp_path / "curated").returncode == 0


@pytest.mark.parametrize("value", [None, 0, 1, "false", [], {}])
def test_job_rejects_nonboolean_load_only_without_execution(
    tmp_path: Path, capsys: Any, value: Any
) -> None:
    job, document, _ = _job(tmp_path)
    document["load_only"] = value
    _write(job, document)
    with pytest.raises(SystemExit) as caught:
        platform_main(["ons-pay-refresh", "--job", str(job)])
    assert caught.value.code == 2 and "load_only must be a boolean" in capsys.readouterr().err
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


@pytest.mark.parametrize("value", [None, 0, 1, "false"])
def test_api_rejects_nonboolean_mode_before_guard_or_audit(tmp_path: Path, value: Any) -> None:
    with pytest.raises(refresh.RefreshError, match="load_only must be a boolean"):
        refresh.refresh_ons_pay(
            sources=[],
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
            load_only=value,
        )
    assert not list(tmp_path.rglob("*.lock")) and not (tmp_path / "curated").exists()


@pytest.mark.parametrize("saved", [False, True])
def test_saved_job_mode_cannot_be_overridden(tmp_path: Path, capsys: Any, saved: bool) -> None:
    job, document, _ = _job(tmp_path)
    document["load_only"] = saved
    _write(job, document)
    with pytest.raises(SystemExit) as caught:
        platform_main(["ons-pay-refresh", "--job", str(job), "--load-only"])
    assert caught.value.code == 2 and "cannot be overridden" in capsys.readouterr().err
    assert not (tmp_path / "objects").exists()


def test_cli_loads_checkout_with_analytics_imports_unavailable(tmp_path: Path) -> None:
    source = local_source(tmp_path)
    script = """
import sys
class NoAnalytics:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'duckdb' or fullname.startswith('duckdb.'):
            raise ImportError('Analytics is unavailable')
sys.meta_path.insert(0, NoAnalytics())
from data_platform_lab.cli.main import main
main(sys.argv[1:])
assert 'duckdb' not in sys.modules
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            script,
            "ons-pay-refresh",
            "--load-only",
            "--source",
            str(source.manifest_path),
            str(source.registry_root),
            "--object-root",
            str(tmp_path / "objects"),
            "--output-root",
            str(tmp_path / "curated"),
        ],
        capture_output=True,
        timeout=30,
        **_child_options(),
    )
    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    assert summary["load_only"] is True and summary["objects_published"] == 1
    assert not list(tmp_path.rglob("*.parquet"))
