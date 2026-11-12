"""Read-only saved-job outcomes, failure recovery and bounded audit inspection."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import URLError

import pytest
from test_local_run_guard import _child_options, _holding_roots
from test_ons_pay_refresh import _source as local_source
from test_ons_pay_refresh_job import _entry, _job, _write
from test_ons_pay_remote_refresh import _source as remote_source
from test_registry_fetch import SECRET, Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability import ons_pay_job_status as status
from data_platform_lab.orchestration import ons_pay_refresh as refresh
from data_platform_lab.orchestration.ons_pay_refresh_job import load_refresh_job
from data_platform_lab.storage import LocalBlobStore


def _execute(path: Path, responses: list[Any]) -> Any:
    job = load_refresh_job(path)
    opener = Mock()
    opener.open.side_effect = responses
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        return refresh.refresh_ons_pay(
            sources=job.sources,
            store=LocalBlobStore(job.object_root),
            output_root=job.output_root,
            max_source_age_days=job.max_source_age_days,
            job_sha256=job.sha256,
            load_only=job.load_only,
        )


def _completed(tmp_path: Path, *, load_only: bool = True) -> tuple[Path, Any, Any]:
    path, document, manifest = _job(tmp_path)
    document["load_only"] = load_only
    _write(path, document)
    result = _execute(path, [Response(manifest["objects"][0])])
    return path, result, manifest


def _inspect(path: Path, **kwargs: Any) -> status.JobStatusReport:
    return status.inspect_ons_pay_job(job_path=path, **kwargs)


def _cli(path: Path, capsys: Any, code: int = 0, *extra: str) -> Any:
    args = ["ons-pay-job-status", "--job", str(path), *extra]
    if code:
        with pytest.raises(SystemExit) as caught:
            platform_main(args)
        assert caught.value.code == code
    else:
        platform_main(args)
    captured = capsys.readouterr()
    assert not captured.err
    return json.loads(captured.out)


def _retime(path: Path, completed: datetime) -> None:
    audit = json.loads(path.read_bytes())
    audit.update(
        created_at=completed.isoformat(),
        started_at=(completed - timedelta(seconds=2)).isoformat(),
        duration_seconds=2.0,
    )
    _write(path, audit)


@pytest.mark.parametrize("load_only", [False, True])
def test_completed_mixed_job_reports_success_without_source_or_data_reads(
    tmp_path: Path, capsys: Any, monkeypatch: Any, load_only: bool
) -> None:
    path, document, manifest = _job(tmp_path)
    document["sources"].append(_entry(local_source(tmp_path)))
    document["load_only"] = load_only
    _write(path, document)
    result = _execute(path, [Response(manifest["objects"][0])])
    # Completed status remains available after source handoffs are removed.
    for source in load_refresh_job(path).sources:
        source.manifest_path.unlink()
    # This is audit evidence, not a fresh verification of the published objects.
    for raw in (tmp_path / "objects").rglob("*.csv"):
        raw.write_bytes(b"corrupt")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob("*") if p.is_file()}
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(refresh, "plan_ons_pay_refresh", side_effect=AssertionError),
        patch.object(LocalBlobStore, "__init__", side_effect=AssertionError),
    ):
        report = _cli(path, capsys)
    assert report["status"] == "success" and report["matching_runs"] == 1
    assert report["job_sha256"] == load_refresh_job(path).sha256
    assert report["pipeline_name"] == ("ons_pay_load" if load_only else "ons_pay_refresh")
    assert report["latest_run"] == report["latest_success"]
    assert report["latest_run"]["run_id"] == result.run_id
    assert report["latest_run"]["completed_sources"] == report["latest_run"]["total_sources"] == 2
    assert report["latest_run"]["failure_phase"] is None
    assert report["last_success_age_hours"] >= 0
    assert {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.rglob("*") if p.is_file()
    } == before
    assert not list(elsewhere.iterdir())


@pytest.mark.parametrize("load_only", [False, True])
def test_never_run_creates_no_destinations_or_guards(
    tmp_path: Path, capsys: Any, load_only: bool
) -> None:
    path, document, _ = _job(tmp_path)
    document["load_only"] = load_only
    _write(path, document)
    before = set(tmp_path.rglob("*"))
    report = _cli(path, capsys, 1)
    assert report["status"] == "never_run" and report["matching_runs"] == 0
    assert (
        report["latest_run"] is report["latest_success"] is report["last_success_age_hours"] is None
    )
    assert set(tmp_path.rglob("*")) == before


@pytest.mark.parametrize("load_only", [False, True])
def test_partial_failure_takes_precedence_and_successful_replay_recovers(
    tmp_path: Path, capsys: Any, load_only: bool
) -> None:
    path, document, first = _job(tmp_path)
    second, manifest = remote_source(tmp_path, "b")
    document["sources"].append(_entry(second))
    document["load_only"] = load_only
    _write(path, document)
    success = _execute(path, [Response(first["objects"][0]), Response(manifest["objects"][0])])
    with pytest.raises(refresh.RefreshError) as caught:
        _execute(path, [Response(first["objects"][0]), URLError(SECRET)])
    failed = json.loads(caught.value.receipt_path.read_bytes())
    failed["failure"]["message"] = failed["failure"]["error_type"] = SECRET
    _write(caught.value.receipt_path, failed)
    report = _cli(path, capsys, 1, "--max-success-age-hours", "0")
    assert report["status"] == "failed" and report["matching_runs"] == 2
    assert report["latest_success"]["run_id"] == success.run_id
    assert report["latest_run"]["failure_phase"] == "ingestion"
    assert (
        report["latest_run"]["failure_source_index"]
        == report["latest_run"]["completed_sources"]
        == 1
    )
    assert report["latest_run"]["total_sources"] == 2 and SECRET not in json.dumps(report)
    recovery = _execute(path, [Response(first["objects"][0]), Response(manifest["objects"][0])])
    assert _cli(path, capsys)["latest_success"]["run_id"] == recovery.run_id


@pytest.mark.parametrize("phase", ["preflight", "curation"])
def test_failure_without_any_success_is_reported(tmp_path: Path, capsys: Any, phase: str) -> None:
    path, _, manifest = _job(tmp_path)
    if phase == "preflight":
        load_refresh_job(path).sources[0].manifest_path.unlink()
    with (
        patch.object(refresh, "curate_ons_pay", side_effect=ValueError(SECRET)),
        pytest.raises(refresh.RefreshError),
    ):
        _execute(path, [Response(manifest["objects"][0])])
    report = _cli(path, capsys, 1)
    assert report["status"] == "failed" and report["latest_success"] is None
    assert report["last_success_age_hours"] is None
    assert report["latest_run"]["failure_phase"] == phase
    assert report["latest_run"]["completed_sources"] == 0


def test_success_age_boundary_timezone_normalization_and_stale_exit(
    tmp_path: Path, capsys: Any
) -> None:
    path, result, _ = _completed(tmp_path)
    finished = datetime.fromisoformat("2000-01-02T10:00:00+02:00")
    _retime(result.receipt_path, finished)
    now = finished + timedelta(hours=24)
    report = _inspect(path, max_success_age_hours=24, now=now)
    assert report.status == "success" and report.last_success_age_hours == 24
    assert report.latest_run.completed_at == "2000-01-02T08:00:00+00:00"
    assert _inspect(path, max_success_age_hours=23.99, now=now).status == "stale"
    assert _cli(path, capsys, 1, "--max-success-age-hours", "24")["status"] == "stale"
    assert _inspect(path, now=now).status == "success"


def test_exact_job_bytes_ignore_other_jobs_direct_runs_and_other_pipeline(tmp_path: Path) -> None:
    path, result, _ = _completed(tmp_path)
    saved = path.read_bytes()
    path.write_bytes(saved + b"\n")
    assert _inspect(path).status == "never_run"
    path.write_bytes(saved)
    audit = json.loads(result.receipt_path.read_bytes())
    audit.pop("job_sha256")
    _write(result.receipt_path, audit)
    assert _inspect(path).status == "never_run"
    # The other pipeline and unfinished temporary files are outside this job's history.
    (result.receipt_path.parent / "ons_pay_refresh_broken.json").write_text("broken")
    (result.receipt_path.parent / ".ons_pay_refresh_unfinished").write_text("broken")
    assert _inspect(path).status == "never_run"


def test_completion_time_controls_order_not_filename_or_mtime(tmp_path: Path) -> None:
    path, first, manifest = _completed(tmp_path)
    second = _execute(path, [Response(manifest["objects"][0])])
    finished = datetime(2000, 1, 2, tzinfo=UTC)
    _retime(first.receipt_path, finished)
    _retime(second.receipt_path, finished - timedelta(hours=2))
    os.utime(first.receipt_path, (1, 1))
    assert _inspect(path).latest_run.run_id == first.run_id


def test_a_different_jobs_success_does_not_mask_failure(tmp_path: Path) -> None:
    path, _, manifest = _completed(tmp_path)
    with pytest.raises(refresh.RefreshError):
        _execute(path, [URLError(SECRET)])
    saved = path.read_bytes()
    path.write_bytes(saved + b"\n")
    _execute(path, [Response(manifest["objects"][0])])
    path.write_bytes(saved)
    report = _inspect(path)
    assert report.status == "failed" and report.matching_runs == 2


def test_a_run_completing_during_inspection_is_not_a_future_audit(tmp_path: Path) -> None:
    path, _, manifest = _completed(tmp_path)
    original = status._matching_audits

    def finish_then_scan(job: Any, pipeline: str) -> Any:
        _execute(path, [Response(manifest["objects"][0])])
        return original(job, pipeline)

    with patch.object(status, "_matching_audits", side_effect=finish_then_scan):
        report = _inspect(path)
    assert report.status == "success" and report.matching_runs == 2
    assert report.last_success_age_hours >= 0
    assert report.checked_at_utc >= report.latest_run.completed_at


@pytest.mark.parametrize("fault", ["model", "row_counts", "duplicate", "older"])
def test_inconsistent_full_refresh_history_is_rejected(tmp_path: Path, fault: str) -> None:
    path, result, manifest = _completed(tmp_path, load_only=False)
    audit = json.loads(result.receipt_path.read_bytes())
    if fault == "model":
        audit["model_version"] = 999
    elif fault == "row_counts":
        audit["sources"][0]["rows_without_pay"] += 1
    elif fault == "duplicate":
        document = json.loads(path.read_bytes())
        document["sources"] *= 2
        _write(path, document)
        audit["job_sha256"] = load_refresh_job(path).sha256
        audit["sources"] *= 2
    else:
        _execute(path, [Response(manifest["objects"][0])])
        audit["metrics"]["vintages_completed"] = -1
    _write(result.receipt_path, audit)
    with pytest.raises(status.JobStatusError):
        _inspect(path)


@pytest.mark.parametrize("fault", ["listing", "read", "directory", "file"])
def test_unreadable_evidence_fails_without_reporting_never_run(tmp_path: Path, fault: str) -> None:
    path, result, _ = _completed(tmp_path)
    if fault == "listing":
        with (
            patch.object(status.os, "scandir", side_effect=PermissionError(SECRET)),
            pytest.raises(status.JobStatusError) as caught,
        ):
            _inspect(path)
    elif fault == "read":
        original = Path.open

        def deny_audit(selected: Path, *args: Any, **kwargs: Any) -> Any:
            if selected == result.receipt_path:
                raise PermissionError(SECRET)
            return original(selected, *args, **kwargs)

        with patch.object(Path, "open", deny_audit), pytest.raises(status.JobStatusError) as caught:
            _inspect(path)
    else:
        original_path = result.receipt_path.parent if fault == "directory" else result.receipt_path
        original_path.rename(original_path.with_name("moved"))
        if fault == "directory":
            original_path.write_text(SECRET)
        else:
            original_path.mkdir()
        with pytest.raises(status.JobStatusError) as caught:
            _inspect(path)
    assert SECRET not in str(caught.value)


@pytest.mark.parametrize("limit", [-1, float("nan"), float("inf"), True, "24"])
def test_invalid_age_limits_are_rejected(tmp_path: Path, limit: Any) -> None:
    path, _, _ = _job(tmp_path)
    with pytest.raises(status.JobStatusError):
        _inspect(path, max_success_age_hours=limit)


def test_naive_reference_time_is_rejected(tmp_path: Path) -> None:
    path, _, _ = _job(tmp_path)
    with pytest.raises(status.JobStatusError, match="timezone-aware"):
        _inspect(path, now=datetime(2000, 1, 1))


@pytest.mark.parametrize(
    "fault",
    [
        "schema",
        "pipeline",
        "model",
        "identity",
        "digest",
        "null_digest",
        "timestamp",
        "future",
        "negative_duration",
        "duration_mismatch",
        "outcome",
        "partial_success",
        "negative_count",
        "boolean_count",
        "aggregate",
        "source_commit",
        "source_sha",
        "policy",
        "missing_policy",
        "reference_date",
        "failure",
        "curated_metrics",
    ],
)
def test_malformed_matching_audits_fail_closed_without_leaking_content(
    tmp_path: Path, capsys: Any, fault: str
) -> None:
    path, result, _ = _completed(tmp_path)
    audit = json.loads(result.receipt_path.read_bytes())
    if fault == "schema":
        audit["schema_version"] = True
    elif fault == "pipeline":
        audit["pipeline_name"] = "ons_pay_refresh"
    elif fault == "model":
        audit["model_version"] = 1
    elif fault == "identity":
        audit["run_id"] = "0" * 32
    elif fault == "digest":
        audit["job_sha256"] = SECRET
    elif fault == "null_digest":
        audit["job_sha256"] = None
    elif fault == "timestamp":
        audit["started_at"] = "2000-01-01T00:00:00"
    elif fault == "future":
        audit["created_at"] = "9998-01-01T00:00:00+00:00"
    elif fault == "negative_duration":
        audit["duration_seconds"] = -1
    elif fault == "duration_mismatch":
        audit["duration_seconds"] += 10
    elif fault == "outcome":
        audit["status"] = "running"
    elif fault == "partial_success":
        audit["sources"] = []
        audit["metrics"] = dict.fromkeys(audit["metrics"], 0)
    elif fault in {"negative_count", "boolean_count"}:
        audit["sources"][0]["objects_published"] = -1 if fault == "negative_count" else True
    elif fault == "aggregate":
        audit["metrics"]["bytes_verified"] += 1
    elif fault == "source_commit":
        audit["sources"][0]["registry_commit"] = "b" * 40
    elif fault == "source_sha":
        audit["sources"][0]["source_sha256"] = SECRET
    elif fault == "policy":
        audit["policy"]["max_source_age_days"] = 1
    elif fault == "missing_policy":
        audit["policy"].pop("max_source_age_days")
    elif fault == "reference_date":
        audit["policy"]["as_of_utc_date"] = SECRET
    elif fault == "failure":
        audit["failure"] = {"message": SECRET}
    else:
        audit["metrics"]["source_rows"] = 0
    _write(result.receipt_path, audit)
    with pytest.raises(SystemExit) as caught:
        platform_main(["ons-pay-job-status", "--job", str(path)])
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert not captured.out and captured.err and SECRET not in captured.err


@pytest.mark.parametrize("fault", ["phase", "index", "completed_preflight"])
def test_inconsistent_failure_progress_is_rejected(tmp_path: Path, fault: str) -> None:
    path, result, _ = _completed(tmp_path)
    audit = json.loads(result.receipt_path.read_bytes())
    audit["status"] = "failed"
    audit["failure"] = {"phase": "ingestion", "source_index": 0}
    if fault == "phase":
        audit["failure"]["phase"] = "curation"
    elif fault == "index":
        audit["failure"]["source_index"] = 1
    else:
        audit["failure"]["phase"] = "preflight"
    _write(result.receipt_path, audit)
    with pytest.raises(status.JobStatusError):
        _inspect(path)


@pytest.mark.parametrize(
    "raw", [b"{", b"[]", b'{"job_sha256":null,"job_sha256":null}', b'{"x":NaN}']
)
def test_ambiguous_or_unreadable_history_cannot_be_ignored(tmp_path: Path, raw: bytes) -> None:
    path, result, _ = _completed(tmp_path)
    (result.receipt_path.parent / "ons_pay_load_broken.json").write_bytes(raw)
    with pytest.raises(status.JobStatusError):
        _inspect(path)


@pytest.mark.parametrize("limit", ["_MAX_FILES", "_MAX_AUDIT_BYTES", "_MAX_HISTORY_BYTES"])
def test_bounded_history_refuses_partial_results(
    tmp_path: Path, monkeypatch: Any, limit: str
) -> None:
    path, _, manifest = _completed(tmp_path)
    _execute(path, [Response(manifest["objects"][0])])
    monkeypatch.setattr(status, limit, 1)
    with pytest.raises(status.JobStatusError, match="limit"):
        _inspect(path)


@pytest.mark.parametrize("target", ["directory", "file"])
def test_symlinked_evidence_is_rejected(tmp_path: Path, target: str) -> None:
    path, result, _ = _completed(tmp_path)
    original = result.receipt_path.parent if target == "directory" else result.receipt_path
    moved = original.with_name("moved")
    original.rename(moved)
    try:
        original.symlink_to(moved, target_is_directory=target == "directory")
    except OSError:
        pytest.skip("Creating symlinks requires local privileges")
    with pytest.raises(status.JobStatusError, match="Unsafe"):
        _inspect(path)


def test_status_works_while_destination_is_busy_and_without_analytics(tmp_path: Path) -> None:
    path, _, _ = _completed(tmp_path)
    code = """
import importlib.abc, json, sys
class BlockAnalytics(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', 'pyarrow'}:
            raise AssertionError('analytics imported')
sys.meta_path.insert(0, BlockAnalytics())
from data_platform_lab.cli.main import main
main(['ons-pay-job-status', '--job', sys.argv[1]])
"""
    with _holding_roots(tmp_path / "objects", tmp_path / "curated"):
        result = subprocess.run(
            [sys.executable, "-c", code, str(path)], capture_output=True, **_child_options()
        )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "success"


@pytest.mark.parametrize(
    "args", [[], ["--job", SECRET], ["--job", "missing", "--max-success-age-hours", "nan"]]
)
def test_invalid_cli_and_configuration_have_safe_diagnostics(capsys: Any, args: list[str]) -> None:
    with pytest.raises(SystemExit) as caught:
        platform_main(["ons-pay-job-status", *args])
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert not captured.out and SECRET not in captured.err
