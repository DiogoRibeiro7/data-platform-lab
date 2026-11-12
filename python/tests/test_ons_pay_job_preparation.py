"""Read-only job preparation, portable execution and exclusive file publication."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_ons_pay_refresh import _source as local_source
from test_ons_pay_refresh_job import _job
from test_ons_pay_refresh_job import _run as run_job
from test_ons_pay_remote_refresh import AS_OF, _args, _source
from test_registry_fetch import SECRET, Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.orchestration import ons_pay_job_preparation as prepare
from data_platform_lab.orchestration import ons_pay_refresh as refresh
from data_platform_lab.orchestration import ons_pay_refresh_cli as cli
from data_platform_lab.orchestration.ons_pay_refresh_job import RefreshJobError, load_refresh_job


def _prepare(tmp_path: Path, sources: list[Any], **kwargs: Any) -> prepare.PreparedRefreshJob:
    return prepare.prepare_refresh_job(
        path=tmp_path / "jobs" / "refresh.json",
        sources=sources,
        object_root=tmp_path / "objects",
        output_root=tmp_path / "curated",
        as_of=AS_OF,
        **kwargs,
    )


def test_preparation_is_deterministic_read_only_and_preserves_mixed_source_order(
    tmp_path: Path,
) -> None:
    remote, _ = _source(tmp_path)
    local = local_source(tmp_path)
    sources = [remote, local]
    before = set(tmp_path.rglob("*"))
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("network opened")),
        patch.object(refresh, "ingest_handoff", side_effect=AssertionError("source loaded")),
        patch.object(cli, "LocalBlobStore", side_effect=AssertionError("store opened")),
    ):
        first = _prepare(tmp_path, sources, max_source_age_days=30, load_only=True)
        second = _prepare(tmp_path, sources, max_source_age_days=30, load_only=True)
    assert first == second and set(tmp_path.rglob("*")) == before
    assert first.sha256 == hashlib.sha256(first.content).hexdigest()
    document = json.loads(first.content)
    assert document["load_only"] is True and document["max_source_age_days"] == 30
    assert document["object_root"] == "../objects" and document["output_root"] == "../curated"
    assert [entry["transport"] for entry in document["sources"]] == ["https", "checkout"]
    for source, entry in zip(sources, document["sources"], strict=True):
        assert entry["manifest_path"] == "../" + source.manifest_path.name
        assert (
            entry["manifest_sha256"]
            == hashlib.sha256(source.manifest_path.read_bytes()).hexdigest()
        )
    assert document["sources"][0]["registry_commit"] == remote.fetch_options.commit
    assert document["sources"][1]["registry_root"] == "../" + local.registry_root.name
    assert document["fetch"] == {
        "timeout_seconds": 30,
        "max_object_bytes": 16777216,
        "max_source_bytes": 67108864,
        "max_attempts": 1,
        "retry_delay_seconds": 1,
    }


@pytest.mark.parametrize("load_only", [False, True])
def test_generated_mixed_job_executes_and_replays_from_another_directory(
    tmp_path: Path, capsys: Any, monkeypatch: Any, load_only: bool
) -> None:
    remote, manifest = _source(tmp_path)
    local = local_source(tmp_path)
    prepared = _prepare(tmp_path, [remote, local], load_only=load_only)
    prepare.save_refresh_job(prepared)
    assert prepared.path.read_bytes() == prepared.content
    job = load_refresh_job(prepared.path)
    assert job.sha256 == prepared.sha256 and job.load_only is load_only
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    opener = Mock()
    opener.open.side_effect = [Response(manifest["objects"][0]), Response(manifest["objects"][0])]
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        first = run_job(prepared.path, capsys)
        replay = run_job(prepared.path, capsys)
    assert first["objects_published"] + first["objects_reused"] == 2
    assert first["vintages_completed"] == replay["objects_reused"] == 2
    assert replay["objects_published"] == 0 and replay["job_sha256"] == prepared.sha256
    assert bool(list((tmp_path / "curated").rglob("*.parquet"))) is not load_only
    assert json.loads(Path(replay["receipt_path"]).read_bytes())["job_sha256"] == prepared.sha256
    assert not list(elsewhere.iterdir())


@pytest.mark.parametrize("load_only", [False, True])
def test_cli_preview_and_save_do_not_execute_or_create_destination_roots(
    tmp_path: Path, capsys: Any, monkeypatch: Any, load_only: bool
) -> None:
    source, _ = _source(tmp_path)
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    args = [
        "ons-pay-refresh",
        "--fetch-source",
        "../handoff-a.json",
        source.fetch_options.commit,
        "--save-job",
        "../jobs/refresh.json",
        "--object-root",
        "../objects",
        "--output-root",
        "../curated",
        "--max-source-age-days",
        "365",
        "--timeout-seconds",
        "7",
        "--max-attempts",
        "3",
        "--retry-delay-seconds",
        "0.5",
        "--max-object-bytes",
        "20000",
        "--max-source-bytes",
        "40000",
        *(["--load-only"] if load_only else []),
    ]
    before = set(tmp_path.rglob("*"))
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("network opened")),
        patch.object(cli, "refresh_ons_pay", side_effect=AssertionError("job executed")),
        patch.object(cli, "LocalBlobStore", side_effect=AssertionError("store opened")),
    ):
        platform_main([*args, "--dry-run"])
        preview = json.loads(capsys.readouterr().out)
        assert preview["status"] == "planned" and preview["writes"] is False
        assert set(tmp_path.rglob("*")) == before
        platform_main(args)
        saved = json.loads(capsys.readouterr().out)
    assert saved["status"] == "saved" and saved["writes"] is True
    assert saved["job_sha256"] == preview["job_sha256"]
    assert saved["job"] == preview["job"] and saved["job"]["load_only"] is load_only
    job = load_refresh_job(Path(saved["job_path"]))
    assert job.object_root == tmp_path / "objects" and job.output_root == tmp_path / "curated"
    options = job.sources[0].fetch_options
    assert (options.timeout_seconds, options.max_attempts, options.retry_delay_seconds) == (
        7,
        3,
        0.5,
    )
    assert (options.max_object_bytes, options.max_total_bytes) == (20000, 40000)
    assert not job.object_root.exists() and not job.output_root.exists()
    assert not list(tmp_path.rglob("*.lock")) and not list(tmp_path.rglob(".ons-job-*.tmp"))


def test_checkout_preparation_needs_only_handoff_not_source_bytes(tmp_path: Path) -> None:
    local = local_source(tmp_path)
    selected = replace(local, registry_root=tmp_path / "checkout-to-provision")
    prepared = _prepare(tmp_path, [selected])
    assert "fetch" not in json.loads(prepared.content)
    prepare.save_refresh_job(prepared)
    assert not selected.registry_root.exists()


@pytest.mark.parametrize("kind", ["file", "directory", "hardlink", "symlink", "dangling"])
def test_existing_job_path_is_never_overwritten(tmp_path: Path, kind: str) -> None:
    source, _ = _source(tmp_path)
    path = tmp_path / "jobs" / "refresh.json"
    path.parent.mkdir()
    original = tmp_path / "original.json"
    original.write_bytes(b"original content")
    if kind == "file":
        path.write_bytes(b"existing job")
    elif kind == "directory":
        path.mkdir()
    elif kind == "hardlink":
        path.hardlink_to(original)
    else:
        try:
            path.symlink_to(original if kind == "symlink" else tmp_path / "missing.json")
        except OSError:
            pytest.skip("Creating symlinks requires platform privileges")
    before = original.read_bytes()
    with pytest.raises(RefreshJobError, match="already exists"):
        _prepare(tmp_path, [source])
    assert original.read_bytes() == before
    if kind == "file":
        assert path.read_bytes() == b"existing job"
    assert not list(tmp_path.rglob(".ons-job-*.tmp"))


def test_creation_race_keeps_the_winner_and_removes_temporary_file(tmp_path: Path) -> None:
    source, _ = _source(tmp_path)
    prepared = _prepare(tmp_path, [source])
    original = prepare.os.link

    def competing_writer(source_path: Path, target: Path) -> None:
        target.write_bytes(b"the other writer won")
        original(source_path, target)

    with (
        patch.object(prepare.os, "link", side_effect=competing_writer),
        pytest.raises(prepare.RefreshJobWriteError, match="already exists"),
    ):
        prepare.save_refresh_job(prepared)
    assert prepared.path.read_bytes() == b"the other writer won"
    assert not list(prepared.path.parent.glob(".ons-job-*.tmp"))


@pytest.mark.parametrize("stage", ["fsync", "link"])
def test_publication_failure_cleans_up_without_partial_job_or_sensitive_details(
    tmp_path: Path, capsys: Any, stage: str
) -> None:
    source, _ = _source(tmp_path)
    path = tmp_path / "jobs" / "refresh.json"
    with (
        patch.object(prepare.os, stage, side_effect=OSError(SECRET)),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*_args(tmp_path, source), "--save-job", str(path)])
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert not captured.out and captured.err == "Cannot save the prepared refresh job.\n"
    assert not path.exists() and not list(path.parent.glob(".ons-job-*.tmp"))
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


@pytest.mark.parametrize("destination", ["job", "objects", "output"])
def test_canonical_checkout_cannot_receive_job_or_destination_artifacts(
    tmp_path: Path, destination: str
) -> None:
    source = local_source(tmp_path)
    before = set(source.registry_root.rglob("*"))
    with pytest.raises(RefreshJobError, match="outside canonical checkouts"):
        prepare.prepare_refresh_job(
            path=(source.registry_root if destination == "job" else tmp_path) / "jobs/job.json",
            sources=[source],
            object_root=(source.registry_root if destination == "objects" else tmp_path)
            / "objects",
            output_root=(source.registry_root if destination == "output" else tmp_path) / "curated",
        )
    assert set(source.registry_root.rglob("*")) == before
    assert not (tmp_path / "jobs").exists()


@pytest.mark.parametrize("fault", ["commit", "digest", "duplicate", "stale", "future", "malformed"])
def test_invalid_source_batch_cannot_generate_a_job(tmp_path: Path, fault: str) -> None:
    source, manifest = _source(tmp_path)
    sources = [source]
    policy: dict[str, Any] = {}
    if fault == "commit":
        sources = [replace(source, fetch_options=replace(source.fetch_options, commit="b" * 40))]
    elif fault == "digest":
        sources = [replace(source, expected_manifest_sha256="0" * 64)]
    elif fault == "duplicate":
        sources *= 2
    elif fault == "stale":
        policy["max_source_age_days"] = 0
    elif fault == "future":
        manifest["source"]["retrieved_at"] = "2099-01-01"
        source.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    else:
        source.manifest_path.write_text(SECRET, encoding="utf-8")
    before = set(tmp_path.rglob("*"))
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(RefreshJobError) as error,
    ):
        _prepare(tmp_path, sources, **policy)
    assert SECRET not in str(error.value) and set(tmp_path.rglob("*")) == before


def test_preparation_retains_preflight_digest_when_manifest_changes(tmp_path: Path) -> None:
    source, _ = _source(tmp_path)
    digest = hashlib.sha256(source.manifest_path.read_bytes()).hexdigest()
    original = prepare.plan_ons_pay_refresh

    def changed(*args: Any, **kwargs: Any) -> Any:
        plan = original(*args, **kwargs)
        source.manifest_path.write_bytes(source.manifest_path.read_bytes() + b"\n")
        return plan

    with patch.object(prepare, "plan_ons_pay_refresh", side_effect=changed):
        prepared = _prepare(tmp_path, [source])
    assert json.loads(prepared.content)["sources"][0]["manifest_sha256"] == digest
    prepare.save_refresh_job(prepared)
    job = load_refresh_job(prepared.path)
    with pytest.raises(refresh.RefreshError, match="manifest"):
        refresh.plan_ons_pay_refresh(job.sources)


def test_shared_fetch_policy_must_not_silently_drop_per_source_settings(tmp_path: Path) -> None:
    first, _ = _source(tmp_path)
    second, _ = _source(tmp_path, "b")
    second = replace(second, fetch_options=replace(second.fetch_options, max_attempts=3))
    with pytest.raises(RefreshJobError, match="shared HTTPS fetch settings"):
        _prepare(tmp_path, [first, second])
    assert not (tmp_path / "jobs").exists()


@pytest.mark.parametrize("count", [0, 257])
def test_job_source_limit_is_checked_before_handoffs_are_opened(tmp_path: Path, count: int) -> None:
    source, _ = _source(tmp_path)
    with (
        patch.object(prepare, "plan_ons_pay_refresh", side_effect=AssertionError),
        pytest.raises(RefreshJobError, match="between 1 and 256"),
    ):
        _prepare(tmp_path, [source] * count)


@pytest.mark.parametrize("value", [None, 0, "true"])
def test_preparation_rejects_invalid_mode_without_writes(tmp_path: Path, value: Any) -> None:
    source, _ = _source(tmp_path)
    with pytest.raises(RefreshJobError, match="boolean"):
        _prepare(tmp_path, [source], load_only=value)
    assert not (tmp_path / "jobs").exists()


def test_saved_job_cannot_be_combined_with_save_job(tmp_path: Path, capsys: Any) -> None:
    job, _, _ = _job(tmp_path)
    path = tmp_path / "copy.json"
    with pytest.raises(SystemExit) as error:
        platform_main(["ons-pay-refresh", "--job", str(job), "--save-job", str(path)])
    assert error.value.code == 2 and "cannot be overridden" in capsys.readouterr().err
    assert not path.exists()


def test_non_ascii_paths_round_trip_and_cross_drive_paths_stay_absolute(tmp_path: Path) -> None:
    source, _ = _source(tmp_path)
    unicode_path = tmp_path / "handoff-á.json"
    unicode_path.write_bytes(source.manifest_path.read_bytes())
    source = replace(source, manifest_path=unicode_path)
    prepared = _prepare(tmp_path, [source])
    assert "handoff-á.json" in prepared.content.decode("utf-8")
    prepare.save_refresh_job(prepared)
    assert load_refresh_job(prepared.path).sources[0].manifest_path == unicode_path
    with patch.object(prepare.os.path, "relpath", side_effect=ValueError("different drives")):
        assert prepare._relative_path(unicode_path, tmp_path) == unicode_path.as_posix()


def test_modified_prepared_bytes_are_rejected_before_directory_creation(tmp_path: Path) -> None:
    source, _ = _source(tmp_path)
    prepared = _prepare(tmp_path, [source])
    with pytest.raises(RefreshJobError, match="inconsistent"):
        prepare.save_refresh_job(replace(prepared, content=prepared.content + b"\n"))
    assert not prepared.path.parent.exists()
