"""Verified store reuse, bounded fallback and saved ONS ingestion policies."""

from __future__ import annotations

import json
from dataclasses import replace
from http.client import IncompleteRead
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_ons_pay_job_preparation import _prepare
from test_ons_pay_refresh_job import _job
from test_ons_pay_refresh_job import _run as run_job
from test_ons_pay_remote_refresh import AS_OF, _source
from test_ons_pay_remote_refresh import _args as refresh_args
from test_registry_fetch import OPTIONS, PAYLOAD, SECRET, Response, _args, _manifest, _write
from test_registry_fetch_retries import _http

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_cli, registry_fetch
from data_platform_lab.ingestion.registry_handoff import HandoffError
from data_platform_lab.observability.ons_pay_monitor import monitor_ons_pay
from data_platform_lab.orchestration import ons_pay_refresh_cli as refresh_cli
from data_platform_lab.orchestration.ons_pay_job_preparation import save_refresh_job
from data_platform_lab.orchestration.ons_pay_refresh import RefreshError, refresh_ons_pay
from data_platform_lab.orchestration.ons_pay_refresh_job import RefreshJobError, load_refresh_job
from data_platform_lab.orchestration.ons_pay_transform import transform_ons_pay
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.storage import LocalBlobStore


def _ingest(tmp_path: Path, manifest: Any, store: Any, **options: Any) -> Any:
    return registry_fetch.ingest_registry_fetch(
        manifest_path=_write(tmp_path, manifest),
        options=replace(OPTIONS, reuse_verified=True, **options),
        store=store,
        receipt_dir=tmp_path / "receipts",
    )


def _receipt(result: Any) -> Any:
    return json.loads(result.receipt_path.read_bytes())


def _pair() -> Any:
    manifest = _manifest()
    first = manifest["objects"][0]
    manifest["objects"].append(
        first
        | {
            "source_path": first["source_path"] + "2",
            "source_uri": first["source_uri"] + "2",
            "storage_key": first["storage_key"] + "2",
        }
    )
    return manifest


def test_missing_object_downloads_once_then_verified_replay_never_opens_http(
    tmp_path: Path,
) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    store = LocalBlobStore(tmp_path / "objects")
    opener = Mock()
    opener.open.return_value = Response(item)
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        first = _ingest(tmp_path, manifest, store)
    assert first.files_published == 1
    assert _receipt(first)["objects"][0]["source_transport"] == "https"
    assert _receipt(first)["fetch_statistics"] == {
        "attempts": 1,
        "retries": 0,
        "downloaded_bytes": len(PAYLOAD),
        "cached_objects": 0,
        "cached_bytes": 0,
    }
    raw = store.root / item["storage_key"]
    before = raw.stat().st_mtime_ns
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("HTTP opened")),
        patch.object(registry_fetch, "sleep", side_effect=AssertionError("retry waited")),
        patch.object(store, "put_bytes", side_effect=AssertionError("raw object rewritten")),
    ):
        replay = _ingest(tmp_path, manifest, store)
    receipt = _receipt(replay)
    assert replay.files_reused == 1 and replay.files_published == 0
    assert replay.bytes_verified == len(PAYLOAD)
    assert raw.read_bytes() == PAYLOAD and raw.stat().st_mtime_ns == before
    assert receipt["source_transport"] == "https_or_verified_store"
    assert receipt["objects"][0]["source_transport"] == "verified_store"
    assert receipt["source"] == manifest["source"]
    assert receipt["objects"][0]["source_uri"] == item["source_uri"]
    assert receipt["fetch_statistics"] == {
        "attempts": 0,
        "retries": 0,
        "downloaded_bytes": 0,
        "cached_objects": 1,
        "cached_bytes": len(PAYLOAD),
    }


def test_default_still_downloads_existing_objects_and_keeps_receipt_shape(tmp_path: Path) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    store = LocalBlobStore(tmp_path / "objects")
    store.put_bytes(item["storage_key"], PAYLOAD)
    opener = Mock()
    opener.open.return_value = Response(item)
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        result = registry_fetch.ingest_registry_fetch(
            manifest_path=_write(tmp_path, manifest),
            options=OPTIONS,
            store=store,
            receipt_dir=tmp_path / "receipts",
        )
    opener.open.assert_called_once()
    receipt = _receipt(result)
    assert result.files_reused == 1 and receipt["source_transport"] == "https"
    assert "source_transport" not in receipt["objects"][0]
    assert receipt["fetch_statistics"] == {
        "attempts": 1,
        "retries": 0,
        "downloaded_bytes": len(PAYLOAD),
    }


@pytest.mark.parametrize("cached_index", [0, 1])
def test_mixed_sources_fetch_only_missing_bytes_with_shared_budget(
    tmp_path: Path,
    cached_index: int,
) -> None:
    manifest = _pair()
    objects = manifest["objects"]
    store = LocalBlobStore(tmp_path / "objects")
    store.put_bytes(objects[cached_index]["storage_key"], PAYLOAD)
    opener = Mock()
    opener.open.return_value = Response(objects[1 - cached_index])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        result = _ingest(tmp_path, manifest, store, max_total_bytes=2 * len(PAYLOAD))
    assert result.files_published == result.files_reused == 1
    assert result.bytes_verified == 2 * len(PAYLOAD)
    opener.open.assert_called_once()
    assert opener.open.call_args.args[0].full_url == objects[1 - cached_index]["source_uri"]
    receipt = _receipt(result)
    assert receipt["objects"][cached_index]["source_transport"] == "verified_store"
    assert receipt["objects"][1 - cached_index]["source_transport"] == "https"
    assert receipt["fetch_statistics"]["cached_bytes"] == len(PAYLOAD)
    assert receipt["fetch_statistics"]["downloaded_bytes"] == len(PAYLOAD)


@pytest.mark.parametrize("failure", ["checksum", "size", "unreadable", "disappeared", "changed"])
def test_bad_cached_object_fails_without_download_repair_or_success_receipt(
    tmp_path: Path,
    failure: str,
) -> None:
    manifest = _manifest()
    store = Mock()
    store.exists.return_value = True
    if failure == "checksum":
        store.get_bytes.return_value = b"x" * len(PAYLOAD)
    elif failure == "size":
        manifest["objects"][0]["size_bytes"] = len(PAYLOAD) + 1
        store.get_bytes.return_value = PAYLOAD
    elif failure == "changed":
        store.get_bytes.side_effect = [PAYLOAD, b"corrupt"]
    else:
        store.get_bytes.side_effect = (
            PermissionError(SECRET) if failure == "unreadable" else FileNotFoundError(SECRET)
        )
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("HTTP opened")),
        patch.object(registry_fetch, "sleep", side_effect=AssertionError("retry waited")),
        pytest.raises((HandoffError, OSError)),
    ):
        _ingest(tmp_path, manifest, store, max_attempts=3)
    store.put_bytes.assert_not_called()
    assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize("limit", ["max_object_bytes", "max_total_bytes"])
def test_unknown_cached_size_still_respects_byte_limits(tmp_path: Path, limit: str) -> None:
    manifest = _manifest()
    manifest["objects"][0]["size_bytes"] = None
    store = Mock()
    store.exists.return_value = True
    store.get_bytes.return_value = PAYLOAD
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(HandoffError, match="byte limits"),
    ):
        _ingest(tmp_path, manifest, store, **{limit: len(PAYLOAD) - 1})
    store.put_bytes.assert_not_called()


@pytest.mark.parametrize("cached_index", [0, 1])
def test_cache_and_partial_retry_bytes_share_the_same_budget(
    tmp_path: Path, cached_index: int
) -> None:
    manifest = _pair()
    for item in manifest["objects"]:
        item["size_bytes"] = None
    store = LocalBlobStore(tmp_path / "objects")
    store.put_bytes(manifest["objects"][cached_index]["storage_key"], PAYLOAD)
    item = manifest["objects"][1 - cached_index]
    partial = Response(item)
    partial.read = Mock(side_effect=IncompleteRead(PAYLOAD[:3], 10))
    opener = Mock()
    opener.open.side_effect = [partial, Response(item)]
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        patch.object(registry_fetch, "sleep") as pause,
        pytest.raises(HandoffError, match="byte limits"),
    ):
        _ingest(tmp_path, manifest, store, max_attempts=2, max_total_bytes=2 * len(PAYLOAD) + 2)
    assert opener.open.call_count == 2
    pause.assert_called_once_with(1)
    assert not (tmp_path / "receipts").exists()


def test_zero_byte_cached_object_is_verified(tmp_path: Path) -> None:
    manifest = _manifest(b"")
    store = Mock()
    store.exists.return_value = True
    store.get_bytes.return_value = b""
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
        result = _ingest(tmp_path, manifest, store)
    assert result.files_reused == 1 and result.bytes_verified == 0
    assert _receipt(result)["fetch_statistics"]["cached_objects"] == 1
    store.put_bytes.assert_not_called()


@pytest.mark.parametrize("invalid", [None, 0, 1, "true", [], {}])
def test_reuse_policy_requires_boolean_before_storage_or_http(tmp_path: Path, invalid: Any) -> None:
    path = _write(tmp_path, _manifest())
    store = Mock()
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(HandoffError, match="boolean"),
    ):
        registry_fetch.ingest_registry_fetch(
            manifest_path=path,
            options=replace(OPTIONS, reuse_verified=invalid),
            store=store,
            receipt_dir=tmp_path / "receipts",
        )
    assert not store.mock_calls


def test_manifest_digest_is_checked_before_any_cache_lookup(tmp_path: Path) -> None:
    store = Mock()
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(HandoffError, match="planned manifest SHA-256"),
    ):
        registry_fetch.ingest_registry_fetch(
            manifest_path=_write(tmp_path, _manifest()),
            options=replace(OPTIONS, reuse_verified=True),
            store=store,
            receipt_dir=tmp_path / "receipts",
            expected_manifest_sha256="0" * 64,
        )
    assert not store.mock_calls


@pytest.mark.parametrize("command", ["registry", "refresh"])
def test_cli_preview_reports_reuse_policy_without_reading_cache(
    tmp_path: Path,
    capsys: Any,
    command: str,
) -> None:
    source, _ = _source(tmp_path)
    args = _args(source.manifest_path) if command == "registry" else refresh_args(tmp_path, source)
    before = set(tmp_path.rglob("*"))
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(registry_cli, "LocalBlobStore", side_effect=AssertionError),
        patch.object(refresh_cli, "LocalBlobStore", side_effect=AssertionError),
    ):
        platform_main([*args, "--reuse-verified", "--dry-run"])
    preview = json.loads(capsys.readouterr().out)
    policy = preview if command == "registry" else preview["vintages"][0]
    assert policy["reuse_verified"] is True
    assert "cached_bytes" not in policy and "cached_objects" not in policy
    assert set(tmp_path.rglob("*")) == before


@pytest.mark.parametrize("corrupt", [False, True])
def test_registry_cli_executes_reuse_and_safely_reports_corruption(
    tmp_path: Path, capsys: Any, corrupt: bool
) -> None:
    manifest = _manifest()
    store = LocalBlobStore(tmp_path / "objects")
    payload = SECRET.encode() if corrupt else PAYLOAD
    key = manifest["objects"][0]["storage_key"]
    store.put_bytes(key, payload)
    args = [
        *_args(_write(tmp_path, manifest)),
        "--reuse-verified",
        "--root",
        str(store.root),
        "--receipt-dir",
        str(tmp_path / "receipts"),
    ]
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
        if corrupt:
            with pytest.raises(SystemExit) as error:
                platform_main(args)
            assert error.value.code == 1
        else:
            platform_main(args)
    output = capsys.readouterr().out
    assert SECRET not in output and store.get_bytes(key) == payload
    document = json.loads(output)
    if corrupt:
        assert document["error"]["code"] == "ingestion_error"
        assert not (tmp_path / "receipts").exists()
    else:
        assert document["files_reused"] == 1
        receipt = json.loads(Path(document["receipt_path"]).read_bytes())
        assert receipt["fetch_statistics"]["attempts"] == 0


@pytest.mark.parametrize("command", ["registry-ingest", "ons-pay-refresh"])
def test_cli_rejects_reuse_without_https_selection(
    tmp_path: Path, capsys: Any, command: str
) -> None:
    source, _ = _source(tmp_path)
    selection = (
        ["--manifest", str(source.manifest_path), "--registry-root", str(tmp_path / "checkout")]
        if command == "registry-ingest"
        else ["--source", str(source.manifest_path), str(tmp_path / "checkout")]
    )
    with pytest.raises(SystemExit) as error:
        platform_main([command, *selection, "--reuse-verified"])
    assert error.value.code == 2
    output = capsys.readouterr()
    assert "require" in output.out + output.err


@pytest.mark.parametrize("load_only", [False, True])
def test_prepared_job_preserves_policy_and_replays_without_http(
    tmp_path: Path,
    capsys: Any,
    load_only: bool,
) -> None:
    source, manifest = _source(tmp_path)
    source = replace(source, fetch_options=replace(source.fetch_options, reuse_verified=True))
    args = [
        *refresh_args(tmp_path, source),
        "--reuse-verified",
        "--save-job",
        str(tmp_path / "job.json"),
    ]
    if load_only:
        args.append("--load-only")
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
        platform_main(args)
    saved = json.loads(capsys.readouterr().out)
    assert saved["job"]["fetch"]["reuse_verified"] is True
    path = Path(saved["job_path"])
    job_bytes = path.read_bytes()
    job = load_refresh_job(path)
    assert job.sources[0].fetch_options.reuse_verified is True
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        first = run_job(path, capsys)
    assert first["objects_published"] == 1
    before = {
        p: p.stat().st_mtime_ns for p in tmp_path.rglob("*") if p.suffix in {".csv", ".parquet"}
    }
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
        replay = run_job(path, capsys)
    assert replay["objects_reused"] == 1 and replay["objects_published"] == 0
    assert replay["job_sha256"] == saved["job_sha256"] and path.read_bytes() == job_bytes
    assert {p: p.stat().st_mtime_ns for p in before} == before
    audit = json.loads(Path(replay["receipt_path"]).read_bytes())
    receipt = json.loads(Path(audit["sources"][0]["ingestion_receipt_path"]).read_bytes())
    assert receipt["fetch_statistics"]["attempts"] == 0
    assert receipt["source"]["retrieved_at"] == manifest["source"]["retrieved_at"]
    key = VintageKey(1, source.fetch_options.commit, manifest["objects"][0]["sha256"])
    if load_only:
        assert not list((tmp_path / "curated").rglob("*.parquet"))
        with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
            transform_ons_pay(
                load_audit=Path(replay["receipt_path"]),
                store=LocalBlobStore(tmp_path / "objects"),
                output_root=tmp_path / "transformed",
            )
        report = monitor_ons_pay(data_root=tmp_path / "transformed", key=key, workflow="transform")
    else:
        assert replay["parquet_reused"] == 3
        report = monitor_ons_pay(data_root=tmp_path / "curated", key=key)
    assert report.status == "healthy"


@pytest.mark.parametrize("invalid", [None, 0, 1, "false"])
def test_saved_job_rejects_non_boolean_reuse(tmp_path: Path, invalid: Any) -> None:
    path, document, _ = _job(tmp_path)
    document["fetch"] = {"reuse_verified": invalid}
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RefreshJobError):
        load_refresh_job(path)


def test_saved_job_rejects_reuse_override(tmp_path: Path, capsys: Any) -> None:
    path, _, _ = _job(tmp_path)
    with pytest.raises(SystemExit) as error:
        platform_main(["ons-pay-refresh", "--job", str(path), "--reuse-verified"])
    assert error.value.code == 2 and "cannot be overridden" in capsys.readouterr().err


def test_preparation_rejects_mixed_reuse_policies_and_omits_default(tmp_path: Path) -> None:
    first, _ = _source(tmp_path)
    second, _ = _source(tmp_path, "b")
    prepared = _prepare(tmp_path, [first])
    assert "reuse_verified" not in json.loads(prepared.content)["fetch"]
    second = replace(second, fetch_options=replace(second.fetch_options, reuse_verified=True))
    with pytest.raises(RefreshJobError, match="shared HTTPS"):
        _prepare(tmp_path, [first, second])
    save_refresh_job(prepared)
    assert load_refresh_job(prepared.path).sources[0].fetch_options.reuse_verified is False


def test_partial_batch_recovery_reuses_verified_prefix_and_rechecks_freshness(
    tmp_path: Path,
) -> None:
    first, one = _source(tmp_path)
    second_payload = PAYLOAD.replace(b"200,Pounds", b"205,Pounds")
    second, two = _source(tmp_path, "b", second_payload)
    sources = [
        replace(s, fetch_options=replace(s.fetch_options, reuse_verified=True))
        for s in [first, second]
    ]
    store = LocalBlobStore(tmp_path / "objects")
    kwargs = {
        "sources": sources,
        "store": store,
        "output_root": tmp_path / "curated",
        "as_of": AS_OF,
    }
    opener = Mock()
    opener.open.side_effect = [Response(one["objects"][0]), _http(two["objects"][0], 503)]
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        pytest.raises(RefreshError) as failure,
    ):
        refresh_ons_pay(**kwargs)
    failed = _receipt(failure.value)
    assert failed["metrics"]["vintages_completed"] == 1
    before = {p: p.stat().st_mtime_ns for p in tmp_path.rglob("*.parquet")}
    opener.open.reset_mock(side_effect=True)
    opener.open.return_value = Response(two["objects"][0], second_payload)
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        replay = refresh_ons_pay(**kwargs)
    opener.open.assert_called_once()
    assert replay.sources[0].objects_reused == 1 and replay.sources[0].parquet_reused == 3
    assert replay.sources[1].objects_published == 1
    assert {p: p.stat().st_mtime_ns for p in before} == before
    with (
        patch.object(store, "exists", side_effect=AssertionError("store consulted")),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(RefreshError) as stale,
    ):
        refresh_ons_pay(**kwargs, max_source_age_days=0)
    assert _receipt(stale.value)["failure"]["phase"] == "preflight"
