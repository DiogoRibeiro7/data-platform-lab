"""Remote ONS refresh, whole-batch preflight, audited recovery and snapshot binding."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import URLError

import pytest
from test_ons_pay_refresh import _source as local_source
from test_registry_fetch import OPTIONS, PAYLOAD, SECRET, Response, _manifest

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch, registry_handoff
from data_platform_lab.observability.ons_pay_monitor import monitor_ons_pay
from data_platform_lab.orchestration import ons_pay_refresh as refresh
from data_platform_lab.orchestration import ons_pay_refresh_cli as refresh_cli
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.storage import LocalBlobStore

AS_OF = date(2026, 11, 10)


def _source(tmp_path: Path, name: str = "a", payload: bytes = PAYLOAD) -> tuple[Any, Any]:
    manifest = _manifest(payload)
    commit = name * 40
    manifest["registry"]["commit"] = commit
    for item in manifest["objects"]:
        item["source_uri"] = item["source_uri"].replace(OPTIONS.commit, commit)
    path = tmp_path / f"handoff-{name}.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return refresh.RemoteRefreshSource(path, replace(OPTIONS, commit=commit)), manifest


def _run(tmp_path: Path, sources: list[Any], responses: list[Any], **kwargs: Any) -> Any:
    opener = Mock()
    opener.open.side_effect = responses
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        return refresh.refresh_ons_pay(
            sources=sources,
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
            as_of=AS_OF,
            **kwargs,
        )


def _audit(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _args(tmp_path: Path, *sources: Any) -> list[str]:
    args = [
        "ons-pay-refresh",
        "--object-root",
        str(tmp_path / "objects"),
        "--output-root",
        str(tmp_path / "curated"),
    ]
    for source in sources:
        args.extend(["--fetch-source", str(source.manifest_path), source.fetch_options.commit])
    return args


def test_remote_backfill_replays_without_changing_data_and_monitor_accepts_audit(
    tmp_path: Path,
) -> None:
    first, one = _source(tmp_path)
    second_payload = PAYLOAD.replace(b"200,Pounds", b"205,Pounds")
    second, two = _source(tmp_path, "b", second_payload)
    sources = [second, first]
    result = _run(
        tmp_path,
        sources,
        [Response(two["objects"][0], second_payload), Response(one["objects"][0])],
    )
    assert [item.registry_commit for item in result.sources] == ["b" * 40, "a" * 40]
    assert [item.source_rows for item in result.sources] == [7, 7]
    assert all(
        _audit(item.ingestion_receipt_path)["source_transport"] == "https"
        for item in result.sources
    )
    assert _audit(result.receipt_path)["metrics"]["source_rows"] == 14
    mtimes = {
        path: path.stat().st_mtime_ns
        for path in tmp_path.rglob("*")
        if path.suffix in {".csv", ".parquet"}
    }
    replay = _run(
        tmp_path,
        sources,
        [Response(two["objects"][0], second_payload), Response(one["objects"][0])],
    )
    assert all(item.objects_reused == 1 and item.parquet_reused == 3 for item in replay.sources)
    assert {path: path.stat().st_mtime_ns for path in mtimes} == mtimes
    assert len(CuratedCatalog(tmp_path / "curated").list_vintages()) == 2
    key = VintageKey(1, first.fetch_options.commit, one["objects"][0]["sha256"])
    report = monitor_ons_pay(
        data_root=tmp_path / "curated", key=key, now=datetime.now(UTC) + timedelta(seconds=1)
    )
    assert report.status == "healthy" and report.latest_batch_run_id == replay.run_id
    assert report.latest_vintage_success_run_id == replay.run_id


@pytest.mark.parametrize(
    "failure", ["duplicate", "commit", "repository", "stale", "future", "budget", "limits"]
)
def test_all_sources_pass_preflight_before_any_network_or_data_write(
    tmp_path: Path, failure: str
) -> None:
    first, _ = _source(tmp_path)
    second, manifest = _source(tmp_path, "b")
    if failure == "duplicate":
        second = first
    elif failure == "commit":
        second = replace(second, fetch_options=replace(second.fetch_options, commit="c" * 40))
    elif failure == "repository":
        manifest["registry"]["repository"] = "other/registry"
        manifest["objects"][0]["source_uri"] = manifest["objects"][0]["source_uri"].replace(
            OPTIONS.repository, "other/registry"
        )
        second.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        second = replace(
            second, fetch_options=replace(second.fetch_options, repository="other/registry")
        )
    elif failure in ("stale", "future"):
        manifest["source"]["retrieved_at"] = "2020-01-01" if failure == "stale" else "2026-11-11"
        second.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    elif failure == "budget":
        second = replace(second, fetch_options=replace(second.fetch_options, max_total_bytes=1))
    else:
        second = replace(
            second, fetch_options=replace(second.fetch_options, timeout_seconds=float("nan"))
        )
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(refresh.RefreshError) as error,
    ):
        refresh.refresh_ons_pay(
            sources=[first, second],
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
            max_source_age_days=30,
            as_of=AS_OF,
        )
    audit = _audit(error.value.receipt_path)
    assert audit["failure"]["phase"] == "preflight" and audit["failure"]["source_index"] == 1
    assert audit["metrics"]["vintages_completed"] == 0
    assert not list((tmp_path / "objects").rglob("*.csv"))
    assert not (tmp_path / "curated" / "gold").exists()


def test_remote_failure_keeps_completed_vintage_and_retry_recovers(tmp_path: Path) -> None:
    first, one = _source(tmp_path)
    second, two = _source(tmp_path, "b")
    with pytest.raises(refresh.RefreshError) as error:
        _run(tmp_path, [first, second], [Response(one["objects"][0]), URLError(SECRET)])
    failed_path = error.value.receipt_path
    before = failed_path.read_bytes()
    audit = _audit(failed_path)
    assert audit["failure"]["phase"] == "ingestion" and audit["failure"]["source_index"] == 1
    assert audit["metrics"]["vintages_completed"] == 1
    assert SECRET not in before.decode()
    assert len(CuratedCatalog(tmp_path / "curated").list_vintages()) == 1
    result = _run(
        tmp_path, [first, second], [Response(one["objects"][0]), Response(two["objects"][0])]
    )
    assert result.sources[0].objects_reused == 1 and result.sources[0].parquet_reused == 3
    assert len(CuratedCatalog(tmp_path / "curated").list_vintages()) == 2
    assert failed_path.read_bytes() == before


def test_downloaded_quality_failure_keeps_raw_receipt_without_gold(tmp_path: Path) -> None:
    bad = PAYLOAD.replace(b"200,Pounds", b"-20,Pounds")
    source, manifest = _source(tmp_path, payload=bad)
    with pytest.raises(refresh.RefreshError) as error:
        _run(tmp_path, [source], [Response(manifest["objects"][0], bad)])
    audit = _audit(error.value.receipt_path)
    assert audit["failure"]["phase"] == "curation"
    assert _audit(Path(audit["failure"]["ingestion_receipt_path"]))["status"] == "success"
    assert (
        audit["metrics"]["vintages_completed"] == 0 and not (tmp_path / "curated" / "gold").exists()
    )


@pytest.mark.parametrize("mode", ["https", "checkout"])
def test_manifest_changes_after_preflight_fail_before_source_io(tmp_path: Path, mode: str) -> None:
    source = _source(tmp_path)[0] if mode == "https" else local_source(tmp_path)
    name = "ingest_registry_fetch" if mode == "https" else "ingest_handoff"
    original = getattr(refresh, name)
    expected_digest = hashlib.sha256(source.manifest_path.read_bytes()).hexdigest()

    def changed(**kwargs: Any) -> Any:
        assert kwargs["expected_manifest_sha256"] == expected_digest
        document = _audit(source.manifest_path)
        document["source"]["url"] = SECRET
        source.manifest_path.write_text(json.dumps(document), encoding="utf-8")
        return original(**kwargs)

    with (
        patch.object(refresh, name, side_effect=changed),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(registry_handoff, "_verify_checkout", side_effect=AssertionError),
        pytest.raises(refresh.RefreshError) as error,
    ):
        refresh.refresh_ons_pay(
            sources=[source],
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
            as_of=AS_OF,
        )
    assert isinstance(error.value.__cause__, registry_handoff.HandoffError)
    assert "planned manifest" in str(error.value.__cause__)
    audit = _audit(error.value.receipt_path)
    assert audit["failure"]["phase"] == "ingestion" and SECRET not in json.dumps(audit)
    assert not list((tmp_path / "objects").rglob("*.csv"))


@pytest.mark.parametrize("value", ["bad", "B" * 64, True])
def test_invalid_expected_manifest_digest_cannot_fetch(tmp_path: Path, value: Any) -> None:
    source, _ = _source(tmp_path)
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(registry_handoff.HandoffError),
    ):
        registry_fetch.ingest_registry_fetch(
            manifest_path=source.manifest_path,
            options=source.fetch_options,
            store=LocalBlobStore(tmp_path / "objects"),
            receipt_dir=tmp_path / "receipts",
            expected_manifest_sha256=value,
        )
    assert not (tmp_path / "receipts").exists()


def test_mixed_library_sources_preserve_order_and_protect_local_checkout(tmp_path: Path) -> None:
    remote, manifest = _source(tmp_path)
    local = local_source(tmp_path)
    result = _run(tmp_path, [remote, local], [Response(manifest["objects"][0])])
    assert result.sources[0].registry_commit == remote.fetch_options.commit
    assert result.sources[1].manifest_path == local.manifest_path.resolve()
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(refresh.RefreshError, match="outside each canonical"),
    ):
        refresh.refresh_ons_pay(
            sources=[remote, local],
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=local.registry_root / "generated",
            as_of=AS_OF,
        )
    assert not (local.registry_root / "generated").exists()


def test_cli_dry_run_has_no_network_or_writes_and_keeps_source_order(
    tmp_path: Path, capsys: Any
) -> None:
    first, _ = _source(tmp_path)
    second, _ = _source(tmp_path, "b")
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(refresh_cli, "LocalBlobStore", side_effect=AssertionError),
    ):
        platform_main(
            [
                *_args(tmp_path, second, first),
                "--dry-run",
                "--timeout-seconds",
                "7",
                "--max-source-bytes",
                "10000",
            ]
        )
    report = json.loads(capsys.readouterr().out)
    assert report["writes"] is False and report["status"] == "planned"
    assert [item["registry_commit"] for item in report["vintages"]] == ["b" * 40, "a" * 40]
    for item, source in zip(report["vintages"], (second, first), strict=True):
        assert item["source_transport"] == "https"
        assert (
            item["manifest_sha256"] == hashlib.sha256(source.manifest_path.read_bytes()).hexdigest()
        )
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


def test_cli_runs_full_remote_refresh_and_applies_fetch_timeout(
    tmp_path: Path, capsys: Any
) -> None:
    source, manifest = _source(tmp_path)
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        platform_main([*_args(tmp_path, source), "--timeout-seconds", "7"])
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "success" and report["parquet_published"] == 3
    assert _audit(Path(report["receipt_path"]))["sources"][0]["source_rows"] == 7
    assert opener.open.call_args.kwargs["timeout"] == 7


@pytest.mark.parametrize("flags", [["--source", "unused", "unused"], ["--timeout-seconds", SECRET]])
def test_cli_invalid_source_modes_fail_before_io(
    tmp_path: Path, capsys: Any, flags: list[str]
) -> None:
    source, _ = _source(tmp_path)
    with (
        patch.object(refresh_cli, "LocalBlobStore", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*_args(tmp_path, source), *flags])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err


def test_cli_fetch_limits_require_fetch_source(tmp_path: Path) -> None:
    with (
        patch.object(refresh_cli, "LocalBlobStore", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main(
            ["ons-pay-refresh", "--source", "unused", "unused", "--max-source-bytes", "1"]
        )
    assert error.value.code == 2


def test_cli_and_audit_do_not_expose_curation_exception_text(tmp_path: Path, capsys: Any) -> None:
    source, manifest = _source(tmp_path)
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        patch.object(refresh, "curate_ons_pay", side_effect=RuntimeError(SECRET)),
        pytest.raises(SystemExit) as error,
    ):
        platform_main(_args(tmp_path, source))
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    audits = list((tmp_path / "curated" / "manifests").glob("ons_pay_refresh_*.json"))
    assert len(audits) == 1 and SECRET not in audits[0].read_text()
    assert _audit(audits[0])["failure"]["error_type"] == "RuntimeError"


@pytest.mark.parametrize("dry_run,exit_code", [(True, 2), (False, 1)])
def test_cli_bad_remote_pin_has_documented_preflight_exit(
    tmp_path: Path, capsys: Any, dry_run: bool, exit_code: int
) -> None:
    source, _ = _source(tmp_path)
    source = replace(source, fetch_options=replace(source.fetch_options, commit=SECRET))
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*_args(tmp_path, source), *(["--dry-run"] if dry_run else [])])
    assert error.value.code == exit_code
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert (tmp_path / "curated").exists() is not dry_run
