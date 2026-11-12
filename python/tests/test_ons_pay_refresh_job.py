"""Saved refresh configuration, portable paths, pinned handoffs and audited replay."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import URLError

import pytest
from test_ons_pay_refresh import _source as local_source
from test_ons_pay_remote_refresh import _source as remote_source
from test_registry_fetch import OPTIONS, PAYLOAD, SECRET, Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability.ons_pay_monitor import monitor_ons_pay
from data_platform_lab.orchestration import ons_pay_refresh as refresh
from data_platform_lab.orchestration import ons_pay_refresh_cli as cli
from data_platform_lab.orchestration.ons_pay_refresh_job import RefreshJobError, load_refresh_job
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.storage import LocalBlobStore


def _entry(source: Any) -> dict[str, Any]:
    result = {
        "manifest_path": "../" + source.manifest_path.name,
        "manifest_sha256": hashlib.sha256(source.manifest_path.read_bytes()).hexdigest(),
    }
    if isinstance(source, refresh.RemoteRefreshSource):
        return result | {"transport": "https", "registry_commit": source.fetch_options.commit}
    return result | {"transport": "checkout", "registry_root": "../" + source.registry_root.name}


def _write(path: Path, document: Any) -> None:
    path.write_text(json.dumps(document), encoding="utf-8")


def _job(tmp_path: Path) -> tuple[Path, dict[str, Any], Any]:
    source, manifest = remote_source(tmp_path)
    document = {
        "schema_version": 1,
        "sources": [_entry(source)],
        "object_root": "../objects",
        "output_root": "../curated",
    }
    path = tmp_path / "jobs" / "refresh.json"
    path.parent.mkdir()
    _write(path, document)
    return path, document, manifest


def _run(path: Path, capsys: Any, *extra: str) -> Any:
    platform_main(["ons-pay-refresh", "--job", str(path), *extra])
    return json.loads(capsys.readouterr().out)


def test_loader_resolves_paths_without_reading_handoffs_or_opening_transport(
    tmp_path: Path, monkeypatch: Any
) -> None:
    path, document, _ = _job(tmp_path)
    document["sources"][0]["manifest_path"] = "missing.json"
    document["fetch"] = {
        "timeout_seconds": 7,
        "max_object_bytes": 12345,
        "max_source_bytes": 23456,
        "max_attempts": 3,
        "retry_delay_seconds": 0.25,
    }
    document["max_source_age_days"] = 10
    _write(path, document)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
        job = load_refresh_job(path)
    assert job.object_root == tmp_path / "objects" and job.output_root == tmp_path / "curated"
    assert job.sources[0].manifest_path == path.parent / "missing.json"
    assert job.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert job.max_source_age_days == 10
    assert job.sources[0].fetch_options == replace(
        OPTIONS,
        timeout_seconds=7,
        max_object_bytes=12345,
        max_total_bytes=23456,
        max_attempts=3,
        retry_delay_seconds=0.25,
    )
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


def test_job_dry_run_keeps_mixed_source_order_and_reports_resolved_destinations(
    tmp_path: Path, capsys: Any
) -> None:
    path, document, _ = _job(tmp_path)
    local = local_source(tmp_path)
    second, _ = remote_source(tmp_path, "b")
    document["sources"] = [_entry(second), _entry(local), document["sources"][0]]
    document["fetch"] = {"max_attempts": 2}
    _write(path, document)
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(cli, "LocalBlobStore", side_effect=AssertionError),
    ):
        result = _run(path, capsys, "--dry-run")
    assert result["writes"] is False and result["job_sha256"] == load_refresh_job(path).sha256
    assert result["object_root"] == str(tmp_path / "objects")
    assert result["output_root"] == str(tmp_path / "curated")
    assert [item["source_transport"] for item in result["vintages"]] == [
        "https",
        "checkout",
        "https",
    ]
    assert [item["manifest_sha256"] for item in result["vintages"]] == [
        item["manifest_sha256"] for item in document["sources"]
    ]
    assert result["vintages"][0]["registry_commit"] == "b" * 40
    assert result["vintages"][0]["max_attempts"] == 2
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


def test_job_execution_and_replay_keep_lineage_and_monitor_compatibility(
    tmp_path: Path, capsys: Any, monkeypatch: Any
) -> None:
    path, document, manifest = _job(tmp_path)
    local = local_source(tmp_path)
    document["sources"].append(_entry(local))
    document["fetch"] = {"timeout_seconds": 7, "max_attempts": 2, "retry_delay_seconds": 0.5}
    _write(path, document)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    elsewhere = tmp_path / "other"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    opener = Mock()
    opener.open.side_effect = [
        URLError(TimeoutError(SECRET)),
        Response(manifest["objects"][0]),
        Response(manifest["objects"][0]),
    ]
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        patch.object(registry_fetch, "sleep") as pause,
    ):
        first = _run(path, capsys)
        before = {p: p.stat().st_mtime_ns for p in (tmp_path / "objects").rglob("*.csv")}
        before.update({p: p.stat().st_mtime_ns for p in (tmp_path / "curated").rglob("*.parquet")})
        replay = _run(path, capsys)
    assert first["vintages_completed"] == 2 and first["job_sha256"] == digest
    assert replay["objects_reused"] == 2 and replay["parquet_reused"] == 6
    assert {p: p.stat().st_mtime_ns for p in before} == before
    assert not list(elsewhere.iterdir())
    audit = json.loads(Path(replay["receipt_path"]).read_bytes())
    assert audit["job_sha256"] == digest and audit["status"] == "success"
    for actual, selected in zip(audit["sources"], document["sources"], strict=True):
        ingestion = json.loads(Path(actual["ingestion_receipt_path"]).read_bytes())
        assert ingestion["manifest_sha256"] == selected["manifest_sha256"]
    pause.assert_called_once_with(0.5)
    assert all(c.kwargs["timeout"] == 7 for c in opener.open.call_args_list)
    key = VintageKey(1, OPTIONS.commit, manifest["objects"][0]["sha256"])
    health = monitor_ons_pay(
        data_root=tmp_path / "curated", key=key, now=datetime.now(UTC) + timedelta(seconds=1)
    )
    assert health.status == "healthy" and health.latest_batch_run_id == replay["run_id"]


@pytest.mark.parametrize("transport", ["https", "checkout"])
@pytest.mark.parametrize("dry_run", [True, False])
def test_changed_handoff_fails_whole_batch_before_first_source_io(
    tmp_path: Path, capsys: Any, transport: str, dry_run: bool
) -> None:
    path, document, _ = _job(tmp_path)
    source = remote_source(tmp_path, "b")[0] if transport == "https" else local_source(tmp_path)
    document["sources"].append(_entry(source))
    _write(path, document)
    source.manifest_path.write_bytes(source.manifest_path.read_bytes() + b" ")
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(refresh, "ingest_handoff", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys, *(["--dry-run"] if dry_run else []))
    assert error.value.code == (2 if dry_run else 1)
    if dry_run:
        assert not (tmp_path / "curated").exists()
    else:
        audits = list((tmp_path / "curated" / "manifests").glob("ons_pay_refresh_*.json"))
        audit = json.loads(audits[0].read_bytes())
        assert audit["job_sha256"] == load_refresh_job(path).sha256
        assert audit["failure"]["phase"] == "preflight" and audit["failure"]["source_index"] == 1
        assert audit["metrics"]["vintages_completed"] == 0
    assert not list((tmp_path / "objects").rglob("*.csv"))


def test_saved_job_partial_failure_keeps_first_vintage_and_recovers(
    tmp_path: Path, capsys: Any
) -> None:
    path, document, one = _job(tmp_path)
    second, two = remote_source(tmp_path, "b", PAYLOAD.replace(b"200,Pounds", b"205,Pounds"))
    document["sources"].append(_entry(second))
    _write(path, document)
    opener = Mock()
    opener.open.side_effect = [Response(one["objects"][0]), URLError(SECRET)]
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys)
    assert error.value.code == 1
    failed = next((tmp_path / "curated" / "manifests").glob("ons_pay_refresh_*.json"))
    before = failed.read_bytes()
    audit = json.loads(before)
    assert audit["job_sha256"] == load_refresh_job(path).sha256
    assert audit["metrics"]["vintages_completed"] == 1 and audit["failure"]["source_index"] == 1
    assert SECRET not in before.decode()
    opener.open.side_effect = [
        Response(one["objects"][0]),
        Response(two["objects"][0], PAYLOAD.replace(b"200,Pounds", b"205,Pounds")),
    ]
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        result = _run(path, capsys)
    assert result["objects_reused"] == 1 and result["objects_published"] == 1
    assert len(CuratedCatalog(tmp_path / "curated").list_vintages()) == 2
    assert failed.read_bytes() == before


@pytest.mark.parametrize(
    "case",
    [
        "version",
        "bool_version",
        "float_version",
        "unknown",
        "missing",
        "empty_sources",
        "many_sources",
        "source_type",
        "transport",
        "source_unknown",
        "missing_digest",
        "bad_digest",
        "uppercase_digest",
        "missing_commit",
        "floating_commit",
        "missing_path",
        "blank_path",
        "null_path",
        "wrong_path_type",
        "fetch_type",
        "fetch_unknown",
        "fetch_timeout",
        "fetch_budget",
        "fetch_attempts",
        "fetch_delay",
        "negative_age",
        "bool_age",
        "float_age",
        "object_root",
        "output_root",
        "checkout_fetch",
    ],
)
def test_invalid_jobs_fail_before_store_or_network_with_safe_diagnostics(
    tmp_path: Path, capsys: Any, case: str
) -> None:
    path, document, _ = _job(tmp_path)
    source = document["sources"][0]
    if case in ("version", "bool_version", "float_version"):
        document["schema_version"] = {"version": 2, "bool_version": True, "float_version": 1.0}[
            case
        ]
    elif case == "unknown":
        document[SECRET] = SECRET
    elif case == "missing":
        del document["object_root"]
    elif case == "empty_sources":
        document["sources"] = []
    elif case == "many_sources":
        document["sources"] *= 257
    elif case == "source_type":
        document["sources"] = [SECRET]
    elif case == "transport":
        source["transport"] = SECRET
    elif case == "source_unknown":
        source[SECRET] = SECRET
    elif case == "missing_digest":
        del source["manifest_sha256"]
    elif case == "bad_digest":
        source["manifest_sha256"] = SECRET
    elif case == "uppercase_digest":
        source["manifest_sha256"] = "B" * 64
    elif case == "missing_commit":
        del source["registry_commit"]
    elif case == "floating_commit":
        source["registry_commit"] = "main"
    elif case == "missing_path":
        del source["manifest_path"]
    elif case == "blank_path":
        source["manifest_path"] = " "
    elif case == "null_path":
        source["manifest_path"] = "a\x00b"
    elif case == "wrong_path_type":
        source["manifest_path"] = []
    elif case == "fetch_type":
        document["fetch"] = None
    elif case == "fetch_unknown":
        document["fetch"] = {SECRET: SECRET}
    elif case == "fetch_timeout":
        document["fetch"] = {"timeout_seconds": 301}
    elif case == "fetch_budget":
        document["fetch"] = {"max_source_bytes": 0}
    elif case == "fetch_attempts":
        document["fetch"] = {"max_attempts": True}
    elif case == "fetch_delay":
        document["fetch"] = {"retry_delay_seconds": "1"}
    elif case in ("negative_age", "bool_age", "float_age"):
        document["max_source_age_days"] = {"negative_age": -1, "bool_age": True, "float_age": 1.5}[
            case
        ]
    elif case in ("object_root", "output_root"):
        document[case] = None
    elif case == "checkout_fetch":
        source.pop("registry_commit")
        source.update(transport="checkout", registry_root="unused")
        document["fetch"] = {}
    _write(path, document)
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        patch.object(cli, "LocalBlobStore", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys)
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


@pytest.mark.parametrize(
    "payload",
    [
        b"not JSON",
        b"[]",
        b"null",
        b"\xff",
        b" " * (256 * 1024 + 1),
        b"[" * 2000,
        b'{"schema_version":1,"schema_version":1}',
        b'{"fetch":{"max_attempts":2,"max_attempts":3}}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
    ],
)
def test_bad_encoding_size_json_or_duplicate_fields_are_rejected(
    tmp_path: Path, payload: bytes
) -> None:
    path = tmp_path / "job.json"
    path.write_bytes(payload)
    with pytest.raises(RefreshJobError):
        load_refresh_job(path)


@pytest.mark.parametrize(
    "flags",
    [
        ["--source", "unused", "unused"],
        ["--fetch-source", "unused", OPTIONS.commit],
        ["--object-root", "unused"],
        ["--output-root", "unused"],
        ["--max-source-age-days", "0"],
        ["--timeout-seconds", "30"],
        ["--max-object-bytes", "1"],
        ["--max-source-bytes", "1"],
        ["--max-attempts", "1"],
        ["--retry-delay-seconds", "1"],
    ],
)
def test_job_cannot_be_overridden_by_cli_even_with_default_values(
    tmp_path: Path, capsys: Any, flags: list[str]
) -> None:
    path, _, _ = _job(tmp_path)
    with (
        patch.object(cli, "load_refresh_job", side_effect=AssertionError),
        patch.object(cli, "LocalBlobStore", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys, *flags)
    assert error.value.code == 2


@pytest.mark.parametrize("destination", ["object_root", "output_root"])
def test_job_cannot_write_into_any_selected_checkout(
    tmp_path: Path, capsys: Any, destination: str
) -> None:
    path, document, _ = _job(tmp_path)
    local = local_source(tmp_path)
    document["sources"].append(_entry(local))
    document[destination] = str(local.registry_root / "generated")
    _write(path, document)
    with (
        patch.object(cli, "LocalBlobStore", side_effect=AssertionError),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys)
    assert error.value.code == 2 and not (local.registry_root / "generated").exists()


def test_job_file_is_read_once_and_does_not_change_during_execution(
    tmp_path: Path, capsys: Any
) -> None:
    path, document, manifest = _job(tmp_path)
    expected = hashlib.sha256(path.read_bytes()).hexdigest()
    original = cli.refresh_ons_pay
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])

    def changed(**kwargs: Any) -> Any:
        document["output_root"] = "../other-output"
        _write(path, document)
        return original(**kwargs)

    with (
        patch.object(cli, "refresh_ons_pay", side_effect=changed),
        patch.object(registry_fetch, "build_opener", return_value=opener),
    ):
        result = _run(path, capsys)
    audit = json.loads(Path(result["receipt_path"]).read_bytes())
    assert audit["job_sha256"] == expected != load_refresh_job(path).sha256
    assert not (tmp_path / "other-output").exists()


def test_job_source_change_between_preflight_and_ingestion_is_rejected(
    tmp_path: Path, capsys: Any
) -> None:
    path, _, _ = _job(tmp_path)
    original = refresh.ingest_registry_fetch

    def changed(**kwargs: Any) -> Any:
        manifest_path = kwargs["manifest_path"]
        manifest_path.write_bytes(manifest_path.read_bytes() + b" ")
        return original(**kwargs)

    with (
        patch.object(refresh, "ingest_registry_fetch", side_effect=changed),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys)
    assert error.value.code == 1
    audit = json.loads(
        next((tmp_path / "curated" / "manifests").glob("ons_pay_refresh_*.json")).read_bytes()
    )
    assert (
        audit["failure"]["phase"] == "ingestion"
        and audit["job_sha256"] == load_refresh_job(path).sha256
    )


@pytest.mark.parametrize("value", ["bad", "B" * 64, True])
def test_invalid_job_digest_cannot_create_audit(tmp_path: Path, value: Any) -> None:
    with pytest.raises(refresh.RefreshError, match="job_sha256"):
        refresh.refresh_ons_pay(
            sources=[],
            store=LocalBlobStore(tmp_path / "objects"),
            output_root=tmp_path / "curated",
            job_sha256=value,
        )
    assert not (tmp_path / "curated").exists()


def test_job_paths_are_literal_without_variable_or_home_expansion(tmp_path: Path) -> None:
    path, document, _ = _job(tmp_path)
    document["object_root"] = "${OUTPUT}/objects"
    document["output_root"] = "~/curated"
    _write(path, document)
    job = load_refresh_job(path)
    assert job.object_root == path.parent / "${OUTPUT}" / "objects"
    assert job.output_root == path.parent / "~" / "curated"


@pytest.mark.parametrize("case", ["duplicate", "stale", "commit"])
def test_job_preserves_existing_batch_identity_and_freshness_gates(
    tmp_path: Path, capsys: Any, case: str
) -> None:
    path, document, _ = _job(tmp_path)
    if case == "duplicate":
        document["sources"].append(document["sources"][0])
    elif case == "commit":
        document["sources"][0]["registry_commit"] = "b" * 40
    else:
        handoff = tmp_path / "handoff-a.json"
        manifest = json.loads(handoff.read_bytes())
        manifest["source"]["retrieved_at"] = "2020-01-01"
        _write(handoff, manifest)
        document["sources"][0]["manifest_sha256"] = hashlib.sha256(handoff.read_bytes()).hexdigest()
        document["max_source_age_days"] = 1
    _write(path, document)
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        _run(path, capsys)
    assert error.value.code == 1
    audit = json.loads(
        next((tmp_path / "curated" / "manifests").glob("ons_pay_refresh_*.json")).read_bytes()
    )
    assert audit["failure"]["phase"] == "preflight" and audit["metrics"]["vintages_completed"] == 0
    assert audit["job_sha256"] == load_refresh_job(path).sha256


@pytest.mark.parametrize(
    "operation,error_type", [("open", PermissionError), ("resolve", RuntimeError)]
)
def test_job_read_errors_do_not_echo_exception_contents(
    tmp_path: Path, operation: str, error_type: type[Exception]
) -> None:
    path, _, _ = _job(tmp_path)
    with (
        patch.object(Path, operation, side_effect=error_type(SECRET)),
        pytest.raises(RefreshJobError) as error,
    ):
        load_refresh_job(path)
    assert SECRET not in str(error.value)
