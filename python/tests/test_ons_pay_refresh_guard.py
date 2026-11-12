"""Refresh integration: exclusion before reads, audit lifetime and CLI outcomes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_local_run_guard import _holding_roots, _probe
from test_ons_pay_refresh_job import _job
from test_ons_pay_remote_refresh import _args, _source
from test_registry_fetch import Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.orchestration import ons_pay_refresh as refresh
from data_platform_lab.storage import BlobStore, LocalBlobStore


@pytest.mark.parametrize("shared", ["objects", "curated"])
def test_busy_destination_rejects_before_preflight_or_source_io(
    tmp_path: Path, shared: str
) -> None:
    source, _ = _source(tmp_path)
    store, output = LocalBlobStore(tmp_path / "objects"), tmp_path / "curated"
    with (
        _holding_roots(tmp_path / shared),
        patch.object(refresh, "plan_ons_pay_refresh", side_effect=AssertionError("preflight ran")),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("network opened")),
        pytest.raises(refresh.RefreshBusyError) as caught,
    ):
        refresh.refresh_ons_pay(sources=[source], store=store, output_root=output)
    assert caught.value.receipt_path is None and caught.value.source_index is None
    assert list(store.list_objects()) == []
    assert not output.exists()
    assert _probe(store.root, output).returncode == 0


@pytest.mark.parametrize("mode", ["direct", "job"])
def test_cli_busy_exit_is_distinct_and_dry_run_remains_read_only(
    tmp_path: Path, capsys: Any, mode: str
) -> None:
    job, _, _ = _job(tmp_path)
    source, _ = _source(tmp_path)
    args = ["ons-pay-refresh", "--job", str(job)] if mode == "job" else _args(tmp_path, source)
    with (
        _holding_roots(tmp_path / "curated"),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("network opened")),
    ):
        before = set(tmp_path.rglob("*"))
        platform_main([*args, "--dry-run"])
        assert json.loads(capsys.readouterr().out)["writes"] is False
        assert set(tmp_path.rglob("*")) == before
        with pytest.raises(SystemExit) as caught:
            platform_main(args)
        assert caught.value.code == 3
    captured = capsys.readouterr()
    assert not captured.out and "retry after it finishes" in captured.err
    assert "audit" not in captured.err and "Traceback" not in captured.err
    assert not (tmp_path / "curated").exists()


@pytest.mark.parametrize("failure", [None, "preflight", "curation"])
def test_both_guards_cover_success_and_failure_audit_publication(
    tmp_path: Path, monkeypatch: Any, failure: str | None
) -> None:
    source, manifest = _source(tmp_path)
    if failure == "preflight":
        source.manifest_path.write_text("invalid JSON", encoding="utf-8")
    elif failure == "curation":
        monkeypatch.setattr(refresh, "curate_ons_pay", Mock(side_effect=ValueError("bad data")))
    store, output = LocalBlobStore(tmp_path / "objects"), tmp_path / "curated"
    original = refresh._write_audit
    audit_calls = 0

    def audit_while_locked(path: Path, document: Any) -> None:
        nonlocal audit_calls
        assert _probe(store.root).returncode == 3
        assert _probe(output).returncode == 3
        original(path, document)
        audit_calls += 1

    monkeypatch.setattr(refresh, "_write_audit", audit_while_locked)
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        if failure is None:
            result = refresh.refresh_ons_pay(sources=[source], store=store, output_root=output)
            assert len(result.sources) == 1
        else:
            with pytest.raises(refresh.RefreshError) as caught:
                refresh.refresh_ons_pay(sources=[source], store=store, output_root=output)
            assert not isinstance(caught.value, refresh.RefreshBusyError)
            assert caught.value.receipt_path.is_file()
    assert audit_calls == 1
    assert _probe(store.root, output).returncode == 0
    assert all(not item.key.endswith(".lock") for item in store.list_objects())


def test_audit_write_failure_releases_both_guards(tmp_path: Path) -> None:
    store, output = LocalBlobStore(tmp_path / "objects"), tmp_path / "curated"
    with (
        patch.object(refresh, "_write_audit", side_effect=OSError("audit unavailable")),
        pytest.raises(OSError, match="audit unavailable"),
    ):
        refresh.refresh_ons_pay(sources=[], store=store, output_root=output)
    assert _probe(store.root, output).returncode == 0


def test_guard_setup_error_has_no_audit_or_sensitive_cli_details(
    tmp_path: Path, capsys: Any
) -> None:
    source, _ = _source(tmp_path)
    (tmp_path / ".curated.ons-pay-refresh.lock").mkdir()
    with pytest.raises(SystemExit) as caught:
        platform_main(_args(tmp_path, source))
    assert caught.value.code == 1
    captured = capsys.readouterr()
    assert not captured.out
    assert captured.err == "Unable to acquire ONS pay refresh destination guards.\n"
    assert not (tmp_path / "curated").exists()


@pytest.mark.parametrize("destination", ["objects", "curated"])
def test_canonical_checkout_is_rejected_before_guard_metadata(
    tmp_path: Path, destination: str
) -> None:
    checkout = tmp_path / "registry"
    checkout.mkdir()
    store = LocalBlobStore((checkout if destination == "objects" else tmp_path) / "objects")
    output = (checkout if destination == "curated" else tmp_path) / "curated"
    source = refresh.RefreshSource(tmp_path / "missing.json", checkout)
    with pytest.raises(refresh.RefreshError, match="outside each canonical"):
        refresh.refresh_ons_pay(sources=[source], store=store, output_root=output)
    assert not list(tmp_path.rglob("*.lock"))
    assert not output.exists()


def test_nonlocal_blob_store_still_guards_local_curation(tmp_path: Path) -> None:
    store = Mock(spec=BlobStore)
    output = tmp_path / "curated"
    with _holding_roots(output), pytest.raises(refresh.RefreshBusyError):
        refresh.refresh_ons_pay(sources=[], store=store, output_root=output)
    assert not store.mock_calls and not output.exists()
