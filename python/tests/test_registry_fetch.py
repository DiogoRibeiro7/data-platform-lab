"""Pinned extraction, verified loading, replay, and optional ONS transformation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from email.message import Message
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Thread
from typing import Any
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError
from urllib.request import build_opener

import pytest
from test_ons_pay_serving import PAY_CSV

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.curation.ons_pay import curate_ons_pay
from data_platform_lab.ingestion import registry_cli, registry_fetch
from data_platform_lab.ingestion.registry_fetch import (
    RegistryFetchOptions,
    ingest_registry_fetch,
    plan_registry_fetch,
)
from data_platform_lab.ingestion.registry_handoff import HandoffError
from data_platform_lab.serving.catalog import VintageKey
from data_platform_lab.serving.publish_plan import plan_ons_pay_publishing
from data_platform_lab.storage import LocalBlobStore

OPTIONS = RegistryFetchOptions("DiogoRibeiro7/data", "a" * 40, "ons-gross-median-weekly-pay")
SOURCE = f"datasets/{OPTIONS.dataset_id}/raw/gross-median-weekly-pay.csv"
PAYLOAD = PAY_CSV.encode("utf-8-sig")
SECRET = "https://private-user:password@private-host/secret"


def _manifest(payload: bytes = PAYLOAD) -> dict[str, Any]:
    sha = hashlib.sha256(payload).hexdigest()
    return {
        "manifest_version": 1,
        "dataset_id": OPTIONS.dataset_id,
        "title": "Synthetic ONS pay",
        "lifecycle_status": "active",
        "registry": {"repository": OPTIONS.repository, "commit": OPTIONS.commit},
        "source": {
            "publisher": "Synthetic fixture",
            "url": "https://example.org/ons",
            "snapshot": "fixture",
            "retrieved_at": "2026-11-09",
        },
        "license": {
            "name": "CC0",
            "url": "https://example.org/license",
            "redistribution": "allowed",
        },
        "citation": "Synthetic fixture",
        "local_bytes_verified": False,
        "objects": [
            {
                "source_path": SOURCE,
                "source_uri": f"https://raw.githubusercontent.com/{OPTIONS.repository}/{OPTIONS.commit}/{SOURCE}",
                "sha256": sha,
                "role": "raw",
                "format": "csv",
                "storage_key": (
                    f"canonical/{OPTIONS.dataset_id}/{sha}/raw/gross-median-weekly-pay.csv"
                ),
                "size_bytes": len(payload),
            }
        ],
    }


def _write(tmp_path: Path, manifest: dict[str, Any]) -> Path:
    path = tmp_path / "handoff.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


class Response(BytesIO):
    """HTTP body that records bounded reads and exposes transport metadata."""

    def __init__(self, item: dict[str, Any], payload: bytes = PAYLOAD, **headers: str) -> None:
        super().__init__(payload)
        self.url = item["source_uri"]
        self.status = 200
        self.headers = headers
        self.read_sizes: list[int] = []

    def geturl(self) -> str:
        return self.url

    def read(self, size: int = -1) -> bytes:
        assert size > 0
        self.read_sizes.append(size)
        return super().read(size)


def _run(
    tmp_path: Path,
    manifest: dict[str, Any],
    responses: list[Any],
    options: RegistryFetchOptions = OPTIONS,
) -> tuple[Any, Any, Any]:
    opener = Mock()
    opener.open.side_effect = responses
    store = LocalBlobStore(tmp_path / "objects")
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        result = ingest_registry_fetch(
            manifest_path=_write(tmp_path, manifest),
            options=options,
            store=store,
            receipt_dir=tmp_path / "receipts",
        )
    return result, store, opener


def _args(path: Path) -> list[str]:
    return [
        "registry-ingest",
        "--manifest",
        str(path),
        "--fetch",
        "--registry-repository",
        OPTIONS.repository,
        "--registry-commit",
        OPTIONS.commit,
        "--dataset-id",
        OPTIONS.dataset_id,
    ]


def test_fetch_load_replay_and_transform_use_existing_contracts(tmp_path: Path) -> None:
    manifest = _manifest()
    first_body = Response(manifest["objects"][0], **{"Content-Length": str(len(PAYLOAD))})
    first, store, opener = _run(tmp_path, manifest, [first_body])
    key = manifest["objects"][0]["storage_key"]
    assert store.get_bytes(key) == PAYLOAD
    before = (store.root / key).stat().st_mtime_ns
    receipt = json.loads(first.receipt_path.read_text())
    assert receipt["pipeline_name"] == "registry_handoff_ingestion"
    assert receipt["source_transport"] == "https" and receipt["registry"] == manifest["registry"]
    assert receipt["source"] == manifest["source"] and receipt["citation"] == manifest["citation"]
    assert receipt["objects"][0]["sha256"] == hashlib.sha256(PAYLOAD).hexdigest()
    assert first.bytes_verified == len(PAYLOAD) and first.files_published == 1
    request = opener.open.call_args.args[0]
    assert request.full_url == manifest["objects"][0]["source_uri"]
    assert request.get_method() == "GET" and request.get_header("Accept-encoding") == "identity"
    assert opener.open.call_args.kwargs == {"timeout": 30}
    assert first_body.closed and max(first_body.read_sizes) <= 64 * 1024
    second, _, _ = _run(tmp_path, manifest, [Response(manifest["objects"][0])])
    assert second.files_reused == 1 and second.files_published == 0
    assert (store.root / key).stat().st_mtime_ns == before
    assert first.receipt_path != second.receipt_path
    assert not (tmp_path / "curated").exists()
    curated = curate_ons_pay(
        input_receipt=second.receipt_path, store=store, output_root=tmp_path / "curated"
    )
    assert curated.source_rows == curated.rows_with_pay + curated.rows_without_pay == 7
    assert len(curated.outputs) == 3
    vintage = VintageKey(1, OPTIONS.commit, manifest["objects"][0]["sha256"])
    for table in ("geography_year", "local_authority_yoy"):
        plan = plan_ons_pay_publishing(
            data_root=tmp_path / "curated", vintage=vintage, table=table, shape="relational"
        )
        assert plan.source_rows > 0


@pytest.mark.parametrize(
    "changes",
    [
        {"repository": "another/repo"},
        {"commit": "b" * 40},
        {"dataset_id": "other-data"},
        {"repository": "../repo"},
        {"commit": "main"},
        {"dataset_id": SECRET},
        {"timeout_seconds": 0},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": True},
        {"timeout_seconds": 301},
        {"max_object_bytes": 0},
        {"max_total_bytes": -1},
        {"max_object_bytes": True},
    ],
)
def test_invalid_pins_and_limits_fail_before_network(
    tmp_path: Path, changes: dict[str, Any]
) -> None:
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(HandoffError) as error,
    ):
        _run(tmp_path, _manifest(), [], replace(OPTIONS, **changes))
    assert SECRET not in str(error.value)
    assert not list((tmp_path / "objects").rglob("*.csv")) and not (tmp_path / "receipts").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "url",
        "license",
        "hash",
        "object_limit",
        "total_limit",
        "objects",
        "manifest_size",
        "json",
        "type",
    ],
)
def test_entire_manifest_is_validated_before_fetch(tmp_path: Path, mutation: str) -> None:
    manifest = _manifest()
    options = OPTIONS
    if mutation == "url":
        manifest["objects"][0]["source_uri"] = SECRET
    elif mutation == "license":
        manifest["license"]["redistribution"] = "forbidden"
    elif mutation == "hash":
        manifest["objects"][0]["sha256"] = "bad"
    elif mutation == "type":
        manifest["lifecycle_status"] = {}
    elif mutation == "object_limit":
        options = replace(OPTIONS, max_object_bytes=len(PAYLOAD) - 1)
    elif mutation == "total_limit":
        options = replace(OPTIONS, max_total_bytes=len(PAYLOAD) - 1)
    elif mutation == "objects":
        item = manifest["objects"][0]
        manifest["objects"] = [
            item
            | {
                "source_path": item["source_path"] + str(i),
                "source_uri": item["source_uri"] + str(i),
                "storage_key": item["storage_key"] + str(i),
            }
            for i in range(257)
        ]
    path = _write(tmp_path, manifest)
    if mutation in ("manifest_size", "json"):
        path.write_bytes(b" " * (1024 * 1024 + 1) if mutation == "manifest_size" else b"bad JSON")
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(HandoffError) as error,
    ):
        plan_registry_fetch(manifest_path=path, options=options)
    assert SECRET not in str(error.value)


@pytest.mark.parametrize(
    "failure",
    [
        "hash",
        "size",
        "status",
        "url",
        "encoding",
        "length",
        "length_limit",
        "length_pin",
        "short",
        "limit",
        "timeout",
        "disconnect",
        "http",
    ],
)
def test_bad_transfers_never_publish_or_claim_success(tmp_path: Path, failure: str) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    options = OPTIONS
    body: Any = Response(item)
    if failure == "hash":
        body = Response(item, b"bad bytes")
    elif failure == "size":
        item["size_bytes"] += 1
    elif failure == "status":
        body.status = 206
    elif failure == "url":
        body.url = SECRET
    elif failure == "encoding":
        body.headers["Content-Encoding"] = "gzip"
    elif failure == "length":
        body.headers["Content-Length"] = "unknown"
    elif failure == "length_limit":
        item["size_bytes"] = None
        options = replace(OPTIONS, max_object_bytes=len(PAYLOAD))
        body.headers["Content-Length"] = str(len(PAYLOAD) + 1)
    elif failure == "length_pin":
        body.headers["Content-Length"] = str(len(PAYLOAD) + 1)
    elif failure == "short":
        item["size_bytes"] = None
        body.headers["Content-Length"] = str(len(PAYLOAD) + 1)
    elif failure == "limit":
        item["size_bytes"] = None
        options = replace(OPTIONS, max_object_bytes=len(PAYLOAD) - 1)
    elif failure == "timeout":
        body = URLError(TimeoutError(SECRET))
    elif failure == "disconnect":
        body.read = Mock(side_effect=IncompleteRead(b"partial"))
    elif failure == "http":
        body = HTTPError(SECRET, 429, SECRET, Message(), BytesIO(b"response secret"))
    with pytest.raises(HandoffError) as error:
        _run(tmp_path, manifest, [body], options)
    assert SECRET not in str(error.value)
    assert not list((tmp_path / "objects").rglob("*.csv")) and not (tmp_path / "receipts").exists()
    if isinstance(body, Response):
        assert body.closed
        if failure in ("length", "length_limit", "length_pin", "url", "status", "encoding"):
            assert not body.read_sizes
    if failure == "http":
        assert body.fp.closed


def test_partial_run_replays_verified_objects_and_enforces_total_budget(tmp_path: Path) -> None:
    manifest = _manifest()
    one = manifest["objects"][0]
    two = one | {
        "source_path": one["source_path"] + "2",
        "source_uri": one["source_uri"] + "2",
        "storage_key": one["storage_key"] + "2",
        "size_bytes": None,
    }
    one["size_bytes"] = None
    manifest["objects"].append(two)
    options = replace(OPTIONS, max_total_bytes=len(PAYLOAD) * 2 - 1)
    with pytest.raises(HandoffError, match="byte limits"):
        _run(tmp_path, manifest, [Response(one), Response(two)], options)
    store = LocalBlobStore(tmp_path / "objects")
    assert store.get_bytes(one["storage_key"]) == PAYLOAD and not store.exists(two["storage_key"])
    assert not (tmp_path / "receipts").exists()
    result, _, _ = _run(tmp_path, manifest, [Response(one), Response(two)])
    assert result.files_reused == result.files_published == 1
    assert result.bytes_verified == len(PAYLOAD) * 2


def test_corrupt_destination_is_not_overwritten(tmp_path: Path) -> None:
    manifest = _manifest()
    store = LocalBlobStore(tmp_path / "objects")
    key = manifest["objects"][0]["storage_key"]
    store.put_bytes(key, b"corrupt")
    with pytest.raises(HandoffError, match="destination is corrupt"):
        _run(tmp_path, manifest, [Response(manifest["objects"][0])])
    assert store.get_bytes(key) == b"corrupt" and not (tmp_path / "receipts").exists()


@pytest.mark.parametrize("code", [301, 302, 303, 307, 308])
def test_real_http_redirect_handler_never_follows_location(code: int) -> None:
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append(self.path)
            self.send_response(code)
            self.send_header("Location", "/other")
            self.end_headers()

        def log_message(self, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with pytest.raises(HTTPError) as error:
                build_opener(registry_fetch._NoRedirect()).open(
                    f"http://127.0.0.1:{server.server_port}/source", timeout=2
                )
            error.value.close()
            assert requests == ["/source"]
        finally:
            server.shutdown()
            thread.join(timeout=2)


def test_cli_preview_is_read_only_and_actual_load_can_stop_before_transform(
    tmp_path: Path, capsys: Any
) -> None:
    manifest = _manifest()
    path = _write(tmp_path, manifest)
    args = [
        *_args(path),
        "--root",
        str(tmp_path / "objects"),
        "--receipt-dir",
        str(tmp_path / "receipts"),
    ]
    with (
        patch.object(registry_cli, "_store", side_effect=AssertionError),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
    ):
        platform_main([*args, "--dry-run", "--backend", "s3", "--endpoint-url", SECRET])
    preview = json.loads(capsys.readouterr().out)
    assert preview["objects"] == 1 and preview["known_source_bytes"] == len(PAYLOAD)
    assert not (tmp_path / "objects").exists() and not (tmp_path / "receipts").exists()
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        platform_main(args)
    result = json.loads(capsys.readouterr().out)
    assert result["files_published"] == 1 and Path(result["receipt_path"]).exists()
    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize(
    "flags",
    [
        ["--registry-commit", SECRET],
        ["--timeout-seconds", SECRET],
        ["--registry-root", SECRET],
        ["--max-object-bytes", "0"],
    ],
)
def test_invalid_cli_options_are_sanitized_before_side_effects(
    tmp_path: Path, capsys: Any, flags: list[str]
) -> None:
    path = _write(tmp_path, _manifest())
    with (
        patch.object(registry_cli, "_store", side_effect=AssertionError),
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*_args(path), *flags])
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert json.loads(captured.out)["error"]["code"] == "invalid_arguments"


def test_cli_transfer_failures_have_no_raw_exception_or_response(
    tmp_path: Path, capsys: Any
) -> None:
    path = _write(tmp_path, _manifest())
    with (
        patch.object(registry_cli, "ingest_registry_fetch", side_effect=RuntimeError(SECRET)),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*_args(path), "--root", str(tmp_path / "objects")])
    assert error.value.code == 1
    captured = capsys.readouterr()
    assert SECRET not in captured.out + captured.err
    assert json.loads(captured.out)["error"]["code"] == "ingestion_error"


@pytest.mark.parametrize(
    "flags", [["--dry-run"], ["--timeout-seconds", "30"], ["--registry-commit", OPTIONS.commit]]
)
def test_fetch_flags_cannot_silently_change_local_loading(
    tmp_path: Path, capsys: Any, flags: list[str]
) -> None:
    with (
        patch.object(registry_cli, "_store", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main(
            [
                "registry-ingest",
                "--manifest",
                str(tmp_path / "unused.json"),
                "--registry-root",
                str(tmp_path / "checkout"),
                *flags,
            ]
        )
    assert error.value.code == 2
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "invalid_arguments"
