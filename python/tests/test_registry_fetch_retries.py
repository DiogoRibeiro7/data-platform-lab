"""Opt-in HTTP recovery, transfer budgets and unchanged verified loading contracts."""

from __future__ import annotations

import json
import socket
import ssl
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from email.message import Message
from email.utils import format_datetime
from http.client import IncompleteRead, RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from threading import Thread
from typing import Any
from unittest.mock import Mock, call, patch
from urllib.error import HTTPError, URLError

import pytest
from test_ons_pay_remote_refresh import _source as remote_source
from test_registry_fetch import OPTIONS, PAYLOAD, SECRET, Response, _args, _manifest, _write

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_cli, registry_fetch
from data_platform_lab.ingestion.registry_handoff import HandoffError, parse_handoff
from data_platform_lab.orchestration import ons_pay_refresh_cli
from data_platform_lab.orchestration.ons_pay_refresh import RefreshError, refresh_ons_pay
from data_platform_lab.storage import LocalBlobStore


def _http(item: dict[str, Any], status: int, retry_after: str | None = None) -> HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return HTTPError(item["source_uri"], status, SECRET, headers, BytesIO(SECRET.encode()))


def _ingest(tmp_path: Path, manifest: dict[str, Any], **options: Any) -> Any:
    return registry_fetch.ingest_registry_fetch(
        manifest_path=_write(tmp_path, manifest),
        options=replace(OPTIONS, max_attempts=3, **options),
        store=LocalBlobStore(tmp_path / "objects"),
        receipt_dir=tmp_path / "receipts",
    )


def _receipt(result: Any) -> Any:
    return json.loads(result.receipt_path.read_bytes())


@pytest.fixture
def transport() -> Any:
    opener = Mock()
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        patch.object(registry_fetch, "sleep") as pause,
    ):
        yield opener, pause


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_transient_http_retries_exact_request_then_loads_once(
    tmp_path: Path, transport: Any, status: int
) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    failure = _http(item, status)
    body = Response(item)
    opener, pause = transport
    opener.open.side_effect = [failure, body]
    result = _ingest(tmp_path, manifest)
    assert result.files_published == 1 and result.bytes_verified == len(PAYLOAD)
    assert _receipt(result)["fetch_statistics"] == {
        "attempts": 2,
        "retries": 1,
        "downloaded_bytes": len(PAYLOAD),
    }
    pause.assert_called_once_with(1)
    assert failure.fp.closed and body.closed
    assert all(c.args[0].full_url == item["source_uri"] for c in opener.open.call_args_list)
    assert all(c.args[0].get_method() == "GET" for c in opener.open.call_args_list)
    assert all(c.kwargs["timeout"] == 30 for c in opener.open.call_args_list)
    assert SECRET not in result.receipt_path.read_text()


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308, 400, 401, 403, 404, 410, 501])
def test_permanent_http_errors_and_redirects_never_retry(
    tmp_path: Path, transport: Any, status: int
) -> None:
    manifest = _manifest()
    failure = _http(manifest["objects"][0], status)
    opener, pause = transport
    opener.open.side_effect = failure
    with pytest.raises(HandoffError) as error:
        _ingest(tmp_path, manifest)
    assert opener.open.call_count == 1 and not pause.called
    assert failure.fp.closed and SECRET not in str(error.value)
    assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError(SECRET),
        URLError(TimeoutError(SECRET)),
        ConnectionResetError(SECRET),
        URLError(ConnectionRefusedError(SECRET)),
        RemoteDisconnected(SECRET),
        URLError(socket.gaierror(socket.EAI_AGAIN, SECRET)),
    ],
)
def test_temporary_transport_errors_retry(
    tmp_path: Path, transport: Any, failure: Exception
) -> None:
    manifest = _manifest()
    opener, pause = transport
    opener.open.side_effect = [failure, Response(manifest["objects"][0])]
    assert _ingest(tmp_path, manifest).files_published == 1
    pause.assert_called_once_with(1)


@pytest.mark.parametrize(
    "failure",
    [
        URLError(ssl.SSLCertVerificationError(SECRET)),
        ssl.SSLError(SECRET),
        PermissionError(SECRET),
        OSError(SECRET),
        URLError(SECRET),
        URLError(socket.gaierror(socket.EAI_NONAME, SECRET)),
    ],
)
def test_tls_configuration_and_permission_failures_do_not_retry(
    tmp_path: Path, transport: Any, failure: Exception
) -> None:
    opener, pause = transport
    opener.open.side_effect = failure
    with pytest.raises(HandoffError) as error:
        _ingest(tmp_path, _manifest())
    assert opener.open.call_count == 1 and not pause.called
    assert SECRET not in str(error.value)


@pytest.mark.parametrize("attempts", [1, 3, 5])
def test_attempt_cap_and_exponential_waits_have_no_final_sleep(
    tmp_path: Path, transport: Any, attempts: int
) -> None:
    opener, pause = transport
    opener.open.side_effect = TimeoutError(SECRET)
    with pytest.raises(HandoffError):
        registry_fetch.ingest_registry_fetch(
            manifest_path=_write(tmp_path, _manifest()),
            options=replace(OPTIONS, max_attempts=attempts, retry_delay_seconds=10),
            store=LocalBlobStore(tmp_path / "objects"),
            receipt_dir=tmp_path / "receipts",
        )
    assert opener.open.call_count == attempts
    assert pause.call_args_list == [call(delay) for delay in [10, 20, 30, 30][: attempts - 1]]
    assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize(
    "header,expected", [(None, 1), ("0", 1), ("7", 7), (" 0002 ", 2), ("invalid", 1), ("-1", 1)]
)
def test_retry_after_seconds_or_invalid_header(
    tmp_path: Path, transport: Any, header: str | None, expected: int
) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    opener, pause = transport
    opener.open.side_effect = [_http(item, 429, header), Response(item)]
    _ingest(tmp_path, manifest)
    pause.assert_called_once_with(expected)


@pytest.mark.parametrize("seconds,expected", [(12, 12), (-12, 1)])
def test_retry_after_http_date(tmp_path: Path, transport: Any, seconds: int, expected: int) -> None:
    now = datetime(2026, 11, 11, tzinfo=UTC)
    manifest = _manifest()
    item = manifest["objects"][0]
    opener, pause = transport
    opener.open.side_effect = [
        _http(item, 503, format_datetime(now + timedelta(seconds=seconds))),
        Response(item),
    ]
    with patch.object(registry_fetch, "datetime") as clock:
        clock.now.return_value = now
        _ingest(tmp_path, manifest)
    pause.assert_called_once_with(expected)


@pytest.mark.parametrize("header", ["31", "9" * 5000, "Wed, 11 Nov 2099 12:00:00 GMT"])
def test_server_wait_beyond_cap_stops_without_retrying_early(
    tmp_path: Path, transport: Any, header: str
) -> None:
    manifest = _manifest()
    failure = _http(manifest["objects"][0], 503, header)
    opener, pause = transport
    opener.open.side_effect = failure
    with pytest.raises(HandoffError, match="wait limit"):
        _ingest(tmp_path, manifest)
    assert opener.open.call_count == 1 and not pause.called and failure.fp.closed


@pytest.mark.parametrize("failure", ["incomplete", "timeout", "short"])
def test_partial_transfer_is_discarded_and_counted_in_shared_budget(
    tmp_path: Path, transport: Any, failure: str
) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    partial = Response(item, **{"Content-Length": str(len(PAYLOAD))})
    if failure == "incomplete":
        partial.read = Mock(side_effect=IncompleteRead(PAYLOAD[:10]))
    else:
        partial.read = Mock(
            side_effect=[PAYLOAD[:10], TimeoutError(SECRET) if failure == "timeout" else b""]
        )
    opener, pause = transport
    opener.open.side_effect = [partial, Response(item)]
    result = _ingest(tmp_path, manifest, max_total_bytes=len(PAYLOAD) + 10)
    assert _receipt(result)["fetch_statistics"]["downloaded_bytes"] == len(PAYLOAD) + 10
    assert result.bytes_verified == len(PAYLOAD) and partial.closed
    assert (tmp_path / "objects" / item["storage_key"]).read_bytes() == PAYLOAD
    pause.assert_called_once_with(1)


@pytest.mark.parametrize("known_size", [True, False])
def test_exhausted_budget_cannot_restart_transfer(
    tmp_path: Path, transport: Any, known_size: bool
) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    if not known_size:
        item["size_bytes"] = None
    partial = Response(item)
    consumed = 1 if known_size else len(PAYLOAD)
    partial.read = Mock(side_effect=IncompleteRead(PAYLOAD[:consumed]))
    opener, pause = transport
    opener.open.side_effect = [partial, Response(item)]
    with pytest.raises(HandoffError, match="remaining fetch byte budget"):
        _ingest(tmp_path, manifest, max_total_bytes=len(PAYLOAD))
    assert opener.open.call_count == 1 and not pause.called and partial.closed
    assert not list((tmp_path / "objects").rglob("*.csv"))


@pytest.mark.parametrize("failure", ["hash", "encoding", "length", "long", "url", "overflow"])
def test_integrity_failures_never_retry(tmp_path: Path, transport: Any, failure: str) -> None:
    manifest = _manifest()
    item = manifest["objects"][0]
    body = Response(item, b"corrupt" if failure == "hash" else PAYLOAD)
    options = {}
    if failure == "encoding":
        body.headers["Content-Encoding"] = "gzip"
    elif failure == "length":
        body.headers["Content-Length"] = str(len(PAYLOAD) + 1)
    elif failure == "long":
        item["size_bytes"] = None
        body.headers["Content-Length"] = str(len(PAYLOAD) - 1)
    elif failure == "url":
        body.url = SECRET
    elif failure == "overflow":
        item["size_bytes"] = None
        body.read = Mock(side_effect=IncompleteRead(PAYLOAD))
        options["max_object_bytes"] = len(PAYLOAD) - 1
    opener, pause = transport
    opener.open.return_value = body
    with pytest.raises(HandoffError):
        _ingest(tmp_path, manifest, **options)
    assert opener.open.call_count == 1 and not pause.called
    assert body.closed and not (tmp_path / "receipts").exists()


def test_successful_first_object_is_not_fetched_again_when_second_retries(
    tmp_path: Path, transport: Any
) -> None:
    manifest = _manifest()
    one = manifest["objects"][0]
    two = one | {key: one[key] + "2" for key in ("source_path", "source_uri", "storage_key")}
    manifest["objects"].append(two)
    opener, pause = transport
    opener.open.side_effect = [Response(one), _http(two, 503), Response(two)]
    result = _ingest(tmp_path, manifest, max_total_bytes=2 * len(PAYLOAD))
    assert result.files_published == 2
    assert _receipt(result)["fetch_statistics"] == {
        "attempts": 3,
        "retries": 1,
        "downloaded_bytes": 2 * len(PAYLOAD),
    }
    assert [c.args[0].full_url for c in opener.open.call_args_list] == [
        one["source_uri"],
        two["source_uri"],
        two["source_uri"],
    ]
    pause.assert_called_once_with(1)


def test_storage_connection_errors_are_not_retried(tmp_path: Path, transport: Any) -> None:
    manifest = _manifest()
    opener, pause = transport
    opener.open.return_value = Response(manifest["objects"][0])
    with (
        patch.object(LocalBlobStore, "put_bytes", side_effect=ConnectionResetError(SECRET)) as put,
        pytest.raises(ConnectionResetError),
    ):
        _ingest(tmp_path, manifest)
    assert put.call_count == opener.open.call_count == 1 and not pause.called
    assert not (tmp_path / "receipts").exists()


@pytest.mark.parametrize(
    "changes",
    [{"max_attempts": value} for value in (0, -1, 6, True, 1.5, "3")]
    + [
        {"retry_delay_seconds": value}
        for value in (0, -1, 31, float("nan"), float("inf"), True, "1")
    ],
)
def test_bad_retry_policy_fails_before_transport(tmp_path: Path, changes: dict[str, Any]) -> None:
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
        pytest.raises(HandoffError),
    ):
        registry_fetch.plan_registry_fetch(
            manifest_path=_write(tmp_path, _manifest()), options=replace(OPTIONS, **changes)
        )


def _cli_args(tmp_path: Path, command: str) -> list[str]:
    path = _write(tmp_path, _manifest())
    if command == "registry-ingest":
        return [
            *_args(path),
            "--root",
            str(tmp_path / "objects"),
            "--receipt-dir",
            str(tmp_path / "receipts"),
        ]
    return [
        command,
        "--fetch-source",
        str(path),
        OPTIONS.commit,
        "--object-root",
        str(tmp_path / "objects"),
        "--output-root",
        str(tmp_path / "curated"),
    ]


@pytest.mark.parametrize("command", ["registry-ingest", "ons-pay-refresh"])
def test_both_clis_apply_retry_policy_and_preserve_receipts(
    tmp_path: Path, capsys: Any, transport: Any, command: str
) -> None:
    item = _manifest()["objects"][0]
    opener, pause = transport
    opener.open.side_effect = [_http(item, 503), Response(item)]
    platform_main(
        [*_cli_args(tmp_path, command), "--max-attempts", "2", "--retry-delay-seconds", "0.5"]
    )
    summary = json.loads(capsys.readouterr().out)
    receipt = json.loads(Path(summary["receipt_path"]).read_bytes())
    if command == "ons-pay-refresh":
        assert summary["parquet_published"] == 3
        receipt = json.loads(Path(receipt["sources"][0]["ingestion_receipt_path"]).read_bytes())
    else:
        assert not list(tmp_path.rglob("*.parquet"))
    assert receipt["fetch_statistics"]["retries"] == 1
    pause.assert_called_once_with(0.5)


@pytest.mark.parametrize("command", ["registry-ingest", "ons-pay-refresh"])
def test_retry_preview_performs_no_network_or_wait_or_writes(
    tmp_path: Path, capsys: Any, transport: Any, command: str
) -> None:
    opener, pause = transport
    with (
        patch.object(registry_cli, "_store", side_effect=AssertionError),
        patch.object(ons_pay_refresh_cli, "LocalBlobStore", side_effect=AssertionError),
    ):
        platform_main([*_cli_args(tmp_path, command), "--max-attempts", "4", "--dry-run"])
    result = json.loads(capsys.readouterr().out)
    policy = result["vintages"][0] if command == "ons-pay-refresh" else result
    assert policy["max_attempts"] == 4 and policy["retry_delay_seconds"] == 1
    assert not opener.open.called and not pause.called
    assert not (tmp_path / "objects").exists() and not (tmp_path / "curated").exists()


@pytest.mark.parametrize("command", ["registry-ingest", "ons-pay-refresh"])
@pytest.mark.parametrize(
    "flag,value",
    [("--max-attempts", "6"), ("--retry-delay-seconds", "nan"), ("--retry-delay-seconds", SECRET)],
)
def test_cli_bad_retry_policy_is_rejected_and_sanitized(
    tmp_path: Path, capsys: Any, transport: Any, command: str, flag: str, value: str
) -> None:
    opener, pause = transport
    with pytest.raises(SystemExit) as error:
        platform_main([*_cli_args(tmp_path, command), flag, value, "--dry-run"])
    assert error.value.code == 2 and not opener.open.called and not pause.called
    output = capsys.readouterr()
    assert SECRET not in output.out + output.err


@pytest.mark.parametrize("command", ["registry-ingest", "ons-pay-refresh"])
@pytest.mark.parametrize("flag", ["--max-attempts", "--retry-delay-seconds"])
def test_retry_flags_cannot_change_checkout_mode(tmp_path: Path, command: str, flag: str) -> None:
    args = (
        [command, "--source", "unused", "unused"]
        if command == "ons-pay-refresh"
        else [command, "--manifest", "unused", "--registry-root", "unused"]
    )
    with (
        patch.object(registry_cli, "_store", side_effect=AssertionError),
        patch.object(ons_pay_refresh_cli, "LocalBlobStore", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*args, flag, "2"])
    assert error.value.code == 2


def test_real_http_handler_retries_service_unavailable() -> None:
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            requests.append(self.path)
            self.send_response(503 if len(requests) == 1 else 200)
            self.send_header("Content-Length", "0" if len(requests) == 1 else str(len(PAYLOAD)))
            self.send_header("Retry-After", "2")
            self.end_headers()
            if len(requests) > 1:
                self.wfile.write(PAYLOAD)

        def log_message(self, *args: Any) -> None:
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            # Exercise urllib's HTTPError path; public manifests still require HTTPS.
            item = replace(
                parse_handoff(_manifest()).objects[0],
                source_uri=f"http://127.0.0.1:{server.server_port}/source",
            )
            reader = registry_fetch._HttpReader(replace(OPTIONS, max_attempts=2))
            with patch.object(registry_fetch, "sleep") as pause:
                assert reader(item) == PAYLOAD
            assert requests == ["/source", "/source"]
            assert reader.statistics == {
                "attempts": 2,
                "retries": 1,
                "downloaded_bytes": len(PAYLOAD),
            }
            pause.assert_called_once_with(2)
        finally:
            server.shutdown()
            thread.join(timeout=2)


def test_exhausted_retry_keeps_earlier_vintage_and_explicit_replay_recovers(
    tmp_path: Path, transport: Any
) -> None:
    first, one = remote_source(tmp_path)
    second, two = remote_source(tmp_path, "b")
    second = replace(second, fetch_options=replace(second.fetch_options, max_attempts=3))
    opener, pause = transport
    opener.open.side_effect = [
        Response(one["objects"][0]),
        *[_http(two["objects"][0], 503) for _ in range(3)],
    ]
    kwargs = {
        "sources": [first, second],
        "store": LocalBlobStore(tmp_path / "objects"),
        "output_root": tmp_path / "curated",
    }
    with pytest.raises(RefreshError) as error:
        refresh_ons_pay(**kwargs)
    audit_path = error.value.receipt_path
    assert audit_path is not None
    before = audit_path.read_bytes()
    audit = json.loads(before)
    assert audit["status"] == "failed" and audit["metrics"]["vintages_completed"] == 1
    assert audit["failure"]["phase"] == "ingestion" and audit["failure"]["source_index"] == 1
    assert opener.open.call_count == 4 and pause.call_args_list == [call(1), call(2)]
    assert SECRET not in before.decode()
    opener.open.side_effect = [Response(one["objects"][0]), Response(two["objects"][0])]
    result = refresh_ons_pay(**kwargs)
    assert result.sources[0].objects_reused == 1 and result.sources[0].parquet_reused == 3
    # Equal raw bytes are shared across registry commits; curated vintages remain distinct.
    assert result.sources[1].objects_reused == 1 and result.sources[1].parquet_published == 3
    assert audit_path.read_bytes() == before


def test_http_failure_from_different_url_cannot_trigger_retry(
    tmp_path: Path, transport: Any
) -> None:
    manifest = _manifest()
    opener, pause = transport
    failure = HTTPError(SECRET, 503, SECRET, Message(), BytesIO())
    opener.open.side_effect = failure
    with pytest.raises(HandoffError):
        _ingest(tmp_path, manifest)
    assert opener.open.call_count == 1 and not pause.called and failure.fp.closed


def test_known_empty_object_can_follow_an_exactly_consumed_budget(
    tmp_path: Path, transport: Any
) -> None:
    manifest = _manifest()
    empty = _manifest(b"")["objects"][0]
    empty = empty | {key: empty[key] + "2" for key in ("source_path", "source_uri", "storage_key")}
    manifest["objects"].append(empty)
    opener, pause = transport
    opener.open.side_effect = [Response(manifest["objects"][0]), Response(empty, b"")]
    result = _ingest(tmp_path, manifest, max_total_bytes=len(PAYLOAD))
    assert result.files_published == 2 and result.bytes_verified == len(PAYLOAD)
    assert (tmp_path / "objects" / empty["storage_key"]).read_bytes() == b""
    assert opener.open.call_count == 2 and not pause.called
