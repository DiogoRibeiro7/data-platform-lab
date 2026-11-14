"""S3-backed ONS loads, saved configuration, deferred models and safe previews."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch

import pytest
from test_ons_pay_refresh import _source as local_source
from test_ons_pay_refresh_job import _job
from test_ons_pay_remote_refresh import _source
from test_registry_fetch import PAYLOAD, SECRET, Response
from test_storage_s3 import FakeS3Client

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability.ons_pay_job_status import inspect_ons_pay_job
from data_platform_lab.orchestration import ons_pay_refresh_cli as refresh_cli
from data_platform_lab.orchestration import ons_pay_transform_cli as transform_cli
from data_platform_lab.orchestration.ons_pay_job_preparation import (
    prepare_refresh_job,
    save_refresh_job,
)
from data_platform_lab.orchestration.ons_pay_refresh_job import RefreshJobError, load_refresh_job
from data_platform_lab.storage import S3BlobStore
from data_platform_lab.storage.config import S3Storage, StorageConfigurationError

CONFIG = S3Storage("platform", "http://localhost:3900", "garage", "ons/raw")


def _storage_args() -> list[str]:
    return [
        "--backend",
        "s3",
        "--bucket",
        CONFIG.bucket,
        "--endpoint-url",
        str(CONFIG.endpoint_url),
        "--region",
        str(CONFIG.region),
        "--key-prefix",
        CONFIG.key_prefix,
    ]


def _args(tmp_path: Path, source: Any, *, load_only: bool = False) -> list[str]:
    return [
        "ons-pay-refresh",
        "--fetch-source",
        str(source.manifest_path),
        source.fetch_options.commit,
        "--output-root",
        str(tmp_path / "curated"),
        "--reuse-verified",
        *_storage_args(),
        *(["--load-only"] if load_only else []),
    ]


def _json(path: Any) -> Any:
    return json.loads(Path(path).read_bytes())


@pytest.fixture
def s3() -> Any:
    client = FakeS3Client()
    store = S3BlobStore(client, bucket=CONFIG.bucket, key_prefix=CONFIG.key_prefix)
    with patch.object(S3BlobStore, "from_boto3", return_value=store) as factory:
        yield client, store, factory


@pytest.mark.parametrize("load_only", [False, True])
def test_direct_s3_refresh_replay_and_deferred_transform_preserve_lineage(
    tmp_path: Path,
    capsys: Any,
    s3: Any,
    load_only: bool,
) -> None:
    source, manifest = _source(tmp_path)
    client, store, factory = s3
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        patch.object(refresh_cli, "LocalBlobStore", side_effect=AssertionError("local store")),
    ):
        platform_main(_args(tmp_path, source, load_only=load_only))
    first = json.loads(capsys.readouterr().out)
    assert first["objects_published"] == 1
    key = manifest["objects"][0]["storage_key"]
    assert client.objects[(CONFIG.bucket, f"{CONFIG.key_prefix}/{key}")] == PAYLOAD
    before = dict(client.objects)
    parquet_before = {p: p.stat().st_mtime_ns for p in tmp_path.rglob("*.parquet")}
    with (
        patch.object(registry_fetch, "build_opener", side_effect=AssertionError("source HTTP")),
        patch.object(client, "put_object", side_effect=AssertionError("raw rewrite")),
    ):
        platform_main(_args(tmp_path, source, load_only=load_only))
    replay = json.loads(capsys.readouterr().out)
    assert replay["objects_reused"] == 1 and replay["objects_published"] == 0
    audit = _json(replay["receipt_path"])
    receipt = _json(audit["sources"][0]["ingestion_receipt_path"])
    assert (
        receipt["objects"][0]["destination_uri"]
        == f"s3://{CONFIG.bucket}/{CONFIG.key_prefix}/{key}"
    )
    assert receipt["fetch_statistics"]["attempts"] == 0
    assert client.objects == before and not list(tmp_path.rglob("*.csv"))
    assert not (tmp_path / "objects").exists()
    factory.assert_called_with(
        bucket=CONFIG.bucket,
        endpoint_url=CONFIG.endpoint_url,
        region_name=CONFIG.region,
        key_prefix=CONFIG.key_prefix,
    )
    if load_only:
        assert not parquet_before and "model_version" not in audit
        source.manifest_path.unlink()
        args = [
            "ons-pay-transform",
            "--load-audit",
            replay["receipt_path"],
            "--output-root",
            str(tmp_path / "transformed"),
            *_storage_args(),
        ]
        with (
            patch.object(registry_fetch, "build_opener", side_effect=AssertionError),
            patch.object(transform_cli, "LocalBlobStore", side_effect=AssertionError),
            patch.object(client, "put_object", side_effect=AssertionError),
        ):
            platform_main(args)
            transformed = json.loads(capsys.readouterr().out)
            platform_main(args)
            transformed_replay = json.loads(capsys.readouterr().out)
        assert transformed["source_rows"] == 7 and transformed["parquet_published"] == 3
        assert transformed_replay["parquet_reused"] == 3
        assert store.get_bytes(key) == PAYLOAD
    else:
        assert replay["parquet_reused"] == 3
        assert {p: p.stat().st_mtime_ns for p in parquet_before} == parquet_before


@pytest.mark.parametrize("load_only", [False, True])
def test_s3_job_preparation_is_read_only_and_replays_from_another_directory(
    tmp_path: Path,
    capsys: Any,
    monkeypatch: Any,
    s3: Any,
    load_only: bool,
) -> None:
    source, manifest = _source(tmp_path)
    path = tmp_path / "jobs" / "refresh.json"
    args = [*_args(tmp_path, source, load_only=load_only), "--save-job", str(path)]
    before = set(tmp_path.rglob("*"))
    with patch.object(S3Storage, "open", side_effect=AssertionError("S3 opened")):
        platform_main([*args, "--dry-run"])
        preview = json.loads(capsys.readouterr().out)
        assert set(tmp_path.rglob("*")) == before
        platform_main(args)
        saved = json.loads(capsys.readouterr().out)
        platform_main(["ons-pay-refresh", "--job", str(path), "--dry-run"])
        execution_plan = json.loads(capsys.readouterr().out)
    assert preview["job_sha256"] == saved["job_sha256"]
    assert saved["job"]["object_store"] == execution_plan["object_store"] == CONFIG.to_dict()
    assert "object_root" not in saved["job"] and "object_root" not in execution_plan
    job = load_refresh_job(path)
    assert job.object_root is None and job.object_store == CONFIG
    assert not (tmp_path / "curated").exists()
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        platform_main(["ons-pay-refresh", "--job", str(path)])
    result = json.loads(capsys.readouterr().out)
    assert result["job_sha256"] == saved["job_sha256"]
    with patch.object(S3Storage, "open", side_effect=AssertionError):
        status = inspect_ons_pay_job(job_path=path)
    assert status.status == "success" and not list(elsewhere.iterdir())
    assert "AWS_ACCESS_KEY_ID" not in path.read_text()


def test_s3_preparation_preserves_mixed_sources_and_protects_checkouts(tmp_path: Path) -> None:
    remote, _ = _source(tmp_path)
    local = local_source(tmp_path)
    options = dict(
        sources=[remote, local],
        object_root=None,
        object_store=CONFIG,
        output_root=tmp_path / "curated",
        load_only=True,
    )
    prepared = prepare_refresh_job(path=tmp_path / "job.json", **options)
    save_refresh_job(prepared)
    job = load_refresh_job(prepared.path)
    assert job.object_root is None and job.object_store == CONFIG
    assert [item["transport"] for item in json.loads(prepared.content)["sources"]] == [
        "https",
        "checkout",
    ]
    with pytest.raises(RefreshJobError, match="outside canonical"):
        prepare_refresh_job(path=local.registry_root / "job.json", **options)
    with pytest.raises(RefreshJobError, match="outside canonical"):
        prepare_refresh_job(
            path=tmp_path / "bad-job.json", **(options | {"output_root": local.registry_root})
        )
    with pytest.raises(RefreshJobError, match="exactly one"):
        prepare_refresh_job(
            path=tmp_path / "bad-job.json", **(options | {"object_root": tmp_path / "objects"})
        )


@pytest.mark.parametrize(
    "change",
    [
        {"bucket": ""},
        {"bucket": None},
        {"bucket": SECRET},
        {"bucket": "x"},
        {"region": "bad region"},
        {"region": 1},
        {"key_prefix": "../escape"},
        {"key_prefix": "/absolute"},
        {"key_prefix": "space "},
        {"key_prefix": "a//b"},
        {"key_prefix": "x\nsecret"},
        {"key_prefix": None},
        {"endpoint_url": SECRET},
        {"endpoint_url": "https://example.org/?token=secret"},
        {"endpoint_url": "https://example.org/#secret"},
        {"endpoint_url": "ftp://example.org"},
        {"endpoint_url": "https://example.org/path"},
        {"endpoint_url": "https://example.org:99999"},
        {"endpoint_url": "https://example.org\n"},
    ],
)
def test_invalid_location_configuration_is_rejected_without_echoing_values(change: Any) -> None:
    with pytest.raises(StorageConfigurationError) as error:
        S3Storage(**({"bucket": "platform"} | change))
    assert SECRET not in str(error.value) and "secret" not in str(error.value)


@pytest.mark.parametrize(
    "extra",
    [
        ["--backend", "s3"],
        ["--bucket", "platform"],
        ["--backend", "local", "--region", "garage"],
        ["--backend", "s3", "--bucket", "platform", "--object-root", "objects"],
        ["--backend", "s3", "--bucket", "platform", "--endpoint-url", SECRET],
    ],
)
def test_cli_rejects_ambiguous_or_invalid_storage_before_client_construction(
    tmp_path: Path,
    capsys: Any,
    extra: list[str],
) -> None:
    source, _ = _source(tmp_path)
    args = [
        "ons-pay-refresh",
        "--fetch-source",
        str(source.manifest_path),
        source.fetch_options.commit,
    ]
    with (
        patch.object(S3Storage, "open", side_effect=AssertionError),
        pytest.raises(SystemExit) as error,
    ):
        platform_main([*args, *extra, "--dry-run"])
    output = capsys.readouterr()
    assert error.value.code == 2 and not output.out and SECRET not in output.err


@pytest.mark.parametrize(
    "field,value",
    [
        ("access_key_id", SECRET),
        ("secret_access_key", SECRET),
        ("backend", "local"),
        ("bucket", None),
        ("endpoint_url", SECRET),
        ("key_prefix", "../escape"),
    ],
)
def test_job_rejects_credentials_or_invalid_s3_locations(
    tmp_path: Path, field: str, value: Any
) -> None:
    path, document, _ = _job(tmp_path)
    del document["object_root"]
    document["object_store"] = CONFIG.to_dict() | {field: value}
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RefreshJobError) as error:
        load_refresh_job(path)
    assert SECRET not in str(error.value)


@pytest.mark.parametrize(
    "extra", [["--backend", "local"], ["--bucket", "platform"], ["--key-prefix", ""]]
)
def test_saved_jobs_reject_all_storage_overrides(
    tmp_path: Path, capsys: Any, extra: list[str]
) -> None:
    path, _, _ = _job(tmp_path)
    with pytest.raises(SystemExit) as error:
        platform_main(["ons-pay-refresh", "--job", str(path), *extra])
    assert error.value.code == 2 and "cannot be overridden" in capsys.readouterr().err


@pytest.mark.parametrize("phase", ["open", "read", "write"])
def test_storage_failures_have_safe_errors_and_no_success_audit(
    tmp_path: Path,
    capsys: Any,
    s3: Any,
    phase: str,
) -> None:
    source, manifest = _source(tmp_path)
    client, _, factory = s3
    if phase == "open":
        factory.side_effect = RuntimeError(SECRET)
    elif phase == "read":
        client.head_object = Mock(side_effect=RuntimeError(SECRET))
    else:
        client.put_object = Mock(side_effect=RuntimeError(SECRET))
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with (
        patch.object(registry_fetch, "build_opener", return_value=opener),
        pytest.raises(SystemExit) as error,
    ):
        platform_main(_args(tmp_path, source, load_only=True))
    output = capsys.readouterr()
    assert error.value.code == 1 and SECRET not in output.err and not output.out
    audits = list((tmp_path / "curated").rglob("ons_pay_load_*.json"))
    assert len(audits) == (0 if phase == "open" else 1)
    for audit in audits:
        assert _json(audit)["status"] == "failed" and SECRET not in audit.read_text()


def test_transform_preview_and_storage_initialization_failure_are_safe(
    tmp_path: Path, capsys: Any, s3: Any
) -> None:
    source, manifest = _source(tmp_path)
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        platform_main(_args(tmp_path, source, load_only=True))
    load = json.loads(capsys.readouterr().out)
    args = [
        "ons-pay-transform",
        "--load-audit",
        load["receipt_path"],
        "--output-root",
        str(tmp_path / "transformed"),
        *_storage_args(),
    ]
    with patch.object(S3Storage, "open", side_effect=AssertionError):
        platform_main([*args, "--dry-run"])
    preview = json.loads(capsys.readouterr().out)
    assert preview["object_store"] == CONFIG.to_dict() and "object_root" not in preview
    with (
        patch.object(S3Storage, "open", side_effect=RuntimeError(SECRET)),
        pytest.raises(SystemExit) as error,
    ):
        platform_main(args)
    assert error.value.code == 1 and SECRET not in capsys.readouterr().err
    assert not (tmp_path / "transformed").exists()
