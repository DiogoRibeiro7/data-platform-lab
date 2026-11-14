"""Opt-in ONS raw storage against a disposable local Garage S3 service."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import Mock, patch
from urllib.parse import urlsplit
from uuid import uuid4

import pytest
from test_ons_pay_remote_refresh import _source
from test_registry_fetch import PAYLOAD, Response

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion import registry_fetch
from data_platform_lab.observability.ons_pay_job_status import inspect_ons_pay_job
from data_platform_lab.storage.config import S3Storage

pytestmark = pytest.mark.skipif(
    os.environ.get("ONS_PAY_LIVE_S3") != "1",
    reason="requires explicit opt-in and disposable local Garage storage",
)


@pytest.fixture
def garage() -> Iterator[Any]:
    """Use a random prefix, reject hosted endpoints and remove only owned test keys."""
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("DPL_S3_ENDPOINT_URL", "")
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"localhost", "127.0.0.1", "garage"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        pytest.fail("live S3 tests require a credential-free local Garage endpoint")
    config = S3Storage(
        bucket="data-platform-lab",
        endpoint_url=endpoint,
        region=os.environ.get("AWS_DEFAULT_REGION", "garage"),
        key_prefix=f"tests/ons-pay/{uuid4().hex}",
    )
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=config.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    keys: list[str] = []
    try:
        client.head_bucket(Bucket=config.bucket)
        yield config, client, keys
    finally:
        for key in keys:
            assert key.startswith(config.key_prefix + "/")
            client.delete_object(Bucket=config.bucket, Key=key)
        client.close()


@pytest.mark.parametrize("load_only", [False, True])
def test_live_s3_saved_job_replay_and_deferred_transform(
    tmp_path: Path,
    capsys: Any,
    garage: Any,
    load_only: bool,
) -> None:
    config, client, keys = garage
    source, manifest = _source(tmp_path)
    storage_args = [
        "--backend",
        "s3",
        "--bucket",
        config.bucket,
        "--endpoint-url",
        config.endpoint_url,
        "--region",
        config.region,
        "--key-prefix",
        config.key_prefix,
    ]
    job = tmp_path / "jobs" / "ons.json"
    platform_main(
        [
            "ons-pay-refresh",
            "--fetch-source",
            str(source.manifest_path),
            source.fetch_options.commit,
            "--output-root",
            str(tmp_path / "curated"),
            "--reuse-verified",
            "--save-job",
            str(job),
            *storage_args,
            *(["--load-only"] if load_only else []),
        ]
    )
    saved = json.loads(capsys.readouterr().out)
    assert saved["job"]["object_store"] == config.to_dict()
    key = config.key_prefix + "/" + manifest["objects"][0]["storage_key"]
    keys.append(key)
    opener = Mock()
    opener.open.return_value = Response(manifest["objects"][0])
    with patch.object(registry_fetch, "build_opener", return_value=opener):
        platform_main(["ons-pay-refresh", "--job", str(job)])
    first = json.loads(capsys.readouterr().out)
    assert first["objects_published"] == 1
    body = client.get_object(Bucket=config.bucket, Key=key)["Body"]
    try:
        assert body.read() == PAYLOAD
    finally:
        body.close()
    original = client.head_object(Bucket=config.bucket, Key=key)
    with patch.object(registry_fetch, "build_opener", side_effect=AssertionError("source HTTP")):
        platform_main(["ons-pay-refresh", "--job", str(job)])
    replay = json.loads(capsys.readouterr().out)
    assert replay["objects_reused"] == 1 and replay["objects_published"] == 0
    after = client.head_object(Bucket=config.bucket, Key=key)
    assert (after["ETag"], after["LastModified"]) == (original["ETag"], original["LastModified"])
    audit = json.loads(Path(replay["receipt_path"]).read_bytes())
    receipt = json.loads(Path(audit["sources"][0]["ingestion_receipt_path"]).read_bytes())
    assert receipt["objects"][0]["destination_uri"] == f"s3://{config.bucket}/{key}"
    assert receipt["fetch_statistics"]["attempts"] == 0
    assert not list(tmp_path.rglob("*.csv"))
    assert inspect_ons_pay_job(job_path=job).status == "success"
    if load_only:
        assert not list(tmp_path.rglob("*.parquet"))
        with patch.object(registry_fetch, "build_opener", side_effect=AssertionError):
            platform_main(
                [
                    "ons-pay-transform",
                    "--load-audit",
                    replay["receipt_path"],
                    "--output-root",
                    str(tmp_path / "transformed"),
                    *storage_args,
                ]
            )
        transformed = json.loads(capsys.readouterr().out)
        assert transformed["source_rows"] == 7 and transformed["parquet_published"] == 3
    else:
        assert replay["parquet_reused"] == 3
