"""Offline integration tests for the registry-to-object-store trust boundary."""

from __future__ import annotations

import hashlib
import json
import subprocess
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.ingestion.registry_handoff import (
    HandoffError,
    ingest_handoff,
    parse_handoff,
)
from data_platform_lab.storage import LocalBlobStore, S3BlobStore

DATASET_ID = "fixture-events"
SOURCE = f"datasets/{DATASET_ID}/raw/events.csv"
PAYLOAD = b"event_id,value\n1,10\n2,20\n"


def _git(*args: str, root: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args], check=True, text=True, capture_output=True
    )
    return result.stdout.strip()


@pytest.fixture
def registry(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    """A real local Git checkout with a single synthetic canonical CSV."""
    root = tmp_path / "registry"
    source = root / SOURCE
    source.parent.mkdir(parents=True)
    source.write_bytes(PAYLOAD)
    _git("init", "-q", root=root)
    _git("add", ".", root=root)
    _git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.org",
        "commit",
        "-qm",
        "fixture",
        root=root,
    )
    commit = _git("rev-parse", "HEAD", root=root)
    digest = hashlib.sha256(PAYLOAD).hexdigest()
    repository = "DiogoRibeiro7/data"
    manifest: dict[str, Any] = {
        "manifest_version": 1,
        "dataset_id": DATASET_ID,
        "title": "Synthetic events",
        "lifecycle_status": "active",
        "registry": {"repository": repository, "commit": commit},
        "source": {
            "publisher": "Fixture",
            "url": "https://example.org/data",
            "snapshot": "2026-10",
            "retrieved_at": "2026-10-31",
        },
        "license": {
            "name": "CC0",
            "url": "https://creativecommons.org/publicdomain/zero/1.0/",
            "redistribution": "allowed",
        },
        "citation": "Fixture, Synthetic events (2026)",
        "local_bytes_verified": True,
        "objects": [
            {
                "source_path": SOURCE,
                "source_uri": f"https://raw.githubusercontent.com/{repository}/{commit}/{SOURCE}",
                "sha256": digest,
                "role": "raw",
                "format": "csv",
                "storage_key": f"canonical/{DATASET_ID}/{digest}/raw/events.csv",
                "size_bytes": len(PAYLOAD),
            }
        ],
    }
    return root, manifest


def _write_manifest(path: Path, manifest: dict[str, Any]) -> Path:
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


def test_ingest_creates_verified_object_and_lineage_receipt(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    manifest_path = _write_manifest(tmp_path / "handoff.json", manifest)
    store = LocalBlobStore(tmp_path / "objects")
    result = ingest_handoff(
        manifest_path=manifest_path,
        registry_root=root,
        store=store,
        receipt_dir=tmp_path / "receipts",
    )
    assert result.files_published == 1
    assert result.files_reused == 0
    assert result.bytes_verified == len(PAYLOAD)
    key = manifest["objects"][0]["storage_key"]
    assert store.get_bytes(key) == PAYLOAD
    receipt = json.loads(result.receipt_path.read_text())
    assert receipt["status"] == "success"
    assert receipt["manifest_sha256"] == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert receipt["registry"] == manifest["registry"]
    assert receipt["citation"] == manifest["citation"]
    assert receipt["objects"][0]["sha256"] == manifest["objects"][0]["sha256"]
    assert receipt["objects"][0]["destination_uri"] == (store.root / key).as_uri()
    assert receipt["objects"][0]["status"] == "published"


def test_rerun_reuses_verified_object_without_overwrite(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    store = LocalBlobStore(tmp_path / "objects")
    receipts = tmp_path / "receipts"
    first = ingest_handoff(
        manifest_path=handoff, registry_root=root, store=store, receipt_dir=receipts
    )
    second = ingest_handoff(
        manifest_path=handoff, registry_root=root, store=store, receipt_dir=receipts
    )
    assert second.files_published == 0
    assert second.files_reused == 1
    assert second.receipt_path != first.receipt_path
    assert json.loads(second.receipt_path.read_text())["objects"][0]["status"] == "reused"


def test_s3_compatible_store_uses_the_same_ingestion_contract(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    class MissingObject(Exception):
        def __init__(self) -> None:
            super().__init__("NoSuchKey")
            self.response = {"Error": {"Code": "NoSuchKey"}}

    class MemoryS3:
        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}

        def head_object(self, **kwargs: object) -> dict[str, object]:
            if str(kwargs["Key"]) not in self.objects:
                raise MissingObject
            return {}

        def put_object(self, **kwargs: object) -> dict[str, object]:
            payload = kwargs["Body"]
            assert isinstance(payload, bytes)
            self.objects[str(kwargs["Key"])] = payload
            return {}

        def get_object(self, **kwargs: object) -> dict[str, object]:
            return {"Body": BytesIO(self.objects[str(kwargs["Key"])])}

        def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
            return {}

    root, manifest = registry
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    client = MemoryS3()
    store = S3BlobStore(client, bucket="demo", key_prefix="registry")
    result = ingest_handoff(
        manifest_path=handoff,
        registry_root=root,
        store=store,
        receipt_dir=tmp_path / "receipts",
    )
    key = manifest["objects"][0]["storage_key"]
    assert client.objects[f"registry/{key}"] == PAYLOAD
    receipt = json.loads(result.receipt_path.read_text())
    assert receipt["objects"][0]["destination_uri"] == f"s3://demo/registry/{key}"


def test_partial_failure_is_reconciled_without_false_success_receipt(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    second_path = f"datasets/{DATASET_ID}/derived/summary.csv"
    second_payload = b"total\n2\n"
    source = root / second_path
    source.parent.mkdir(parents=True)
    source.write_bytes(second_payload)
    _git("add", ".", root=root)
    _git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.org",
        "commit",
        "-qm",
        "second file",
        root=root,
    )
    commit = _git("rev-parse", "HEAD", root=root)
    manifest["registry"]["commit"] = commit
    first = manifest["objects"][0]
    first["source_uri"] = f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{commit}/{SOURCE}"
    sha = hashlib.sha256(second_payload).hexdigest()
    manifest["objects"].append(
        {
            "source_path": second_path,
            "source_uri": f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{commit}/{second_path}",
            "sha256": sha,
            "role": "derived",
            "format": "csv",
            "storage_key": f"canonical/{DATASET_ID}/{sha}/derived/summary.csv",
            "size_bytes": len(second_payload),
        }
    )
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    store = LocalBlobStore(tmp_path / "objects")
    receipts = tmp_path / "receipts"
    source.write_bytes(b"broken")
    with pytest.raises(HandoffError, match="source checksum mismatch"):
        ingest_handoff(manifest_path=handoff, registry_root=root, store=store, receipt_dir=receipts)
    assert store.exists(first["storage_key"])
    assert not store.exists(manifest["objects"][1]["storage_key"])
    assert not receipts.exists()  # Never claim a multi-file run succeeded.

    source.write_bytes(second_payload)
    result = ingest_handoff(
        manifest_path=handoff, registry_root=root, store=store, receipt_dir=receipts
    )
    assert result.files_reused == 1
    assert result.files_published == 1
    assert json.loads(result.receipt_path.read_text())["status"] == "success"


def test_source_mutation_is_rejected_before_destination_write(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    (root / SOURCE).write_bytes(b"tampered")
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(HandoffError, match="source checksum mismatch"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=store,
            receipt_dir=tmp_path / "receipts",
        )
    assert not store.list_objects()
    assert not (tmp_path / "receipts").exists()


def test_forged_manifest_cannot_publish_uncommitted_bytes(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    forged_bytes = b"uncommitted contents"
    (root / SOURCE).write_bytes(forged_bytes)
    digest = hashlib.sha256(forged_bytes).hexdigest()
    manifest["objects"][0]["sha256"] = digest
    manifest["objects"][0]["size_bytes"] = len(forged_bytes)
    manifest["objects"][0]["storage_key"] = f"canonical/{DATASET_ID}/{digest}/raw/events.csv"
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    with pytest.raises(HandoffError, match="differs from its pinned Git blob"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=LocalBlobStore(tmp_path / "objects"),
            receipt_dir=tmp_path / "receipts",
        )


def test_rejects_corrupt_existing_destination_without_overwrite(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    store = LocalBlobStore(tmp_path / "objects")
    store.put_bytes(manifest["objects"][0]["storage_key"], b"corruption")
    with pytest.raises(HandoffError, match="existing destination is corrupt"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=store,
            receipt_dir=tmp_path / "receipts",
        )
    assert store.get_bytes(manifest["objects"][0]["storage_key"]) == b"corruption"


def test_rejects_wrong_checkout_commit(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    manifest["registry"]["commit"] = "0" * 40
    manifest["objects"][0]["source_uri"] = (
        f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{'0' * 40}/{SOURCE}"
    )
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    with pytest.raises(HandoffError, match="pinned commit"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=LocalBlobStore(tmp_path / "objects"),
            receipt_dir=tmp_path / "receipts",
        )


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"manifest_version": 2}, "manifest_version"),
        ({"dataset_id": "../escape"}, "dataset ID"),
        (
            {
                "license": {
                    "name": "Restricted",
                    "url": "https://example.org",
                    "redistribution": "forbidden",
                }
            },
            "redistribution",
        ),
        ({"objects": []}, "non-empty array"),
    ],
)
def test_rejects_invalid_manifest_fields(
    registry: tuple[Path, dict[str, Any]], change: dict[str, Any], expected: str
) -> None:
    _, manifest = registry
    manifest.update(change)
    with pytest.raises(HandoffError, match=expected):
        parse_handoff(manifest)


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("source_path", "datasets/fixture-events/raw/../secret.csv", "source_path"),
        ("source_path", "datasets/fixture-events/other/events.csv", "source_path"),
        ("storage_key", "canonical/fixture-events/incorrect/raw/events.csv", "storage_key"),
        ("source_uri", "https://evil.example/anything", "source_uri"),
        ("sha256", "invalid", "SHA-256"),
        ("size_bytes", -1, "size_bytes"),
        ("size_bytes", True, "size_bytes"),
    ],
)
def test_rejects_invalid_source_identity(
    registry: tuple[Path, dict[str, Any]], field: str, value: Any, expected: str
) -> None:
    _, manifest = registry
    manifest["objects"][0][field] = value
    with pytest.raises(HandoffError, match=expected):
        parse_handoff(manifest)


def test_rejects_duplicate_destination_key(registry: tuple[Path, dict[str, Any]]) -> None:
    _, manifest = registry
    manifest["objects"].append(deepcopy(manifest["objects"][0]))
    with pytest.raises(HandoffError, match="duplicate"):
        parse_handoff(manifest)


def test_rejects_symlink_escaping_checkout(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    other = tmp_path / "outside.csv"
    other.write_bytes(PAYLOAD)
    (root / SOURCE).unlink()
    (root / SOURCE).symlink_to(other)
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    with pytest.raises(HandoffError, match="unsafe or missing source"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=LocalBlobStore(tmp_path / "objects"),
            receipt_dir=tmp_path / "receipts",
        )


def test_never_writes_generated_artifacts_into_canonical_registry(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(HandoffError, match="receipt directory must be outside"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=store,
            receipt_dir=root / "generated-receipts",
        )
    with pytest.raises(HandoffError, match="destination store must be outside"):
        ingest_handoff(
            manifest_path=handoff,
            registry_root=root,
            store=LocalBlobStore(root / "generated-objects"),
            receipt_dir=tmp_path / "receipts",
        )
    assert not store.list_objects()


def test_handoff_not_verified_requires_source_checksum_anyway(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path
) -> None:
    root, manifest = registry
    manifest["local_bytes_verified"] = False
    manifest["objects"][0]["size_bytes"] = None
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    result = ingest_handoff(
        manifest_path=handoff,
        registry_root=root,
        store=LocalBlobStore(tmp_path / "objects"),
        receipt_dir=tmp_path / "receipts",
    )
    assert result.bytes_verified == len(PAYLOAD)


def test_cli_is_runnable_without_cloud_credentials(
    registry: tuple[Path, dict[str, Any]], tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, manifest = registry
    handoff = _write_manifest(tmp_path / "handoff.json", manifest)
    platform_main(
        [
            "registry-ingest",
            "--manifest",
            str(handoff),
            "--registry-root",
            str(root),
            "--root",
            str(tmp_path / "objects"),
            "--receipt-dir",
            str(tmp_path / "receipts"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    assert output["files_published"] == 1
    assert Path(output["receipt_path"]).exists()
