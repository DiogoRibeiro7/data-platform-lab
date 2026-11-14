"""Offline preflight and side-effect boundaries for the pinned PostgreSQL workflow."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from test_ons_pay_refresh import _source

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.orchestration import ons_pay_postgres_cli as workflow
from data_platform_lab.orchestration.ons_pay_refresh import RefreshSource


def arguments(tmp_path: Path, source: RefreshSource) -> list[str]:
    manifest = json.loads(source.manifest_path.read_text())
    return [
        "--handoff",
        str(source.manifest_path),
        "--registry-root",
        str(source.registry_root),
        "--registry-commit",
        manifest["registry"]["commit"],
        "--source-sha256",
        manifest["objects"][0]["sha256"],
        "--dataset-id",
        "ons-gross-median-weekly-pay",
        "--data-root",
        str(tmp_path / "curated"),
        "--object-root",
        str(tmp_path / "objects"),
        "--target-id",
        "isolated-workflow",
        "--attempt-id",
        str(uuid4()),
    ]


def _snapshot(root: Path) -> dict[str, tuple[bytes, int] | None]:
    return {
        str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mtime_ns)
        if path.is_file()
        else None
        for path in root.rglob("*")
    }


def test_preview_has_no_clients_network_or_filesystem_changes(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    source = _source(tmp_path)
    argv = arguments(tmp_path, source)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Preview must not construct a client or execute ingestion")

    monkeypatch.setattr(workflow, "refresh_ons_pay", forbidden)
    monkeypatch.setattr(workflow, "LocalBlobStore", forbidden)
    monkeypatch.setattr("psycopg.connect", forbidden)
    monkeypatch.setattr("data_platform_lab.storage.config.S3Storage.open", forbidden)
    monkeypatch.setattr("urllib.request.urlopen", forbidden)
    before = _snapshot(tmp_path)
    platform_main(["ons-pay-postgres", *argv])
    local = json.loads(capsys.readouterr().out)
    assert local["status"] == "previewed"
    assert local["source_mode"] == "checkout"
    assert len(set(local["attempts"].values())) == 2
    assert local["publications"] == []
    assert _snapshot(tmp_path) == before
    # The same metadata-only contract covers HTTPS and S3, with no credentials present.
    index = argv.index("--registry-root")
    del argv[index : index + 2]
    index = argv.index("--object-root")
    del argv[index : index + 2]
    platform_main(["ons-pay-postgres", *argv, "--backend", "s3", "--bucket", "test-bucket"])
    remote = json.loads(capsys.readouterr().out)
    assert remote["source_mode"] == "https"
    assert remote["raw_storage"]["backend"] == "s3"
    assert remote["attempts"] == local["attempts"]
    assert _snapshot(tmp_path) == before


def test_neon_env_file_is_never_read_during_preview(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    """A preview must not access secret files, even when their path is supplied."""
    source = _source(tmp_path)
    missing_file = tmp_path / "absent.env.local"

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Preview tried to load Neon connection settings")

    monkeypatch.setattr(workflow, "_postgres_address", forbidden)
    workflow.main([*arguments(tmp_path, source), "--env-file", str(missing_file)])
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "previewed"
    assert not missing_file.exists()


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--registry-commit", "b" * 40),
        ("--source-sha256", "c" * 64),
        ("--dataset-id", "unapproved"),
        ("--target-id", "password://secret"),
        ("--attempt-id", "../secret"),
        ("--statement-timeout-ms", "0"),
        ("--lock-timeout-ms", "2147483648"),
    ],
)
def test_invalid_preflight_never_applies(
    tmp_path: Path, monkeypatch: Any, capsys: Any, flag: str, value: str
) -> None:
    source = _source(tmp_path)
    argv = arguments(tmp_path, source)
    if flag in argv:
        argv[argv.index(flag) + 1] = value
    else:
        argv.extend([flag, value])
    monkeypatch.setattr(workflow, "_apply", lambda *args: pytest.fail("Invalid preflight applied"))
    before = _snapshot(tmp_path)
    with pytest.raises(SystemExit) as error:
        workflow.main([*argv, "--apply"])
    assert error.value.code == 2
    output = capsys.readouterr().out
    assert "secret" not in output
    assert json.loads(output)["status"] == "failed"
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("flag", ["--data-root", "--object-root"])
def test_checkout_destinations_rejected_before_creating_store(
    tmp_path: Path, capsys: Any, flag: str
) -> None:
    source = _source(tmp_path)
    argv = arguments(tmp_path, source)
    argv[argv.index(flag) + 1] = str(source.registry_root / "output")
    before = _snapshot(tmp_path)
    with pytest.raises(SystemExit):
        workflow.main([*argv, "--apply"])
    assert json.loads(capsys.readouterr().out)["status"] == "failed"
    assert _snapshot(tmp_path) == before


def test_handoff_is_pinned_between_preflight_and_execution(tmp_path: Path) -> None:
    source = _source(tmp_path)
    args = workflow._parser().parse_args(arguments(tmp_path, source))
    pinned, _, _ = workflow._preflight(args)
    source.manifest_path.write_text(source.manifest_path.read_text() + "\n")
    with pytest.raises(ValueError, match="manifest"):
        workflow.plan_ons_pay_refresh([pinned])


def test_execution_failure_has_no_secrets_or_success(
    tmp_path: Path, capsys: Any, monkeypatch: Any
) -> None:
    source = _source(tmp_path)

    def fail(*args: Any) -> Any:
        raise RuntimeError("postgresql://password-secret row-value-secret")

    monkeypatch.setattr(workflow, "_apply", fail)
    with pytest.raises(SystemExit) as error:
        workflow.main([*arguments(tmp_path, source), "--apply"])
    assert error.value.code == 1
    output = capsys.readouterr().out
    assert "secret" not in output
    assert json.loads(output)["status"] == "failed"
