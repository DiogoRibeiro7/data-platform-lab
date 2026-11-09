"""Prometheus publication status contracts and explicit atomic exports."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from test_ons_pay_publication_status import NOW, _args, _record, _status
from test_ons_pay_publication_status import archived as archived
from test_ons_pay_publish_cli import SECRET, _snapshot

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability import ons_pay_publication_status_cli as status_cli
from data_platform_lab.observability import textfile
from data_platform_lab.observability.ons_pay_publication_status import (
    format_publication_prometheus,
    publication_status,
)
from data_platform_lab.serving.publish_evidence import PublicationArchive


def _samples(metrics: str, suffix: str) -> dict[tuple[tuple[str, str], ...], float]:
    """Inspect complete samples, including exact labels and absence of sample timestamps."""
    result = {}
    for line in metrics.splitlines():
        if line.startswith(f"dpl_ons_pay_publication_{suffix}{{"):
            matched = re.fullmatch(r"\w+\{(.*)\} (-?[0-9.e+]+|\+Inf)", line)
            assert matched is not None
            labels = tuple(re.findall(r'(\w+)="([a-z0-9_-]*)"', matched[1]))
            assert labels not in result
            result[labels] = float(matched[2])
    return result


def _invoke(capsys: Any, args: list[str], exit_code: int) -> str:
    if exit_code:
        with pytest.raises(SystemExit) as error:
            platform_main(["ons-pay-publication-status", *args])
        assert error.value.code == exit_code
    else:
        platform_main(["ons-pay-publication-status", *args])
    captured = capsys.readouterr()
    assert not captured.err and SECRET not in captured.out
    return captured.out


@pytest.mark.parametrize(
    "scenario,state,reason",
    [
        ("accepted", "ok", "latest_attempt_accepted"),
        ("failed", "alert", "latest_attempt_failed"),
        ("rejected", "alert", "latest_attempt_rejected"),
        ("stale", "alert", "stale"),
        ("future", "unavailable", "future_timestamp"),
        ("ambiguous", "unavailable", "ambiguous_latest_attempt"),
        ("missing", "unavailable", "no_attempts"),
    ],
)
def test_metrics_preserve_status_semantics_and_replace_old_exports(
    archived: Any, tmp_path: Path, capsys: Any, scenario: str, state: str, reason: str
) -> None:
    archive, plan, _ = archived
    ended = NOW + timedelta(seconds=1) if scenario == "future" else NOW - timedelta(hours=1)
    if scenario == "stale":
        ended = NOW - timedelta(hours=2)
    _record(
        archived,
        outcome=scenario if scenario in ("failed", "rejected") else "accepted",
        ended_at=ended,
    )
    if scenario == "ambiguous":
        _record(archived, ended_at=ended)
    target = "absent-target" if scenario == "missing" else "local-target"
    report = _status(archived, target_id=target, max_age_hours=1)
    before = _snapshot(archive.data_root)
    output = tmp_path / "publication.prom"
    output.write_text("previous complete metrics\n", encoding="utf-8")
    with patch.object(
        status_cli,
        "publication_status",
        side_effect=lambda **kwargs: publication_status(**kwargs, checked_at=NOW),
    ):
        metrics = _invoke(
            capsys,
            [
                *_args(archive.data_root, asdict(plan.vintage)),
                "--target-id",
                target,
                "--max-age-hours",
                "1",
                "--format",
                "prometheus",
                "--textfile",
                str(output),
            ],
            0 if state == "ok" else 1,
        )
    assert output.read_bytes() == metrics.encode("utf-8")
    assert metrics.endswith("\n") and "\r" not in metrics
    assert metrics == format_publication_prometheus(report)
    assert list(_samples(metrics, "healthy").values()) == [int(state == "ok")]
    assert {
        dict(labels)["state"]: value for labels, value in _samples(metrics, "status").items()
    } == {selected: int(selected == state) for selected in ("ok", "alert", "unavailable")}
    assert [dict(labels)["reason"] for labels in _samples(metrics, "reason")] == [reason]
    assert list(_samples(metrics, "checked_at_seconds").values()) == [NOW.timestamp()]
    assert list(_samples(metrics, "max_age_seconds").values()) == [3600]
    assert list(_samples(metrics, "latest_attempts").values()) == [report.latest_attempt_count]
    counts = {
        dict(labels)["outcome"]: value for labels, value in _samples(metrics, "attempts").items()
    }
    assert counts == report.counts
    expected_labels = {name: str(value) for name, value in asdict(plan.vintage).items()} | {
        "sink": "mongodb",
        "table": plan.table,
        "target_id": target,
    }
    for line in metrics.splitlines():
        if not line.startswith("#"):
            assert all(f'{key}="{value}"' in line for key, value in expected_labels.items())
    assert (
        "counter" not in metrics and "attempt_id" not in metrics and "record_sha256" not in metrics
    )
    if scenario in ("missing", "ambiguous"):
        assert "latest_attempt_age_seconds" not in metrics
        assert "latest_attempt_ended_at_seconds" not in metrics
    else:
        assert list(_samples(metrics, "latest_attempt_ended_at_seconds").values()) == [
            ended.timestamp()
        ]
        assert list(_samples(metrics, "latest_attempt_age_seconds").values()) == pytest.approx(
            [(NOW - ended).total_seconds()]
        )
    assert _snapshot(archive.data_root) == before
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize("target", [None, "local-target", "other-target"])
def test_unconfigured_age_and_exact_target_selection(archived: Any, target: str | None) -> None:
    _record(archived, target_id=None, outcome="failed")
    _record(archived, target_id="other-target", outcome="rejected")
    metrics = format_publication_prometheus(_status(archived, target_id=target))
    assert "max_age_seconds" not in metrics
    assert f'target_id="{target or ""}"' in metrics
    counts = {
        dict(labels)["outcome"]: value for labels, value in _samples(metrics, "attempts").items()
    }
    expected = {None: "failed", "local-target": "accepted", "other-target": "rejected"}[target]
    assert counts == {
        outcome: int(outcome == expected) for outcome in ("accepted", "failed", "rejected")
    }


def test_stdout_only_export_does_not_create_files(tmp_path: Path, capsys: Any) -> None:
    metrics = _invoke(capsys, [*_args(tmp_path), "--format", "prometheus"], 1)
    assert list(_samples(metrics, "healthy").values()) == [0]
    assert not list(tmp_path.iterdir())
    types = [line.split()[2] for line in metrics.splitlines() if line.startswith("# TYPE")]
    assert len(types) == len(set(types))
    assert all(f"# HELP {name} " in metrics for name in types)


def test_finite_hours_overflow_uses_prometheus_infinity(tmp_path: Path, capsys: Any) -> None:
    metrics = _invoke(
        capsys, [*_args(tmp_path), "--format", "prometheus", "--max-age-hours", "1e308"], 1
    )
    assert list(_samples(metrics, "max_age_seconds").values()) == [float("inf")]
    assert "+Inf\n" in metrics


@pytest.mark.parametrize(
    "flags",
    [
        ["--format", SECRET],
        ["--textfile", SECRET],
        ["--format", "prometheus", "--target-id", SECRET],
    ],
)
def test_invalid_options_fail_before_archive_or_export_io(
    tmp_path: Path, capsys: Any, flags: list[str]
) -> None:
    with (
        patch.object(PublicationArchive, "history", side_effect=AssertionError),
        patch.object(status_cli, "write_prometheus_textfile", side_effect=AssertionError),
    ):
        result = json.loads(_invoke(capsys, [*_args(tmp_path), *flags], 2))
    assert result["error"]["code"] == "invalid_arguments"
    assert not list(tmp_path.iterdir())


def test_corrupt_archive_does_not_replace_previous_metrics(
    archived: Any, tmp_path: Path, capsys: Any
) -> None:
    archive, plan, _ = archived
    (archive.root / f"{'f' * 64}.json").write_text(SECRET, encoding="utf-8")
    output = tmp_path / "publication.prom"
    output.write_text("old metrics\n", encoding="utf-8")
    with patch.object(status_cli, "write_prometheus_textfile", side_effect=AssertionError):
        result = json.loads(
            _invoke(
                capsys,
                [
                    *_args(archive.data_root, asdict(plan.vintage)),
                    "--format",
                    "prometheus",
                    "--textfile",
                    str(output),
                ],
                5,
            )
        )
    assert result["error"]["code"] == "evidence_error"
    assert output.read_text(encoding="utf-8") == "old metrics\n"


@pytest.mark.parametrize("operation", ["fsync", "replace"])
def test_failed_export_preserves_old_metrics_and_cleans_temporary_file(
    tmp_path: Path, capsys: Any, operation: str
) -> None:
    output = tmp_path / "publication.prom"
    output.write_text("old metrics\n", encoding="utf-8")
    with patch.object(textfile.os, operation, side_effect=OSError(SECRET)):
        result = json.loads(
            _invoke(
                capsys,
                [*_args(tmp_path / "data"), "--format", "prometheus", "--textfile", str(output)],
                2,
            )
        )
    assert result["error"]["code"] == "textfile_error"
    assert output.read_text(encoding="utf-8") == "old metrics\n"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["publication.prom"]


@pytest.mark.parametrize(
    "destination", ["wrong.txt", ".hidden.prom", "missing/file.prom", "directory.prom", "link.prom"]
)
def test_unsafe_textfile_destinations_are_rejected(
    tmp_path: Path, capsys: Any, destination: str
) -> None:
    output = tmp_path / destination
    if destination == "directory.prom":
        output.mkdir()
    if destination == "link.prom":
        actual = tmp_path / "previous.prom"
        actual.write_text("old metrics\n", encoding="utf-8")
        try:
            output.symlink_to(actual)
        except OSError:
            pytest.skip("symlink creation is unavailable on this host")
    result = json.loads(
        _invoke(
            capsys,
            [*_args(tmp_path / "data"), "--format", "prometheus", "--textfile", str(output)],
            2,
        )
    )
    assert result["error"]["code"] == "textfile_error"
    if destination == "link.prom":
        assert actual.read_text(encoding="utf-8") == "old metrics\n"
    elif destination != "directory.prom":
        assert not output.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_textfile_export_needs_no_optional_clients(tmp_path: Path) -> None:
    output = tmp_path / "publication.prom"
    code = (
        "import sys; "
        "sys.modules.update({name: None for name in ('duckdb', 'psycopg', 'pymongo', 'fastapi')}); "
        "from data_platform_lab.cli.main import main; main(sys.argv[1:])"
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            "ons-pay-publication-status",
            *_args(tmp_path / "absent"),
            "--format",
            "prometheus",
            "--textfile",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 1 and not result.stderr
    assert output.read_text(encoding="utf-8") == result.stdout
    assert list(_samples(result.stdout, "healthy").values()) == [0]
    assert not (tmp_path / "absent").exists()
