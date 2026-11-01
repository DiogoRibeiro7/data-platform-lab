"""Exporter failure isolation and offline dashboard/alert contract checks."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import pytest

from data_platform_lab.observability import textfile
from data_platform_lab.observability.ons_pay_monitor import (
    _METRIC_HELP,
    MonitorCheck,
    MonitorReport,
    format_prometheus,
)
from data_platform_lab.serving.catalog import VintageKey


def test_atomic_writer_replaces_exact_file_and_cleans_temporary(tmp_path: Path) -> None:
    output = tmp_path / "ons-pay.prom"
    output.write_text("old complete file\n", encoding="utf-8")
    payload = '# TYPE example gauge\nexample{pin="fixed"} 1\n'
    textfile.write_prometheus_textfile(path=output, metrics=payload)
    assert output.read_text(encoding="utf-8") == payload
    assert output.stat().st_mode & 0o777 == 0o644
    assert sorted(tmp_path.iterdir()) == [output]


def test_writer_accepts_relative_parent_and_normalizes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = tmp_path / "work"
    work.mkdir()
    output_directory = tmp_path / "textfiles"
    output_directory.mkdir()
    monkeypatch.chdir(work)
    textfile.write_prometheus_textfile(path=Path("../textfiles/ons-pay.prom"), metrics="ok\n")
    assert (output_directory / "ons-pay.prom").read_text(encoding="utf-8") == "ok\n"


def test_failed_atomic_replace_keeps_last_full_metrics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "ons-pay.prom"
    output.write_text("original report\n", encoding="utf-8")

    def fail_rename(_source: Path, _target: Path) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(textfile.os, "replace", fail_rename)
    with pytest.raises(textfile.TextfileExportError, match="atomically"):
        textfile.write_prometheus_textfile(path=output, metrics="truncated report")

    assert output.read_text(encoding="utf-8") == "original report\n"
    assert sorted(tmp_path.iterdir()) == [output]


def test_writer_rejects_non_textfile_and_unsafe_paths(tmp_path: Path) -> None:
    real = tmp_path / "original.prom"
    real.write_text("preserve me\n", encoding="utf-8")
    symlink = tmp_path / "alias.prom"
    symlink.symlink_to(real)
    directory = tmp_path / "directory.prom"
    directory.mkdir()
    nested_symlink = tmp_path / "linked-directory"
    nested_symlink.symlink_to(tmp_path, target_is_directory=True)

    for path in (
        tmp_path / "ons-pay.txt",
        tmp_path / ".hidden.prom",
        symlink,
        directory,
        tmp_path / "missing" / "file.prom",
        nested_symlink / "new.prom",
    ):
        with pytest.raises(textfile.TextfileExportError):
            textfile.write_prometheus_textfile(path=path, metrics="new data\n")
    assert real.read_text(encoding="utf-8") == "preserve me\n"
    assert not list(tmp_path.glob(".*.tmp"))


def _example_root() -> Path:
    return Path(__file__).resolve().parents[2] / "docs" / "examples"


def test_dashboard_and_alert_examples_use_only_exported_gauges() -> None:
    report = MonitorReport(
        checked_at_utc=datetime(2026, 11, 1, tzinfo=UTC).isoformat(),
        vintage=VintageKey(1, "a" * 40, "b" * 64),
        status="healthy",
        checks=(MonitorCheck("curation_integrity", "pass", 1, 1, "checked"),),
        metrics=dict.fromkeys(_METRIC_HELP, 1),
        latest_batch_run_id=None,
        latest_vintage_success_run_id=None,
    )
    metrics = format_prometheus(report)
    exported_names = {
        line.split("{", 1)[0] for line in metrics.splitlines() if line.startswith("dpl_ons_pay_")
    }
    assert "dpl_ons_pay_monitor_checked_at_seconds" in exported_names

    examples = _example_root()
    rules = json.loads((examples / "ons-pay-alert-rules.json").read_text(encoding="utf-8"))
    alerts = rules["groups"][0]["rules"]
    assert len(alerts) == 3
    assert len({alert["alert"] for alert in alerts}) == 3
    assert all(alert["for"].endswith("m") for alert in alerts)
    for alert in alerts:
        expr = alert["expr"]
        references = set(re.findall(r"dpl_ons_pay_[a-z_]+", expr))
        assert len(references) == 1
        assert references <= exported_names
        assert 'registry_commit="COMMIT_SHA40"' in expr
        assert 'source_sha256="SOURCE_SHA256"' in expr
    assert " == 0" in alerts[0]["expr"]
    assert "time() - " in alerts[1]["expr"]
    assert "> 7200" in alerts[1]["expr"]
    assert alerts[2]["expr"].startswith("absent(")

    dashboard = json.loads((examples / "ons-pay-health-dashboard.json").read_text(encoding="utf-8"))
    assert dashboard["__inputs"][0]["pluginId"] == "prometheus"
    assert dashboard["timezone"] == "utc"
    assert {variable["name"] for variable in dashboard["templating"]["list"]} == {
        "registry_commit",
        "source_sha256",
    }
    panels = dashboard["panels"]
    assert len(panels) == 7
    assert len({panel["id"] for panel in panels}) == len(panels)
    for panel in panels:
        assert panel["type"] in {"stat", "timeseries"}
        assert panel["datasource"]["uid"] == "${DS_PROMETHEUS}"
        assert panel["gridPos"]["x"] >= 0
        assert panel["gridPos"]["x"] + panel["gridPos"]["w"] <= 24
        expr = panel["targets"][0]["expr"]
        assert re.findall(r"dpl_ons_pay_[a-z_]+", expr)[0] in exported_names
        assert 'registry_commit="$registry_commit"' in expr
        assert 'source_sha256="$source_sha256"' in expr
