"""Offline, content-addressed release-health archive and trend regression tests."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from test_ons_pay_release_health import _prepare

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_release_health import assess_ons_pay_release
from data_platform_lab.observability.ons_pay_release_history import (
    ReleaseHealthArchive,
    ReleaseHistoryError,
    format_trends_csv,
)
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.serving.release_store import ReleaseStore


@pytest.fixture
def prepared(tmp_path: Path) -> tuple[Path, tuple[VintageKey, ...]]:
    root, keys, _ = _prepare(tmp_path)
    return root, keys


def _evaluate(root: Path, *, minutes: int, maximum: float = 20.0):
    return assess_ons_pay_release(
        data_root=root,
        max_missing_pay_percent=maximum,
        now=datetime.now(UTC) + timedelta(minutes=minutes),
    )


def test_read_only_empty_history_creates_no_files(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    assert not archive.root.exists()
    history = archive.trends()
    assert history.total_records == 0
    assert history.summary.assessments == 0
    assert history.summary.first_checked_at_utc is None
    assert history.summary.missing_pay_change_pp is None
    assert history.samples == () and history.by_generation == ()
    assert format_trends_csv(history).startswith("record_sha256,checked_at_utc,generation,")
    assert not archive.root.exists()


def test_explicit_record_is_idempotent_content_addressed_and_trendable(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, keys = prepared
    store = ReleaseStore(root)
    before_head = store.head.read_bytes()
    before_events = {path.name: path.read_bytes() for path in store.events.glob("*.json")}
    archive = ReleaseHealthArchive(root)
    passing = _evaluate(root, minutes=1, maximum=40)
    failing = _evaluate(root, minutes=2, maximum=20)
    assert passing.active_monitor.status == "healthy"
    assert failing.active_monitor.status == "unhealthy"
    assert not archive.root.exists()  # evaluation itself performs no writes

    first_sha = archive.record(passing)
    assert archive.record(passing) == first_sha  # safe replay, never another event
    second_sha = archive.record(failing)
    assert first_sha != second_sha
    assert len(list(archive.records.glob("*.json"))) == 2
    for sha in (first_sha, second_sha):
        raw = (archive.records / f"{sha}.json").read_bytes()
        assert hashlib.sha256(raw).hexdigest() == sha
        assert raw.endswith(b"\n")

    trends = archive.trends()
    assert trends.total_records == 2
    assert trends.summary.assessments == 2
    assert trends.summary.generations == 1
    assert trends.summary.healthy_assessments == 1
    assert trends.summary.unhealthy_assessments == 1
    assert trends.summary.healthy_to_unhealthy == 1
    assert trends.summary.unhealthy_to_healthy == 0
    assert trends.summary.rollback_reviews == 1
    assert trends.summary.missing_pay_change_pp == 0
    assert [item.record_sha256 for item in trends.samples] == [first_sha, second_sha]
    assert [item.status for item in trends.samples] == ["healthy", "unhealthy"]
    assert trends.samples[-1].failed_checks == ("missing_pay_rate",)
    assert trends.samples[-1].rollback_to_generation == 1
    assert trends.by_generation[0].vintage == keys[1]
    assert trends.by_generation[0].assessments == 2
    assert trends.by_generation[0].latest_status == "unhealthy"

    reader = csv.DictReader(io.StringIO(format_trends_csv(trends)))
    rows = list(reader)
    assert len(rows) == 2
    assert rows[0]["generation"] == "2"
    assert rows[0]["registry_commit"] == keys[1].registry_commit
    assert rows[0]["rollback_to_generation"] == ""  # missing != 0
    assert rows[1]["rollback_to_generation"] == "1"
    assert store.head.read_bytes() == before_head
    assert {path.name: path.read_bytes() for path in store.events.glob("*.json")} == before_events


def test_prior_generation_records_remain_linked_after_rollback(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, keys = prepared
    archive = ReleaseHealthArchive(root)
    previous = archive.record(_evaluate(root, minutes=1))
    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=2, reason="Manual").applied
    current = archive.record(_evaluate(root, minutes=2))
    trends = archive.trends()
    assert trends.total_records == 2
    assert [item.generation for item in trends.samples] == [2, 3]
    assert [item.record_sha256 for item in trends.samples] == [previous, current]
    assert [item.vintage for item in trends.samples] == [keys[1], keys[0]]
    assert trends.summary.unhealthy_to_healthy == 1
    assert trends.summary.generations == 2
    assert [item.generation for item in trends.by_generation] == [2, 3]

    filtered = archive.trends(generation=2, last=1)
    assert filtered.total_records == 2
    assert filtered.summary.assessments == 1
    assert filtered.samples[0].record_sha256 == previous
    assert archive.trends(generation=123).summary.assessments == 0
    assert archive.trends(last=1).samples[0].record_sha256 == current


def test_stale_or_contradictory_assessment_never_creates_archive(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    original = _evaluate(root, minutes=1)
    forged = replace(
        original,
        active_monitor=replace(original.active_monitor, vintage=original.advice.target),
    )
    with pytest.raises(ReleaseHistoryError, match="monitor pin contradicts"):
        archive.record(forged)
    assert not archive.root.exists()

    store = ReleaseStore(root)
    assert store.rollback(to_generation=1, expected_generation=2, reason="Manual").applied
    with pytest.raises(ReleaseHistoryError, match="active generation changed"):
        archive.record(original)
    assert not archive.root.exists()


def test_tampered_or_symlinked_old_record_fails_even_when_filtered(
    prepared: tuple[Path, tuple[VintageKey, ...]], tmp_path: Path
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    old_sha = archive.record(_evaluate(root, minutes=1))
    archive.record(_evaluate(root, minutes=2, maximum=40))
    old_path = archive.records / f"{old_sha}.json"
    original = old_path.read_bytes()
    old_path.write_bytes(original + b" ")
    with pytest.raises(ReleaseHistoryError, match="health assessment"):
        archive.trends(last=1)

    old_path.unlink()
    external = tmp_path / "outside.json"
    external.write_bytes(original)
    old_path.symlink_to(external)
    with pytest.raises(ReleaseHistoryError, match="health assessment"):
        archive.trends(generation=123)


def test_release_event_tamper_breaks_the_archive_chain(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    archive.record(_evaluate(root, minutes=1))
    store = ReleaseStore(root)
    state = store.history()
    assert state is not None
    event = store.events / f"{state.event_sha256}.json"
    event.write_bytes(event.read_bytes() + b" ")
    with pytest.raises(ValueError, match="release event"):
        archive.trends()


def test_failed_publish_does_not_expose_partial_record(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    report = _evaluate(root, minutes=1)
    head = ReleaseStore(root).head.read_bytes()
    with (
        patch(
            "data_platform_lab.observability.ons_pay_release_history.os.link",
            side_effect=OSError("interrupted"),
        ),
        pytest.raises(ReleaseHistoryError, match="durably publish"),
    ):
        archive.record(report)
    assert not list(archive.records.glob("*.json"))
    assert not list(archive.records.glob("*.tmp"))
    assert ReleaseStore(root).head.read_bytes() == head
    assert archive.record(report)


def test_cli_default_assess_remains_read_only_and_record_is_explicit(
    prepared: tuple[Path, tuple[VintageKey, ...]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    prefix = ["ons-pay-release", "--data-root", str(root)]
    args = [*prefix, "assess", "--max-missing-pay-percent", "20"]

    with pytest.raises(SystemExit) as preview:
        platform_main(args)
    assert preview.value.code == 1
    assert "record_sha256" not in json.loads(capsys.readouterr().out)
    assert not archive.root.exists()

    with pytest.raises(SystemExit) as recorded:
        platform_main([*args, "--record"])
    assert recorded.value.code == 1
    body = json.loads(capsys.readouterr().out)
    sha = body["record_sha256"]
    assert (archive.records / f"{sha}.json").is_file()

    platform_main([*prefix, "trends", "--format", "json"])
    trends = json.loads(capsys.readouterr().out)
    assert trends["total_records"] == 1
    assert trends["summary"]["rollback_reviews"] == 1
    assert trends["samples"][0]["record_sha256"] == sha
    platform_main([*prefix, "trends", "--format", "csv", "--last", "1"])
    assert sha in capsys.readouterr().out


def test_cli_invalid_filters_fail_before_writing(
    prepared: tuple[Path, tuple[VintageKey, ...]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, _ = prepared
    prefix = ["ons-pay-release", "--data-root", str(root), "trends"]
    for args in (["--last", "0"], ["--last", "1001"], ["--generation", "-1"]):
        with pytest.raises(SystemExit) as error:
            platform_main([*prefix, *args])
        assert error.value.code == 2
        assert capsys.readouterr().err
    assert not ReleaseHealthArchive(root).root.exists()


def test_bad_record_fields_and_invalid_metric_do_not_go_unnoticed(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, _ = prepared
    archive = ReleaseHealthArchive(root)
    report = _evaluate(root, minutes=1)
    sha = archive.record(report)
    original = json.loads((archive.records / f"{sha}.json").read_text(encoding="utf-8"))
    original["report"]["active_monitor"]["metrics"]["missing_pay_percent"] = "unknown"
    # Canonicalize and rename the tampered record to defeat the simple
    # filename-vs-content mismatch. Schema validation must still reject it.
    path = archive.records / f"{sha}.json"
    path.unlink()
    body = (
        json.dumps(original, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    new_sha = hashlib.sha256(body).hexdigest()
    (archive.records / f"{new_sha}.json").write_bytes(body)
    with pytest.raises(ReleaseHistoryError, match="missing_pay_percent"):
        archive.trends()


def test_corrupt_active_parquet_remains_visible_in_recorded_incidents(
    prepared: tuple[Path, tuple[VintageKey, ...]],
) -> None:
    root, keys = prepared
    observations = CuratedCatalog(root).get_vintage(keys[1]).artifacts["observations"].path
    observations.write_bytes(b"broken silver")
    report = _evaluate(root, minutes=1)
    assert report.active_monitor.status == "unhealthy"
    assert report.advice.action == "investigate"
    archive = ReleaseHealthArchive(root)
    archive.record(report)
    assert archive.trends().summary.unhealthy_assessments == 1
