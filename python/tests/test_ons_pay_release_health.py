"""Active-vintage health and read-only, generation-bound rollback recommendations."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_ons_pay_refresh import AS_OF, PAY_CSV, _source

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_release_health import (
    ReleaseHealthError,
    assess_ons_pay_release,
    format_release_prometheus,
)
from data_platform_lab.orchestration.ons_pay_refresh import RefreshResult, refresh_ons_pay
from data_platform_lab.serving.api import create_app
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.serving.release_store import ReleaseError, ReleaseState, ReleaseStore
from data_platform_lab.storage import LocalBlobStore

ONE_MORE_MISSING = PAY_CSV.replace("2022-04-01,100,Pounds,£,90,110,,", "2022-04-01,,Pounds,£,,,x,")
TWO_MORE_MISSING = ONE_MORE_MISSING.replace(
    "2022-04-01,200,Pounds,£,,,,", "2022-04-01,,Pounds,£,,,x,"
)


def _now() -> datetime:
    return datetime.now(UTC) + timedelta(minutes=2)


def _policy() -> dict[str, int | float]:
    return {
        "max_added_rows": 0,
        "max_removed_rows": 0,
        "max_revised_rows": 1,
        "max_newly_missing_pay_rows": 1,
        "max_missing_pay_increase_pp": 15.0,
        "max_absolute_pay_revision_gbp": 0.0,
    }


def _prepare(
    tmp_path: Path, *, three: bool = False
) -> tuple[Path, tuple[VintageKey, ...], RefreshResult]:
    sources = [
        _source(tmp_path, name="first", csv_text=PAY_CSV),
        _source(tmp_path, name="second", csv_text=ONE_MORE_MISSING),
    ]
    if three:
        sources.append(_source(tmp_path, name="third", csv_text=TWO_MORE_MISSING))
    root = tmp_path / "curated"
    result = refresh_ons_pay(
        sources=sources,
        store=LocalBlobStore(tmp_path / "objects"),
        output_root=root,
        as_of=AS_OF,
    )
    keys = tuple(VintageKey(1, item.registry_commit, item.source_sha256) for item in result.sources)
    store = ReleaseStore(root)
    assert store.activate(
        candidate=keys[0], expected_generation=0, bootstrap=True, reason="Start"
    ).applied
    for generation, candidate in enumerate(keys[1:], start=1):
        assert store.activate(
            candidate=candidate,
            expected_generation=generation,
            reason="Reviewed source refresh",
            **_policy(),
        ).applied
    return root, keys, result


@pytest.fixture
def pair(tmp_path: Path) -> tuple[Path, tuple[VintageKey, ...], RefreshResult]:
    return _prepare(tmp_path)


def test_active_rate_breach_recommends_verified_prior_generation_without_writes(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
) -> None:
    root, keys, _ = pair
    store = ReleaseStore(root)
    state = store.status()
    assert state is not None and state.generation == 2
    before = store.head.read_bytes()
    events = {path.name: path.read_bytes() for path in store.events.glob("*.json")}

    report = assess_ons_pay_release(
        data_root=root,
        max_source_age_days=30,
        max_refresh_age_hours=2,
        max_missing_pay_percent=20.0,
        now=_now(),
    )
    assert report.generation == 2 and report.event_sha256 == state.event_sha256
    assert report.vintage == keys[1]
    assert report.failed_checks == ("missing_pay_rate",)
    assert report.active_monitor.status == "unhealthy"
    assert report.advice.action == "review_rollback"
    assert report.advice.to_generation == 1 and report.advice.target == keys[0]
    assert len(report.advice.reviewed_candidates) == 1
    assert report.advice.reviewed_candidates[0].status == "healthy"
    assert report.to_dict()["advice"]["target"]["source_sha256"] == keys[0].source_sha256
    assert store.head.read_bytes() == before
    assert {path.name: path.read_bytes() for path in store.events.glob("*.json")} == events
    assert store.status() == state
    # An active health assessment does not turn data queries into implicit aliases.
    assert TestClient(create_app(root)).get("/v1/ons-pay/geography-year").status_code == 422


def test_healthy_active_release_does_not_recommend_rollback(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
) -> None:
    root, _, _ = pair
    report = assess_ons_pay_release(
        data_root=root,
        max_source_age_days=30,
        max_refresh_age_hours=2,
        max_missing_pay_percent=40,
        now=_now(),
    )
    assert report.active_monitor.status == "healthy"
    assert not report.failed_checks
    assert report.advice.action == "keep"
    assert report.advice.to_generation is None
    assert not report.advice.reviewed_candidates


def test_selects_most_recent_healthy_distinct_prior_vintage(tmp_path: Path) -> None:
    root, keys, _ = _prepare(tmp_path, three=True)
    report = assess_ons_pay_release(data_root=root, max_missing_pay_percent=20, now=_now())
    assert report.generation == 3 and report.vintage == keys[2]
    assert report.advice.action == "review_rollback"
    assert report.advice.to_generation == 1 and report.advice.target == keys[0]
    assert [item.generation for item in report.advice.reviewed_candidates] == [2, 1]
    assert [item.status for item in report.advice.reviewed_candidates] == [
        "unhealthy",
        "healthy",
    ]
    assert report.advice.reviewed_candidates[0].failed_checks == ("missing_pay_rate",)


def test_freshness_or_shared_batch_failure_never_generates_rollback_advice(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
) -> None:
    root, _, result = pair
    stale = assess_ons_pay_release(
        data_root=root,
        max_source_age_days=0,
        max_missing_pay_percent=20,
        now=_now(),
    )
    assert set(stale.failed_checks) == {"source_freshness", "missing_pay_rate"}
    assert stale.advice.action == "investigate" and stale.advice.target is None
    assert not stale.advice.reviewed_candidates

    original = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    failed = {
        **original,
        "run_id": uuid4().hex,
        "created_at": (datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        "status": "failed",
        "failure": {
            "phase": "ingestion",
            "source_index": 0,
            "error_type": "HandoffError",
            "message": "simulated failure",
        },
        "sources": [],
        "metrics": dict.fromkeys(original["metrics"], 0),
    }
    (root / "manifests" / f"ons_pay_refresh_{failed['run_id']}.json").write_text(
        json.dumps(failed), encoding="utf-8"
    )
    report = assess_ons_pay_release(
        data_root=root,
        max_missing_pay_percent=20,
        now=_now(),
    )
    assert set(report.failed_checks) == {"missing_pay_rate", "last_batch"}
    assert report.advice.action == "investigate"
    assert report.advice.to_generation is None


def test_broken_active_curated_data_is_reported_without_unverified_advice(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
) -> None:
    root, keys, _ = pair
    store = ReleaseStore(root)
    state = store.history()
    assert state is not None
    path = CuratedCatalog(root).get_vintage(keys[1]).artifacts["observations"].path
    path.write_bytes(b"corrupt parquet")
    with pytest.raises(ReleaseError, match="trusted curated data"):
        store.status()

    report = assess_ons_pay_release(data_root=root, max_missing_pay_percent=20, now=_now())
    assert "curation_integrity" in report.failed_checks
    assert report.advice.action == "investigate" and report.advice.target is None
    assert report.advice.reviewed_candidates == ()
    assert store.history() == state  # hash-linked event history remains inspectable


def test_bootstrap_with_no_earlier_candidate_requires_investigation(tmp_path: Path) -> None:
    source = _source(tmp_path, name="only", csv_text=ONE_MORE_MISSING)
    root = tmp_path / "curated"
    result = refresh_ons_pay(
        sources=[source],
        store=LocalBlobStore(tmp_path / "objects"),
        output_root=root,
        as_of=AS_OF,
    )
    item = result.sources[0]
    key = VintageKey(1, item.registry_commit, item.source_sha256)
    store = ReleaseStore(root)
    assert store.activate(
        candidate=key, expected_generation=0, bootstrap=True, reason="Initial"
    ).applied
    report = assess_ons_pay_release(data_root=root, max_missing_pay_percent=20, now=_now())
    assert report.active_monitor.status == "unhealthy"
    assert report.advice.action == "investigate"
    assert report.advice.target is None and report.advice.reviewed_candidates == ()
    assert report.generation == 1


def test_tampered_release_history_fails_closed(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
) -> None:
    root, _, _ = pair
    store = ReleaseStore(root)
    state = store.status()
    assert state is not None
    event = store.events / f"{state.event_sha256}.json"
    event.write_bytes(event.read_bytes() + b" ")
    with pytest.raises(ReleaseError, match="release event"):
        assess_ons_pay_release(data_root=root, now=_now())


def test_rejects_concurrent_generation_change_during_assessment(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
) -> None:
    root, _, _ = pair
    real_history = ReleaseStore.history
    reads = 0

    def changed_history(store: ReleaseStore) -> ReleaseState | None:
        nonlocal reads
        reads += 1
        state = real_history(store)
        if reads == 2 and state is not None:
            return replace(state, generation=state.generation + 1, event_sha256="f" * 64)
        return state

    with (
        patch.object(ReleaseStore, "history", changed_history),
        pytest.raises(ReleaseHealthError, match="changed during assessment"),
    ):
        assess_ons_pay_release(data_root=root, max_missing_pay_percent=40, now=_now())


def test_cli_assess_exit_codes_and_opt_in_prometheus_export(
    pair: tuple[Path, tuple[VintageKey, ...], RefreshResult],
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root, keys, _ = pair
    prefix = ["ons-pay-release", "--data-root", str(root), "assess"]
    args = [*prefix, "--max-missing-pay-percent", "20"]
    before = ReleaseStore(root).head.read_bytes()

    with pytest.raises(SystemExit) as failed:
        platform_main(args)
    assert failed.value.code == 1
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["advice"]["action"] == "review_rollback"
    assert parsed["advice"]["target"]["registry_commit"] == keys[0].registry_commit

    textfile = tmp_path / "release-health.prom"
    with pytest.raises(SystemExit) as exported:
        platform_main([*args, "--format", "prometheus", "--textfile", str(textfile)])
    assert exported.value.code == 1
    output = capsys.readouterr().out
    assert textfile.read_text(encoding="utf-8") == output
    assert "dpl_ons_pay_release_rollback_recommended{" in output
    assert "dpl_ons_pay_release_rollback_target_generation{" in output
    assert "dpl_ons_pay_monitor_healthy{" in output
    assert ReleaseStore(root).head.read_bytes() == before

    platform_main([*prefix, "--max-missing-pay-percent", "40"])
    assert json.loads(capsys.readouterr().out)["advice"]["action"] == "keep"
    assert "dpl_ons_pay_release_rollback_target_generation" not in format_release_prometheus(
        assess_ons_pay_release(data_root=root, max_missing_pay_percent=40, now=_now())
    )


def test_cli_misconfiguration_and_uninitialized_release_are_explicit(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "unused"
    with pytest.raises(SystemExit) as missing:
        platform_main(["ons-pay-release", "--data-root", str(root), "assess"])
    assert missing.value.code == 2
    assert "no active release" in capsys.readouterr().err
    assert not root.exists()

    with pytest.raises(SystemExit) as bad_format:
        platform_main(
            [
                "ons-pay-release",
                "--data-root",
                str(root),
                "assess",
                "--textfile",
                str(tmp_path / "bad.prom"),
            ]
        )
    assert bad_format.value.code == 2
    assert "--textfile requires" in capsys.readouterr().err

    prepared, _, _ = _prepare(tmp_path / "prepared")
    with pytest.raises(SystemExit) as bad_threshold:
        platform_main(
            [
                "ons-pay-release",
                "--data-root",
                str(prepared),
                "assess",
                "--max-missing-pay-percent",
                "101",
            ]
        )
    assert bad_threshold.value.code == 2
    assert "cannot exceed 100" in capsys.readouterr().err
