"""Offline tests for a manual, auditable ONS pay release pointer lifecycle."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from test_ons_pay_serving import COMMIT_A, COMMIT_B, PAY_CSV, _curate, _pin

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.serving.api import create_app
from data_platform_lab.serving.catalog import CuratedCatalog, VintageKey
from data_platform_lab.serving.release_store import ReleaseError, ReleaseStore


def _key(receipt: dict[str, object]) -> VintageKey:
    pin = _pin(receipt)
    return VintageKey(1, pin["registry_commit"], pin["source_sha256"])


@pytest.fixture
def pair(tmp_path: Path) -> tuple[Path, VintageKey, VintageKey]:
    root, first = _curate(tmp_path, commit=COMMIT_A)
    _, second = _curate(tmp_path, commit=COMMIT_B)
    return root, _key(first), _key(second)


def _strict() -> dict[str, int | float]:
    return {
        "max_added_rows": 0,
        "max_removed_rows": 0,
        "max_revised_rows": 0,
        "max_newly_missing_pay_rows": 0,
        "max_missing_pay_increase_pp": 0.0,
        "max_absolute_pay_revision_gbp": 0.0,
    }


def _bootstrap(store: ReleaseStore, first: VintageKey) -> None:
    plan = store.activate(candidate=first, expected_generation=0, bootstrap=True, reason="First")
    assert plan.eligible and plan.applied


def test_empty_status_and_bootstrap_preview_are_read_only(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, _ = pair
    store = ReleaseStore(root)
    assert store.status() is None
    assert TestClient(create_app(root)).get("/v1/ons-pay/releases/active").status_code == 404
    preview = store.plan_activate(candidate=first, expected_generation=0, bootstrap=True)
    assert preview.action == "bootstrap" and preview.eligible and not preview.applied
    assert preview.comparison is None
    assert preview.baseline is None
    assert not (root / "releases").exists()

    with pytest.raises(ReleaseError, match="explicit --bootstrap"):
        store.plan_activate(candidate=first, expected_generation=0)
    with pytest.raises(ReleaseError, match="omit comparison"):
        store.plan_activate(candidate=first, expected_generation=0, bootstrap=True, **_strict())
    assert not (root / "releases").exists()


def test_bootstrap_creates_audited_pointer_and_keeps_exact_pins(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, _ = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    state = store.status()
    assert state is not None
    assert state.generation == 1 and state.active == first
    assert len(state.events) == 1 and state.events[0].action == "bootstrap"
    assert state.events[0].reason == "First"

    event_file = store.events / f"{state.event_sha256}.json"
    assert hashlib.sha256(event_file.read_bytes()).hexdigest() == state.event_sha256
    head = json.loads(store.head.read_text(encoding="utf-8"))
    assert head["generation"] == 1
    assert head["event_sha256"] == state.event_sha256
    assert head["active"]["registry_commit"] == COMMIT_A

    client = TestClient(create_app(root))
    response = client.get("/v1/ons-pay/releases/active")
    assert response.status_code == 200
    assert response.json()["generation"] == 1
    assert response.json()["event_sha256"] == state.event_sha256
    assert response.json()["vintage"]["registry_commit"] == COMMIT_A
    # Discovery does not weaken the existing endpoint contract: query pins remain mandatory.
    assert client.get("/v1/ons-pay/geography-year").status_code == 422
    assert (
        client.get(
            "/v1/ons-pay/geography-year",
            params={"registry_commit": COMMIT_A, "source_sha256": first.source_sha256},
        ).status_code
        == 200
    )


def test_promotion_preview_and_immutable_history(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, second = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    before = store.status()
    assert before is not None
    event_bytes = (store.events / f"{before.event_sha256}.json").read_bytes()

    preview = store.plan_activate(candidate=second, expected_generation=1, **_strict())
    assert preview.action == "promote" and preview.eligible and not preview.applied
    assert preview.comparison is not None
    assert preview.comparison.status == "pass"
    assert len(preview.comparison.policy_checks) == 6
    assert not any(check.state != "pass" for check in preview.comparison.policy_checks)
    assert store.status() == before
    assert len(list(store.events.glob("*.json"))) == 1

    applied = store.activate(
        candidate=second, expected_generation=1, reason="Approved", **_strict()
    )
    assert applied.applied and applied.eligible
    after = store.status()
    assert after is not None
    assert after.generation == 2 and after.active == second
    assert [event.action for event in after.events] == ["bootstrap", "promote"]
    assert after.events[1].previous_event_sha256 == before.event_sha256
    assert after.events[1].comparison is not None
    assert {check["state"] for check in after.events[1].comparison["policy_checks"]} == {"pass"}
    assert (store.events / f"{before.event_sha256}.json").read_bytes() == event_bytes
    assert len(list(store.events.glob("*.json"))) == 2
    assert (
        TestClient(create_app(root))
        .get("/v1/ons-pay/releases/active")
        .json()["vintage"]["registry_commit"]
        == COMMIT_B
    )


def test_policy_breach_cannot_write_even_with_apply(tmp_path: Path) -> None:
    root, first = _curate(tmp_path, commit=COMMIT_A)
    revised = PAY_CSV.replace("2022-04-01,100,Pounds", "2022-04-01,105,Pounds")
    _, second = _curate(tmp_path, commit=COMMIT_B, payload=revised.encode("utf-8-sig"))
    store = ReleaseStore(root)
    _bootstrap(store, _key(first))
    before = store.status()
    preview = store.plan_activate(candidate=_key(second), expected_generation=1, **_strict())
    assert not preview.eligible and not preview.applied
    assert preview.comparison is not None and preview.comparison.status == "breach"
    applied = store.activate(
        candidate=_key(second), expected_generation=1, reason="No", **_strict()
    )
    assert not applied.eligible and not applied.applied
    assert store.status() == before
    assert len(list(store.events.glob("*.json"))) == 1


def test_promotion_requires_all_six_valid_policies_and_fresh_generation(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, second = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    before = store.status()
    with pytest.raises(ReleaseError, match="six explicit"):
        store.activate(candidate=second, expected_generation=1, reason="Incomplete")
    with pytest.raises(ReleaseError, match="comparison cannot be trusted"):
        store.activate(
            candidate=second,
            expected_generation=1,
            reason="Bad limit",
            **{**_strict(), "max_added_rows": -1},
        )
    with pytest.raises(ReleaseError, match="stale expected"):
        store.activate(candidate=second, expected_generation=0, reason="Stale", **_strict())
    with pytest.raises(ReleaseError, match="already active"):
        store.activate(candidate=first, expected_generation=1, reason="Duplicate", **_strict())
    with pytest.raises(ReleaseError, match="reason"):
        store.activate(candidate=second, expected_generation=1, reason="   ", **_strict())
    assert store.status() == before
    assert len(list(store.events.glob("*.json"))) == 1


def test_rollback_only_to_prior_verified_activation(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, second = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    store.activate(candidate=second, expected_generation=1, reason="Next", **_strict())
    before = store.status()
    preview = store.plan_rollback(to_generation=1, expected_generation=2)
    assert preview.eligible and not preview.applied and preview.candidate == first
    assert store.status() == before
    applied = store.rollback(to_generation=1, expected_generation=2, reason="Revert")
    assert applied.applied
    after = store.status()
    assert after is not None and after.generation == 3 and after.active == first
    assert [event.action for event in after.events] == ["bootstrap", "promote", "rollback"]
    assert after.events[-1].reason == "Revert"
    assert after.events[-1].comparison is None

    with pytest.raises(ReleaseError, match="earlier positive generation"):
        store.rollback(to_generation=3, expected_generation=3, reason="No")
    with pytest.raises(ReleaseError, match="already active"):
        store.rollback(to_generation=1, expected_generation=3, reason="No")
    with pytest.raises(ReleaseError, match="no matching active generation"):
        store.rollback(to_generation=2, expected_generation=2, reason="Stale")
    assert store.status() == after


def test_tamper_evidence_fails_closed_with_sanitized_api_error(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, _ = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    state = store.status()
    assert state is not None
    event_file = store.events / f"{state.event_sha256}.json"
    event_file.write_bytes(event_file.read_bytes() + b" ")
    with pytest.raises(ReleaseError, match="release event"):
        store.status()
    response = TestClient(create_app(root)).get("/v1/ons-pay/releases/active")
    assert response.status_code == 503
    assert response.json() == {"detail": "Active release integrity failure"}
    assert str(root) not in response.text


def test_pointer_symlink_and_missing_curated_data_fail_closed(
    pair: tuple[Path, VintageKey, VintageKey],
    tmp_path: Path,
) -> None:
    root, first, _ = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    original = store.head.read_bytes()
    store.head.unlink()
    external = tmp_path / "outside.json"
    external.write_bytes(original)
    store.head.symlink_to(external)
    with pytest.raises(ReleaseError, match="symlinked active"):
        store.status()
    store.head.unlink()
    store.head.write_bytes(original)
    # A missing pinned table must fail closed rather than selecting another vintage.
    active = store.status()
    assert active is not None
    observations = CuratedCatalog(root).get_vintage(first).artifacts["observations"].path
    observations.rename(observations.with_name(observations.name + ".disabled"))
    with pytest.raises(ReleaseError, match="trusted curated data"):
        store.status()
    assert TestClient(create_app(root)).get("/v1/ons-pay/releases/active").status_code == 503


def test_failed_pointer_swap_keeps_old_release_and_allows_retry(
    pair: tuple[Path, VintageKey, VintageKey],
) -> None:
    root, first, second = pair
    store = ReleaseStore(root)
    _bootstrap(store, first)
    before = store.status()
    assert before is not None
    with (
        patch(
            "data_platform_lab.serving.release_store.os.replace", side_effect=OSError("power loss")
        ),
        pytest.raises(ReleaseError, match="cannot atomically publish"),
    ):
        store.activate(candidate=second, expected_generation=1, reason="Interrupted", **_strict())
    assert store.status() == before
    assert len(list(store.events.glob("*.json"))) == 2  # durable, unreferenced event
    assert store.activate(
        candidate=second, expected_generation=1, reason="Retry", **_strict()
    ).applied
    after = store.status()
    assert after is not None and after.generation == 2


def test_cli_status_preview_apply_breach_and_rollback(
    pair: tuple[Path, VintageKey, VintageKey],
    capsys: pytest.CaptureFixture[str],
) -> None:
    root, first, second = pair
    prefix = ["ons-pay-release", "--data-root", str(root)]
    platform_main([*prefix, "status"])
    assert json.loads(capsys.readouterr().out) == {"active": None}

    activate = [
        *prefix,
        "activate",
        "--candidate-registry-commit",
        COMMIT_A,
        "--candidate-source-sha256",
        first.source_sha256,
        "--expected-generation",
        "0",
        "--bootstrap",
        "--reason",
        "First",
    ]
    platform_main(activate)
    assert json.loads(capsys.readouterr().out)["applied"] is False
    assert not (root / "releases").exists()
    platform_main([*activate, "--apply"])
    assert json.loads(capsys.readouterr().out)["applied"] is True

    promote = [
        *prefix,
        "activate",
        "--candidate-registry-commit",
        COMMIT_B,
        "--candidate-source-sha256",
        second.source_sha256,
        "--expected-generation",
        "1",
        "--reason",
        "Next",
    ]
    for key, value in _strict().items():
        promote.extend(["--" + key.replace("_", "-"), str(value)])
    platform_main(promote)
    assert json.loads(capsys.readouterr().out)["eligible"] is True
    platform_main([*promote, "--apply"])
    assert json.loads(capsys.readouterr().out)["applied"] is True

    rollback = [
        *prefix,
        "rollback",
        "--to-generation",
        "1",
        "--expected-generation",
        "2",
        "--reason",
        "Revert",
    ]
    platform_main(rollback)
    assert json.loads(capsys.readouterr().out)["applied"] is False
    platform_main([*rollback, "--apply"])
    assert json.loads(capsys.readouterr().out)["applied"] is True
    platform_main([*prefix, "status"])
    assert json.loads(capsys.readouterr().out)["active"]["generation"] == 3

    with pytest.raises(SystemExit) as error:
        platform_main([*promote, "--apply"])
    assert error.value.code == 2
    assert "stale expected release generation" in capsys.readouterr().err


def test_cli_breached_policy_exits_one(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root, first = _curate(tmp_path, commit=COMMIT_A)
    updated = PAY_CSV.replace("2022-04-01,100,Pounds", "2022-04-01,105,Pounds")
    _, second = _curate(tmp_path, commit=COMMIT_B, payload=updated.encode("utf-8-sig"))
    store = ReleaseStore(root)
    _bootstrap(store, _key(first))
    args = [
        "ons-pay-release",
        "--data-root",
        str(root),
        "activate",
        "--candidate-registry-commit",
        COMMIT_B,
        "--candidate-source-sha256",
        _key(second).source_sha256,
        "--expected-generation",
        "1",
        "--reason",
        "Attempted",
        "--apply",
    ]
    for key, value in _strict().items():
        args.extend(["--" + key.replace("_", "-"), str(value)])
    with pytest.raises(SystemExit) as error:
        platform_main(args)
    assert error.value.code == 1
    plan = json.loads(capsys.readouterr().out)
    assert plan["eligible"] is False and plan["applied"] is False
    assert len(list(store.events.glob("*.json"))) == 1
