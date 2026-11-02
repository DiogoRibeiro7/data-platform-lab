"""Versioned alert policy files, deterministic provenance and CLI overrides."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from test_ons_pay_release_health import _prepare

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.observability.ons_pay_alert_policy import (
    AlertPolicy,
    AlertPolicyError,
    load_alert_policy,
)
from data_platform_lab.observability.ons_pay_history_alerts import (
    evaluate_ons_pay_history_alerts,
    format_history_alerts_prometheus,
)
from data_platform_lab.observability.ons_pay_release_health import assess_ons_pay_release
from data_platform_lab.observability.ons_pay_release_history import ReleaseHealthArchive
from data_platform_lab.serving.release_store import ReleaseStore

_POLICY = {
    "schema_version": 1,
    "name": "ons-pay-example",
    "last": 1,
    "max_assessment_age_hours": 24,
    "alert_after_unhealthy": 2,
    "max_unhealthy_share_percent": 50,
    "max_missing_pay_increase_pp": 3,
    "max_cumulative_missing_pay_drift_pp": 8,
}


def _write(path: Path, config: object) -> Path:
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def _record_healthy(root: Path) -> None:
    report = assess_ons_pay_release(
        data_root=root, max_missing_pay_percent=40, now=datetime.now(UTC)
    )
    assert report.active_monitor.status == "healthy"
    ReleaseHealthArchive(root).record(report)


def test_policy_file_is_independent_of_key_order_and_whitespace(tmp_path: Path) -> None:
    compact = _write(tmp_path / "first.json", _POLICY)
    reordered = tmp_path / "second.json"
    reordered.write_text(
        json.dumps(dict(reversed(list(_POLICY.items()))), indent=4) + "\n", encoding="utf-8"
    )
    first = load_alert_policy(compact)
    second = load_alert_policy(reordered)
    assert first == second
    assert first.to_dict() == _POLICY
    assert first.sha256 == second.sha256
    assert re.fullmatch(r"[0-9a-f]{64}", first.sha256)
    assert first == load_alert_policy(compact)  # no file is modified by validation


def test_effective_policy_override_changes_digest_and_preserves_base() -> None:
    base = AlertPolicy(
        name="ons-pay-example", last=7, alert_after_unhealthy=3, max_missing_pay_increase_pp=2.5
    )
    unchanged = base.with_overrides()
    overridden = base.with_overrides(last=3, alert_after_unhealthy=1)
    assert unchanged == base
    assert unchanged.sha256 == base.sha256
    assert overridden.name == base.name
    assert overridden.last == 3 and overridden.alert_after_unhealthy == 1
    assert overridden.max_missing_pay_increase_pp == 2.5
    assert overridden.sha256 != base.sha256
    assert base.last == 7 and base.alert_after_unhealthy == 3
    assert AlertPolicy().to_dict()["last"] == 50  # backward-compatible default


def test_example_config_is_valid_and_needs_no_active_release(
    capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    example = Path(__file__).resolve().parents[2] / "configs/ons-pay-alert-policy.example.json"
    policy = load_alert_policy(example)
    assert policy.name == "ons-pay-quality-example" and policy.last == 50

    absent_data_root = tmp_path / "no-release"
    with patch(
        "data_platform_lab.serving.release_store.ReleaseStore",
        side_effect=AssertionError("validation must not inspect release data"),
    ):
        platform_main(
            [
                "ons-pay-release",
                "--data-root",
                str(absent_data_root),
                "validate-alert-policy",
                "--policy-file",
                str(example),
            ]
        )
    result = json.loads(capsys.readouterr().out)
    assert result == {"policy": policy.to_dict(), "policy_sha256": policy.sha256}
    assert not absent_data_root.exists()


def test_policy_and_explicit_cli_overrides_are_reflected_in_json_and_prometheus(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root, _, _ = _prepare(tmp_path)
    _record_healthy(root)
    archive = ReleaseHealthArchive(root)
    before_files = {p.name: p.read_bytes() for p in archive.records.glob("*.json")}
    store = ReleaseStore(root)
    before_head = store.head.read_bytes()
    path = _write(tmp_path / "policy.json", _POLICY)
    before_policy = path.read_bytes()
    prefix = ["ons-pay-release", "--data-root", str(root), "alerts", "--policy-file", str(path)]

    # With a single healthy record, configured streak/share/age pass and
    # cross-generation drift remains unavailable until more releases are recorded.
    with pytest.raises(SystemExit) as no_drift:
        platform_main(prefix)
    assert no_drift.value.code == 1
    original = json.loads(capsys.readouterr().out)
    assert original["status"] == "unavailable"
    assert original["policy"] == _POLICY
    assert original["policy_sha256"] == load_alert_policy(path).sha256

    # CLI flags override the file's observation window and assessment SLA.
    with pytest.raises(SystemExit) as overridden:
        platform_main([*prefix, "--last", "2", "--max-assessment-age-hours", "0"])
    assert overridden.value.code == 1
    changed = json.loads(capsys.readouterr().out)
    assert changed["status"] == "alert"
    assert changed["policy"]["last"] == 2
    assert changed["policy"]["max_assessment_age_hours"] == 0.0
    assert changed["policy"]["alert_after_unhealthy"] == 2
    assert changed["policy_sha256"] != original["policy_sha256"]
    assert (
        next(c for c in changed["checks"] if c["name"] == "assessment_age_hours")["state"]
        == "alert"
    )

    with pytest.raises(SystemExit) as prometheus:
        platform_main([*prefix, "--format", "prometheus"])
    assert prometheus.value.code == 1
    output = capsys.readouterr().out
    assert (
        'dpl_ons_pay_history_policy_info{policy_name="ons-pay-example",'
        f'policy_sha256="{original["policy_sha256"]}"}} 1'
    ) in output
    assert before_policy == path.read_bytes()
    assert before_head == store.head.read_bytes()
    assert before_files == {p.name: p.read_bytes() for p in archive.records.glob("*.json")}


def test_api_evaluation_includes_canonical_effective_policy(tmp_path: Path) -> None:
    baseline = AlertPolicy(name="quality-review", last=9, alert_after_unhealthy=4)
    report = evaluate_ons_pay_history_alerts(
        data_root=tmp_path / "absent", policy=baseline, alert_after_unhealthy=2
    )
    assert report.policy == baseline.with_overrides(alert_after_unhealthy=2)
    assert report.policy_sha256 == report.policy.sha256
    assert report.to_dict()["policy_sha256"] == report.policy_sha256
    assert f'policy_sha256="{report.policy_sha256}"' in format_history_alerts_prometheus(report)
    assert baseline.alert_after_unhealthy == 4


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ('{"schema_version":1,"name":"one","name":"two"}', "duplicate"),
        ('{"schema_version":1,"name":"one","max_missing_rate":3}', "unknown"),
        ('{"schema_version":1}', "missing"),
        ('{"name":"one"}', "missing"),
        ('{"schema_version":2,"name":"one"}', "schema_version"),
        ('{"schema_version":true,"name":"one"}', "schema_version"),
        ('{"schema_version":1,"name":"Bad Name"}', "name"),
        ('{"schema_version":1,"name":"one","last":true}', "last"),
        ('{"schema_version":1,"name":"one","last":0}', "last"),
        ('{"schema_version":1,"name":"one","alert_after_unhealthy":0}', "alert_after"),
        ('{"schema_version":1,"name":"one","max_unhealthy_share_percent":101}', "share"),
        ('{"schema_version":1,"name":"one","max_missing_pay_increase_pp":-1}', "increase"),
        ('{"schema_version":1,"name":"one","max_assessment_age_hours":"4"}', "age"),
        (
            '{"schema_version":1,"name":"one","max_cumulative_missing_pay_drift_pp":1e400}',
            "cumulative",
        ),
        ('{"schema_version":1,"name":"one","max_assessment_age_hours":NaN}', "non-finite"),
        (
            '{"schema_version":1,"name":"one","unused":' + "[" * 12_000 + "0" + "]" * 12_000 + "}",
            "UTF-8 JSON",
        ),
        ("[]", "JSON object"),
        ("{bad JSON", "UTF-8 JSON"),
    ],
)
def test_malformed_policies_fail_closed(
    tmp_path: Path, payload: str, expected: str, capsys: pytest.CaptureFixture[str]
) -> None:
    policy_file = tmp_path / "invalid.json"
    policy_file.write_text(payload, encoding="utf-8")
    with pytest.raises(AlertPolicyError, match=expected):
        load_alert_policy(policy_file)
    with pytest.raises(SystemExit) as exit_code:
        platform_main(
            ["ons-pay-release", "validate-alert-policy", "--policy-file", str(policy_file)]
        )
    assert exit_code.value.code == 2
    assert capsys.readouterr().err
    assert list(tmp_path.iterdir()) == [policy_file]


def test_oversized_non_utf8_symlink_and_missing_policy_are_rejected(tmp_path: Path) -> None:
    too_large = tmp_path / "large.json"
    too_large.write_bytes(b" " * 70_000)
    non_utf8 = tmp_path / "bad-encoding.json"
    non_utf8.write_bytes(b"\xff\xfe")
    link = tmp_path / "linked.json"
    link.symlink_to(non_utf8)
    for path, message in (
        (too_large, "64 KiB"),
        (non_utf8, "UTF-8"),
        (link, "symlink"),
        (tmp_path / "missing.json", "cannot read"),
        (tmp_path, "cannot read"),
    ):
        with pytest.raises(AlertPolicyError, match=message):
            load_alert_policy(path)


def test_invalid_cli_override_rejects_before_history_or_export(tmp_path: Path) -> None:
    policy_file = _write(tmp_path / "policy.json", _POLICY)
    output = tmp_path / "should-not-exist.prom"
    root = tmp_path / "absent"
    with pytest.raises(SystemExit) as failed:
        platform_main(
            [
                "ons-pay-release",
                "--data-root",
                str(root),
                "alerts",
                "--policy-file",
                str(policy_file),
                "--last",
                "0",
                "--format",
                "prometheus",
                "--textfile",
                str(output),
            ]
        )
    assert failed.value.code == 2
    assert not output.exists() and not root.exists()


def test_cli_override_cannot_mask_invalid_policy_file(tmp_path: Path) -> None:
    policy_file = _write(tmp_path / "broken-policy.json", {**_POLICY, "last": 0})
    root = tmp_path / "absent"
    with pytest.raises(SystemExit) as failed:
        platform_main(
            [
                "ons-pay-release",
                "--data-root",
                str(root),
                "alerts",
                "--policy-file",
                str(policy_file),
                "--last",
                "50",
            ]
        )
    assert failed.value.code == 2
    assert not root.exists()
