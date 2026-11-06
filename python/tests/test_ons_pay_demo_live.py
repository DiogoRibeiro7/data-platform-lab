"""Opt-in smoke of the documented host entry point and packaged Docker workflow."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("ONS_PAY_LIVE_DEMO") != "1",
    reason="requires explicit opt-in, local Docker Compose and image-build access",
)


def _check_report(output: Path, *, recovery: bool) -> dict[str, object]:
    """Validate data lineage, gate outcomes, durable hashes and scoped service cleanup."""
    report = json.loads((output / "report.json").read_text())
    launcher = json.loads((output / "launcher.json").read_text())
    assert report["status"] == launcher["status"] == launcher["cleanup"] == "success"
    assert report["synthetic"] is True
    assert report["release_activation"] == "not_requested"
    assert report["exercise_recovery"] is recovery
    assert report["elapsed_seconds"] >= 0
    assert all(stage["status"] == "success" for stage in report["stages"])
    assert all(stage["elapsed_seconds"] >= 0 for stage in report["stages"])
    assert not list(output.rglob("active.json"))
    ingestion = next(stage for stage in report["stages"] if stage["stage"] == "verify_and_ingest")
    assert ingestion["committed_blob_verified"]
    assert ingestion["source_checksum_verified"] and ingestion["destination_checksum_verified"]
    curated = next(
        stage for stage in report["stages"] if stage["stage"] == "curate_silver_and_gold"
    )
    assert curated["source_rows"] == 8 and curated["rows_without_pay"] == 1
    for artifact in curated["outputs"]:
        assert (
            hashlib.sha256((output / artifact["path"]).read_bytes()).hexdigest()
            == artifact["sha256"]
        )
    plans = [stage for stage in report["stages"] if stage["stage"] == "plan"]
    assert {(plan["table"], plan["sink"]) for plan in plans} == {
        (table, sink)
        for table in ("geography_year", "local_authority_yoy")
        for sink in ("postgresql", "mongodb")
    }
    fingerprints = {}
    for stage in report["stages"]:
        if "publication" not in stage:
            continue
        result = stage["publication"]
        key = (stage["table"], stage["sink"])
        assert result["vintage"] == report["source"]["vintage"]
        assert (
            result["counts"]["source"]
            == result["counts"]["applied"]
            == result["counts"]["observed"]
        )
        if stage["stage"] == "controlled_drift":
            assert recovery and stage["expected_rejection_verified"]
            assert result["status"] == "rejected" and result["acceptance"] is None
            assert result["reconciliation"]["changed_record_count"] == 1
        else:
            assert result["status"] == "accepted" and result["acceptance"] is not None
            if stage["stage"] == "publish":
                fingerprints[key] = result["fingerprints"]
            else:
                assert result["fingerprints"] == fingerprints[key]
    evidence = list((output / "work/curated/manifests/ons_pay_publication").glob("*.json"))
    assert len(evidence) == (16 if recovery else 8)
    outcomes = []
    for path in evidence:
        assert hashlib.sha256(path.read_bytes()).hexdigest() == path.stem
        record = json.loads(path.read_text())
        outcomes.append(record["outcome"])
        assert record["vintage"] == report["source"]["vintage"]
        assert (record["acceptance"] is not None) == (record["outcome"] == "accepted")
    assert outcomes.count("rejected") == (4 if recovery else 0)
    assert outcomes.count("accepted") == (12 if recovery else 8)
    remaining = subprocess.run(
        [
            "docker",
            "--context",
            "default",
            "ps",
            "--all",
            "--quiet",
            "--filter",
            f"label=com.docker.compose.project={launcher['compose_project']}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert remaining.stdout.strip() == ""
    return {
        "source": report["source"],
        "plans": sorted(
            (plan["table"], plan["sink"], plan["source_plan_sha256"]) for plan in plans
        ),
    }


def test_documented_demo_repeats_with_stable_pins_and_preserves_unrelated_files(
    tmp_path: Path,
) -> None:
    """Run both explicit recovery and the default non-mutating publication exercise."""
    repository = Path(__file__).resolve().parents[2]
    output_dir = tmp_path / "demo runs with spaces"
    output_dir.mkdir()
    sentinel = output_dir / "keep.txt"
    sentinel.write_text("unrelated local content")
    reports = []
    for recovery in (True, False):
        arguments = [
            sys.executable,
            str(repository / "scripts/ons_pay_publishing_demo.py"),
            "--output-dir",
            str(output_dir),
        ]
        if recovery:
            arguments.append("--exercise-recovery")
        completed = subprocess.run(arguments, cwd=repository, capture_output=True, text=True)
        assert completed.returncode == 0, (completed.stdout, completed.stderr)
        result = json.loads(completed.stdout)
        reports.append(_check_report(Path(result["artifacts"]), recovery=recovery))
    assert reports[0] == reports[1]
    assert sentinel.read_text() == "unrelated local content"
    assert len(list(output_dir.glob("run-*"))) == 2
