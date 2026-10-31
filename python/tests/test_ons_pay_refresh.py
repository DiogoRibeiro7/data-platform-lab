"""Offline, Git-pinned source refresh/backfill and durable audit regression tests."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import date
from pathlib import Path

import pytest

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.orchestration.ons_pay_refresh import (
    RefreshError,
    RefreshSource,
    plan_ons_pay_refresh,
    refresh_ons_pay,
)
from data_platform_lab.serving.catalog import CuratedCatalog
from data_platform_lab.storage import LocalBlobStore

DATASET = "ons-gross-median-weekly-pay"
SOURCE_PATH = f"datasets/{DATASET}/raw/gross-median-weekly-pay.csv"
PAY_CSV = (
    "areacd,areanm,geography,indicator,period,observation,measure,unit,lci_95,uci_95,status,notes\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2022-04-01,100,Pounds,£,90,110,,\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2023-04-01,110,Pounds,£,100,120,,\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2024-04-01,,Pounds,£,,,x,\n"
    "A001,Area A,Local Authority District,Gross median weekly pay,"
    "2025-04-01,130,Pounds,£,120,140,,\n"
    "B001,Area B,Local Authority District,Gross median weekly pay,2022-04-01,200,Pounds,£,,,,\n"
    "B001,Area B,Local Authority District,Gross median weekly pay,2024-04-01,220,Pounds,£,,,,\n"
    "R001,Region A,Region,Gross median weekly pay,2023-04-01,500,Pounds,£,,,,\n"
)
AS_OF = date(2026, 10, 31)


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(root), *args], check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def _source(
    tmp_path: Path,
    *,
    name: str = "first",
    csv_text: str = PAY_CSV,
    retrieved_at: str = "2026-10-20",
) -> RefreshSource:
    """Build a real committed source plus verified canonical handoff JSON."""
    root = tmp_path / f"registry-{name}"
    source_file = root / SOURCE_PATH
    source_file.parent.mkdir(parents=True)
    payload = csv_text.encode("utf-8-sig")
    source_file.write_bytes(payload)
    _git(root, "init", "-q")
    _git(root, "add", ".")
    _git(
        root,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.org",
        "commit",
        "-qm",
        f"source-{name}",
    )
    commit = _git(root, "rev-parse", "HEAD")
    sha = hashlib.sha256(payload).hexdigest()
    manifest = {
        "manifest_version": 1,
        "dataset_id": DATASET,
        "title": "Synthetic ONS gross median weekly pay",
        "lifecycle_status": "active",
        "registry": {"repository": "DiogoRibeiro7/data", "commit": commit},
        "source": {
            "publisher": "Office for National Statistics",
            "url": "https://example.org/ons",
            "snapshot": f"synthetic-{name}",
            "retrieved_at": retrieved_at,
        },
        "license": {
            "name": "Open Government Licence v3.0",
            "url": "https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
            "redistribution": "allowed",
        },
        "citation": "ONS, Synthetic weekly pay",
        "local_bytes_verified": True,
        "objects": [
            {
                "source_path": SOURCE_PATH,
                "source_uri": (
                    f"https://raw.githubusercontent.com/DiogoRibeiro7/data/{commit}/{SOURCE_PATH}"
                ),
                "sha256": sha,
                "role": "raw",
                "format": "csv",
                "storage_key": f"canonical/{DATASET}/{sha}/raw/gross-median-weekly-pay.csv",
                "size_bytes": len(payload),
            }
        ],
    }
    manifest_path = tmp_path / f"handoff-{name}.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return RefreshSource(manifest_path, root)


def _run(
    tmp_path: Path,
    *sources: RefreshSource,
    as_of: date = AS_OF,
    max_age_days: int | None = None,
) -> tuple[LocalBlobStore, Path, object]:
    store = LocalBlobStore(tmp_path / "object-store")
    root = tmp_path / "curated"
    result = refresh_ons_pay(
        sources=sources,
        store=store,
        output_root=root,
        as_of=as_of,
        max_source_age_days=max_age_days,
    )
    return store, root, result


def _audit(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_plan_is_read_only_and_preserves_explicit_source_order(tmp_path: Path) -> None:
    first = _source(tmp_path, name="first")
    second = _source(tmp_path, name="second", csv_text=PAY_CSV.replace("200,Pounds", "205,Pounds"))
    plan = plan_ons_pay_refresh([second, first], max_source_age_days=30, as_of=AS_OF)
    assert len(plan) == 2
    assert [item.source for item in plan] == [second, first]
    assert [item.age_days for item in plan] == [11, 11]
    assert plan[0].source_sha256 != plan[1].source_sha256
    assert not (tmp_path / "curated").exists()
    assert not (tmp_path / "object-store").exists()


def test_refresh_end_to_end_audit_and_serving_catalog(tmp_path: Path) -> None:
    source = _source(tmp_path)
    _, root, result = _run(tmp_path, source, max_age_days=30)
    assert len(result.sources) == 1
    item = result.sources[0]
    assert item.source_rows == 7
    assert item.rows_with_pay == 6
    assert item.rows_without_pay == 1
    assert item.objects_published == 1
    assert item.parquet_published == 3
    assert item.silver_rows == 7
    assert item.source_age_days == 11
    assert item.ingestion_receipt_path.is_file()
    assert item.curation_receipt_path.is_file()

    audit = _audit(result.receipt_path)
    assert audit["pipeline_name"] == "ons_pay_refresh"
    assert audit["schema_version"] == 1
    assert audit["status"] == "success"
    assert audit["policy"] == {
        "max_source_age_days": 30,
        "as_of_utc_date": "2026-10-31",
    }
    assert audit["metrics"]["vintages_completed"] == 1
    assert audit["metrics"]["source_rows"] == 7
    assert audit["metrics"]["parquet_published"] == 3
    assert audit["failure"] is None
    assert len(CuratedCatalog(root).list_vintages()) == 1


def test_replay_verifies_and_reuses_immutable_outputs(tmp_path: Path) -> None:
    source = _source(tmp_path)
    store, root, first = _run(tmp_path, source)
    first_item = first.sources[0]
    outputs = _audit(first_item.curation_receipt_path)["outputs"]
    paths = [Path(item["path"]) for item in outputs]
    mtimes = [path.stat().st_mtime_ns for path in paths]

    second = refresh_ons_pay(sources=[source], store=store, output_root=root, as_of=AS_OF)
    second_item = second.sources[0]
    assert second_item.objects_published == 0
    assert second_item.objects_reused == 1
    assert second_item.parquet_published == 0
    assert second_item.parquet_reused == 3
    assert [path.stat().st_mtime_ns for path in paths] == mtimes
    assert first.receipt_path != second.receipt_path
    assert len(CuratedCatalog(root).list_vintages()) == 1


def test_two_source_historical_backfill_is_explicit(tmp_path: Path) -> None:
    first = _source(tmp_path, name="old", retrieved_at="2024-05-01")
    second = _source(
        tmp_path,
        name="new",
        retrieved_at="2026-10-20T21:20:00Z",
        csv_text=PAY_CSV.replace("200,Pounds", "205,Pounds"),
    )
    store, root, result = _run(tmp_path, first, second)
    assert len(result.sources) == 2
    assert result.sources[0].source_age_days > 365
    assert result.sources[1].source_age_days == 11
    assert result.sources[0].source_sha256 != result.sources[1].source_sha256
    assert len(CuratedCatalog(root).list_vintages()) == 2
    audit = _audit(result.receipt_path)
    assert audit["metrics"]["vintages_completed"] == 2
    assert audit["metrics"]["source_rows"] == 14  # Across vintages, not unique rows.
    assert audit["metrics"]["parquet_published"] == 6

    replay = refresh_ons_pay(sources=[first, second], store=store, output_root=root, as_of=AS_OF)
    assert all(item.parquet_reused == 3 for item in replay.sources)
    assert len(CuratedCatalog(root).list_vintages()) == 2


def test_equal_source_bytes_at_distinct_commits_preserve_two_vintages(tmp_path: Path) -> None:
    first = _source(tmp_path, name="before")
    second = _source(tmp_path, name="after")
    _, root, result = _run(tmp_path, first, second)
    assert result.sources[0].source_sha256 == result.sources[1].source_sha256
    assert result.sources[0].registry_commit != result.sources[1].registry_commit
    assert result.sources[0].objects_published == 1
    assert result.sources[1].objects_reused == 1
    assert result.sources[0].parquet_published == 3
    assert result.sources[1].parquet_published == 3
    assert len(CuratedCatalog(root).list_vintages()) == 2


def test_duplicate_pin_fails_preflight_with_failure_receipt(tmp_path: Path) -> None:
    source = _source(tmp_path)
    store = LocalBlobStore(tmp_path / "objects")
    root = tmp_path / "curated"
    with pytest.raises(RefreshError, match="failed in preflight") as failure:
        refresh_ons_pay(sources=[source, source], store=store, output_root=root, as_of=AS_OF)
    audit = _audit(failure.value.receipt_path)
    assert audit["status"] == "failed"
    assert audit["failure"]["phase"] == "preflight"
    assert audit["failure"]["source_index"] == 1
    assert audit["metrics"]["vintages_completed"] == 0
    assert store.list_objects() == []
    assert not (root / "silver").exists()


def test_stale_source_is_rejected_before_publishing(tmp_path: Path) -> None:
    old = _source(tmp_path, retrieved_at="2024-01-01")
    with pytest.raises(RefreshError, match="freshness SLA breached"):
        plan_ons_pay_refresh([old], max_source_age_days=30, as_of=AS_OF)
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(RefreshError) as failure:
        refresh_ons_pay(
            sources=[old],
            store=store,
            output_root=tmp_path / "curated",
            max_source_age_days=30,
            as_of=AS_OF,
        )
    audit = _audit(failure.value.receipt_path)
    assert audit["status"] == "failed"
    assert audit["failure"]["phase"] == "preflight"
    assert store.list_objects() == []


@pytest.mark.parametrize(
    "retrieved_at",
    ["2026-11-01", "2026-10-20T12:30:00", "not-a-date"],
)
def test_invalid_or_future_retrieval_date(tmp_path: Path, retrieved_at: str) -> None:
    source = _source(tmp_path, retrieved_at=retrieved_at)
    with pytest.raises(RefreshError, match="invalid handoff"):
        plan_ons_pay_refresh([source], as_of=AS_OF)


@pytest.mark.parametrize("max_days", [-1, True, 0.5])
def test_invalid_freshness_policy(tmp_path: Path, max_days: object) -> None:
    source = _source(tmp_path)
    with pytest.raises(RefreshError, match="max_source_age_days"):
        plan_ons_pay_refresh([source], max_source_age_days=max_days, as_of=AS_OF)


def test_broken_manifest_or_wrong_dataset_fails_preflight(tmp_path: Path) -> None:
    source = _source(tmp_path)
    source.manifest_path.write_text("not json", encoding="utf-8")
    with pytest.raises(RefreshError, match="invalid handoff"):
        plan_ons_pay_refresh([source], as_of=AS_OF)
    source = _source(tmp_path, name="wrong-dataset")
    document = _audit(source.manifest_path)
    document["dataset_id"] = "other-dataset"
    source.manifest_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RefreshError, match="invalid handoff"):
        plan_ons_pay_refresh([source], as_of=AS_OF)


def test_failed_second_source_records_partial_backfill_then_replays(tmp_path: Path) -> None:
    first = _source(tmp_path, name="first")
    second = _source(tmp_path, name="second", csv_text=PAY_CSV.replace("200,Pounds", "205,Pounds"))
    wrong_checkout = RefreshSource(second.manifest_path, first.registry_root)
    store = LocalBlobStore(tmp_path / "objects")
    root = tmp_path / "curated"
    with pytest.raises(RefreshError, match="failed in ingestion") as failure:
        refresh_ons_pay(sources=[first, wrong_checkout], store=store, output_root=root, as_of=AS_OF)
    audit = _audit(failure.value.receipt_path)
    assert audit["status"] == "failed"
    assert audit["failure"]["source_index"] == 1
    assert audit["failure"]["phase"] == "ingestion"
    assert audit["metrics"]["vintages_completed"] == 1
    assert len(CuratedCatalog(root).list_vintages()) == 1

    retry = refresh_ons_pay(sources=[first, second], store=store, output_root=root, as_of=AS_OF)
    assert retry.sources[0].objects_reused == 1
    assert retry.sources[0].parquet_reused == 3
    assert retry.sources[1].objects_published == 1
    assert len(CuratedCatalog(root).list_vintages()) == 2


def test_modified_git_blob_fails_in_ingestion(tmp_path: Path) -> None:
    source = _source(tmp_path)
    (source.registry_root / SOURCE_PATH).write_bytes(b"modified without commit")
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(RefreshError, match="failed in ingestion") as failure:
        refresh_ons_pay(
            sources=[source], store=store, output_root=tmp_path / "curated", as_of=AS_OF
        )
    assert _audit(failure.value.receipt_path)["failure"]["phase"] == "ingestion"
    assert store.list_objects() == []


def test_source_quality_failure_retains_ingestion_receipt_for_repair(tmp_path: Path) -> None:
    source = _source(tmp_path, csv_text=PAY_CSV.replace("200,Pounds", "-20,Pounds"))
    store = LocalBlobStore(tmp_path / "objects")
    root = tmp_path / "curated"
    with pytest.raises(RefreshError, match="failed in curation") as failure:
        refresh_ons_pay(sources=[source], store=store, output_root=root, as_of=AS_OF)
    audit = _audit(failure.value.receipt_path)
    assert audit["failure"]["phase"] == "curation"
    assert Path(audit["failure"]["ingestion_receipt_path"]).is_file()
    assert len(store.list_objects()) == 1
    assert not (root / "gold").exists()


def test_corrupt_immutable_output_stops_replay(tmp_path: Path) -> None:
    source = _source(tmp_path)
    store, root, result = _run(tmp_path, source)
    output = _audit(result.sources[0].curation_receipt_path)["outputs"][1]
    path = Path(output["path"])
    path.write_bytes(b"corrupted")
    with pytest.raises(RefreshError, match="failed in curation") as failure:
        refresh_ons_pay(sources=[source], store=store, output_root=root, as_of=AS_OF)
    assert _audit(failure.value.receipt_path)["failure"]["phase"] == "curation"
    assert path.read_bytes() == b"corrupted"  # Never overwrite an immutable conflict.


def test_refuses_to_write_audits_inside_canonical_checkout(tmp_path: Path) -> None:
    source = _source(tmp_path)
    store = LocalBlobStore(tmp_path / "objects")
    with pytest.raises(RefreshError, match="outside each canonical"):
        refresh_ons_pay(
            sources=[source], store=store, output_root=source.registry_root, as_of=AS_OF
        )
    assert not (source.registry_root / "manifests").exists()


def test_cli_rejects_object_store_inside_canonical_checkout_before_write(tmp_path: Path) -> None:
    source = _source(tmp_path)
    forbidden = source.registry_root / "forbidden-objects"
    with pytest.raises(SystemExit) as failure:
        platform_main(
            [
                "ons-pay-refresh",
                "--source",
                str(source.manifest_path),
                str(source.registry_root),
                "--object-root",
                str(forbidden),
                "--output-root",
                str(tmp_path / "curated"),
            ]
        )
    assert failure.value.code == 2
    assert not forbidden.exists()


def test_dry_run_cli_has_no_side_effects(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = _source(tmp_path)
    object_root = tmp_path / "objects"
    output_root = tmp_path / "curated"
    platform_main(
        [
            "ons-pay-refresh",
            "--source",
            str(source.manifest_path),
            str(source.registry_root),
            "--object-root",
            str(object_root),
            "--output-root",
            str(output_root),
            "--dry-run",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "planned"
    assert result["writes"] is False
    assert len(result["vintages"]) == 1
    assert not object_root.exists()
    assert not output_root.exists()


def test_cli_run_reports_success_and_receipt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = _source(tmp_path)
    platform_main(
        [
            "ons-pay-refresh",
            "--source",
            str(source.manifest_path),
            str(source.registry_root),
            "--object-root",
            str(tmp_path / "objects"),
            "--output-root",
            str(tmp_path / "curated"),
        ]
    )
    reported = json.loads(capsys.readouterr().out)
    assert reported["status"] == "success"
    assert reported["objects_published"] == 1
    assert reported["parquet_published"] == 3
    assert _audit(Path(reported["receipt_path"]))["status"] == "success"


def test_cli_failure_exits_nonzero_and_preserves_audit(tmp_path: Path) -> None:
    source = _source(tmp_path, retrieved_at="2024-01-01")
    with pytest.raises(SystemExit) as failure:
        platform_main(
            [
                "ons-pay-refresh",
                "--source",
                str(source.manifest_path),
                str(source.registry_root),
                "--object-root",
                str(tmp_path / "objects"),
                "--output-root",
                str(tmp_path / "curated"),
                "--max-source-age-days",
                "30",
            ]
        )
    assert failure.value.code == 1
    receipts = list((tmp_path / "curated" / "manifests").glob("ons_pay_refresh_*.json"))
    assert len(receipts) == 1
    assert _audit(receipts[0])["status"] == "failed"
