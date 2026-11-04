"""Read-only SQL/document publishing plans from real curated ONS Gold Parquet."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
from test_ons_pay_serving import COMMIT_A, COMMIT_B, _curate, _pin

from data_platform_lab.cli.main import main as platform_main
from data_platform_lab.serving.catalog import (
    CatalogError,
    CuratedCatalog,
    VintageKey,
    VintageNotFound,
)
from data_platform_lab.serving.publish_plan import (
    _TABLES,
    PublishPlanError,
    _canonical_json,
    _convert_row,
    _scalar,
    plan_ons_pay_publishing,
)


@pytest.fixture
def curated(tmp_path: Path) -> tuple[Path, VintageKey]:
    """Produce real Silver/Gold Parquet and a successful pinned curation receipt."""
    root, receipt = _curate(tmp_path)
    pins = _pin(receipt)
    return root, VintageKey(1, pins["registry_commit"], pins["source_sha256"])


def test_geography_year_flat_and_document_models_share_stable_ids(
    curated: tuple[Path, VintageKey],
) -> None:
    root, key = curated
    flat = plan_ons_pay_publishing(
        data_root=root, vintage=key, table="geography_year", shape="relational"
    )
    docs = plan_ons_pay_publishing(
        data_root=root, vintage=key, table="geography_year", shape="document"
    )
    assert flat.schema_version == docs.schema_version == 1
    assert flat.dataset_id == docs.dataset_id == "ons-gross-median-weekly-pay"
    assert flat.table == docs.table == "geography_year"
    assert flat.source_rows == docs.source_rows == 5
    assert len(flat.records) == len(docs.records) == 5
    assert [item["record_id"] for item in flat.records] == [item["_id"] for item in docs.records]
    assert len(set(str(item["record_id"]) for item in flat.records)) == 5
    assert all(item["model_version"] == 1 for item in flat.records)
    assert [item["pay_year"] for item in flat.records[:4]] == [2022, 2023, 2024, 2025]
    assert all(item["registry_commit"] == COMMIT_A for item in flat.records)
    assert all(item["source_sha256"] == key.source_sha256 for item in flat.records)
    assert all(item["vintage"]["registry_commit"] == COMMIT_A for item in docs.records)
    assert docs.records[0]["data"]["geography"] == "Local Authority District"
    assert docs.records[0]["schema_version"] == 1
    assert flat.source_artifact_sha256 == docs.source_artifact_sha256
    assert flat.records_sha256 != docs.records_sha256


def test_local_authority_documents_preserve_null_and_adjacent_yoy(
    curated: tuple[Path, VintageKey],
) -> None:
    root, key = curated
    docs = plan_ons_pay_publishing(
        data_root=root, vintage=key, table="local_authority_yoy", shape="document"
    )
    flat = plan_ons_pay_publishing(
        data_root=root, vintage=key, table="local_authority_yoy", shape="relational"
    )
    assert docs.source_rows == flat.source_rows == 6
    assert [item["_id"] for item in docs.records] == [item["record_id"] for item in flat.records]
    rows = {
        (item["data"]["area_code"], item["data"]["pay_year"]): item["data"] for item in docs.records
    }
    suppressed = rows["A001", 2024]
    assert suppressed["reference_date"] == "2024-04-01"
    assert suppressed["weekly_pay_gbp"] is None
    assert suppressed["status_code"] == "x"
    assert suppressed["yoy_nominal_pct"] is None
    assert rows["A001", 2023]["yoy_nominal_pct"] == pytest.approx(10)
    assert rows["A001", 2025]["yoy_nominal_pct"] is None
    assert rows["B001", 2024]["previous_year"] == 2022
    assert rows["B001", 2024]["yoy_nominal_pct"] is None
    assert all(item["vintage"]["source_sha256"] == key.source_sha256 for item in docs.records)


@pytest.mark.parametrize("shape", ["relational", "document"])
def test_plan_is_deterministic_and_hashes_exact_projected_records(
    curated: tuple[Path, VintageKey], shape: str
) -> None:
    root, key = curated
    params = {"data_root": root, "vintage": key, "table": "geography_year", "shape": shape}
    first = plan_ons_pay_publishing(**params)  # type: ignore[arg-type]
    second = plan_ons_pay_publishing(**params)  # type: ignore[arg-type]
    assert first.to_dict() == second.to_dict()
    assert first.records_sha256 == hashlib.sha256(_canonical_json(list(first.records))).hexdigest()
    assert first.records_sha256 == second.records_sha256
    assert len(first.records_sha256) == 64


def test_different_vintages_never_share_record_ids(tmp_path: Path) -> None:
    root, first_receipt = _curate(tmp_path, commit=COMMIT_A)
    _, second_receipt = _curate(tmp_path, commit=COMMIT_B)
    first_pins = _pin(first_receipt)
    second_pins = _pin(second_receipt)
    first = plan_ons_pay_publishing(
        data_root=root,
        vintage=VintageKey(1, first_pins["registry_commit"], first_pins["source_sha256"]),
        table="geography_year",
        shape="relational",
    )
    second = plan_ons_pay_publishing(
        data_root=root,
        vintage=VintageKey(1, second_pins["registry_commit"], second_pins["source_sha256"]),
        table="geography_year",
        shape="relational",
    )
    assert [row["geography"] for row in first.records] == [
        row["geography"] for row in second.records
    ]
    assert [row["pay_year"] for row in first.records] == [row["pay_year"] for row in second.records]
    assert not {row["record_id"] for row in first.records} & {
        row["record_id"] for row in second.records
    }
    assert first.records_sha256 != second.records_sha256


def test_cli_output_is_read_only_and_fails_on_over_budget(
    curated: tuple[Path, VintageKey], capsys: pytest.CaptureFixture[str]
) -> None:
    root, key = curated
    manifest_paths = sorted((root / "manifests").glob("*.json"))
    catalog = CuratedCatalog(root)
    vintage = catalog.get_vintage(key)
    paths = [*manifest_paths, *(item.path for item in vintage.artifacts.values())]
    previous = {path: path.read_bytes() for path in paths}
    command = [
        "ons-pay-publish-plan",
        "--data-root",
        str(root),
        "--registry-commit",
        key.registry_commit,
        "--source-sha256",
        key.source_sha256,
        "--table",
        "local_authority_yoy",
        "--shape",
        "document",
    ]
    platform_main(command)
    output = json.loads(capsys.readouterr().out)
    assert output["source_rows"] == 6
    assert len(output["records"]) == 6
    assert output["records"][0]["_id"]
    assert output["records"][0]["vintage"]["registry_commit"] == COMMIT_A
    with pytest.raises(SystemExit) as too_many:
        platform_main([*command, "--max-rows", "5"])
    assert too_many.value.code == 2
    assert "max_rows" in capsys.readouterr().err
    assert previous == {path: path.read_bytes() for path in paths}


@pytest.mark.parametrize("limit", [0, -1, 100001, True, 2.5, "3"])
def test_invalid_max_row_limits_fail_before_catalog_read(tmp_path: Path, limit: object) -> None:
    root = tmp_path / "absent"
    with pytest.raises(PublishPlanError, match="max_rows"):
        plan_ons_pay_publishing(
            data_root=root,
            vintage=VintageKey(1, COMMIT_A, "a" * 64),
            table="geography_year",
            shape="document",
            max_rows=limit,  # type: ignore[arg-type]
        )
    assert not root.exists()


@pytest.mark.parametrize(
    ("vintage", "table", "shape", "error"),
    [
        (VintageKey(True, COMMIT_A, "a" * 64), "geography_year", "document", "exact"),
        (VintageKey(2, COMMIT_A, "a" * 64), "geography_year", "document", "exact"),
        (VintageKey(1, "bad", "a" * 64), "geography_year", "document", "exact"),
        (VintageKey(1, COMMIT_A, "bad"), "geography_year", "document", "exact"),
        (VintageKey(1, COMMIT_A, "a" * 64), "untrusted", "document", "table"),
        (VintageKey(1, COMMIT_A, "a" * 64), "geography_year", "sql", "shape"),
    ],
)
def test_invalid_pins_tables_and_shapes_are_rejected_before_read(
    tmp_path: Path, vintage: VintageKey, table: str, shape: str, error: str
) -> None:
    root = tmp_path / "absent"
    with pytest.raises(PublishPlanError, match=error):
        plan_ons_pay_publishing(
            data_root=root,
            vintage=vintage,
            table=table,  # type: ignore[arg-type]
            shape=shape,  # type: ignore[arg-type]
        )
    assert not root.exists()


def test_unpublished_exact_vintage_does_not_fallback(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    with pytest.raises(VintageNotFound, match="requested"):
        plan_ons_pay_publishing(
            data_root=root,
            vintage=VintageKey(1, COMMIT_A, "f" * 64),
            table="geography_year",
            shape="document",
        )
    assert not root.exists()


def test_tampered_gold_artifact_invalidates_publishing_plan(
    curated: tuple[Path, VintageKey],
) -> None:
    root, key = curated
    artifact = CuratedCatalog(root).get_vintage(key).artifacts["geography_year"].path
    artifact.write_bytes(artifact.read_bytes() + b" ")
    with pytest.raises(CatalogError, match="SHA-256"):
        plan_ons_pay_publishing(
            data_root=root, vintage=key, table="geography_year", shape="relational"
        )


def test_duplicate_business_grain_is_rejected(curated: tuple[Path, VintageKey]) -> None:
    root, key = curated
    with (
        patch("data_platform_lab.serving.publish_plan._id", return_value="0" * 64),
        pytest.raises(PublishPlanError, match="duplicate business keys"),
    ):
        plan_ons_pay_publishing(
            data_root=root, vintage=key, table="geography_year", shape="document"
        )


@pytest.mark.parametrize(
    ("column", "value", "error"),
    [
        ("geography", None, "non-nullable"),
        ("geography", 42, "unexpected type"),
        ("pay_year", "2022", "unexpected type"),
        ("pay_year", True, "unsafe scalar"),
        ("areas_total", None, "non-nullable"),
        ("min_area_weekly_pay_gbp", "unknown", "unexpected type"),
        ("areas_total", 999, "do not reconcile"),
    ],
)
def test_v1_gold_field_contract_rejects_wrong_types_and_counts(
    curated: tuple[Path, VintageKey], column: str, value: object, error: str
) -> None:
    root, key = curated
    plan = plan_ons_pay_publishing(
        data_root=root, vintage=key, table="geography_year", shape="relational"
    )
    contract = _TABLES["geography_year"]
    reference = plan.records[0]
    values = tuple(value if name == column else reference[name] for name in contract.columns)
    with pytest.raises(PublishPlanError, match=error):
        _convert_row(columns=contract.columns, values=values, contract=contract, vintage=key)


def test_v1_gold_date_field_requires_actual_date_type(curated: tuple[Path, VintageKey]) -> None:
    root, key = curated
    plan = plan_ons_pay_publishing(
        data_root=root, vintage=key, table="local_authority_yoy", shape="relational"
    )
    contract = _TABLES["local_authority_yoy"]
    reference = plan.records[0]
    values = tuple(
        date.fromisoformat(str(reference[name])) if name == "reference_date" else reference[name]
        for name in contract.columns
    )
    assert (
        _convert_row(columns=contract.columns, values=values, contract=contract, vintage=key)[
            "reference_date"
        ]
        == reference["reference_date"]
    )
    with pytest.raises(PublishPlanError, match="unexpected type: reference_date"):
        _convert_row(
            columns=contract.columns,
            values=tuple(reference[name] for name in contract.columns),
            contract=contract,
            vintage=key,
        )


def test_plan_invalidates_an_artifact_changed_during_read(
    curated: tuple[Path, VintageKey],
) -> None:
    root, key = curated
    artifact = CuratedCatalog(root).get_vintage(key).artifacts["geography_year"].path
    from data_platform_lab.serving import publish_plan

    original = publish_plan._project
    changed = False

    def tamper(**kwargs: object):
        nonlocal changed
        projected = original(**kwargs)  # type: ignore[arg-type]
        if not changed:
            artifact.write_bytes(artifact.read_bytes() + b" ")
            changed = True
        return projected

    with (
        patch.object(publish_plan, "_project", tamper),
        pytest.raises(PublishPlanError, match="changed during publishing"),
    ):
        plan_ons_pay_publishing(
            data_root=root, vintage=key, table="geography_year", shape="document"
        )


@pytest.mark.parametrize("value", [True, float("nan"), float("inf"), object()])
def test_unsafe_scalar_values_are_rejected(value: object) -> None:
    with pytest.raises(PublishPlanError, match="unsafe scalar"):
        _scalar(value)


def test_cli_rejects_untrusted_input_without_creating_a_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = tmp_path / "absent"
    command = [
        "ons-pay-publish-plan",
        "--data-root",
        str(root),
        "--registry-commit",
        COMMIT_A,
        "--source-sha256",
        "a" * 64,
        "--table",
        "geography_year",
        "--shape",
        "document",
    ]
    for invalid in (
        ["--model-version", "2"],
        ["--max-rows", "0"],
        ["--registry-commit", "unsafe"],
        ["--shape", "sql"],
    ):
        with pytest.raises(SystemExit) as failed:
            platform_main([*command, *invalid])
        assert failed.value.code == 2
        assert capsys.readouterr().err
    assert not root.exists()
