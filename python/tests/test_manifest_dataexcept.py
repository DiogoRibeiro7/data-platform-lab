"""DataExcept boundary tests for manifest persistence."""

from __future__ import annotations

from pathlib import Path

import pytest
from dataexcept import DataValidationError, FileReadError, FileWriteError, ParsingError

from data_platform_lab.manifest import read_manifest, write_manifest


def test_read_manifest_classifies_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"

    with pytest.raises(FileReadError) as error:
        read_manifest(missing)

    assert error.value.path == str(missing)
    assert isinstance(error.value.__cause__, OSError)


def test_read_manifest_classifies_malformed_json_without_retaining_payload(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bad.json"
    secret = "token=SUPERSECRET"
    path.write_text("{" + secret, encoding="utf-8")

    with pytest.raises(ParsingError, match="valid UTF-8 JSON") as error:
        read_manifest(path)

    assert error.value.text == "manifest JSON"
    assert secret not in str(error.value)
    assert isinstance(error.value.__cause__, Exception)


def test_read_manifest_classifies_invalid_utf8_without_retaining_payload(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bad-utf8.json"
    path.write_bytes(b"{\xff\xfe}")

    with pytest.raises(ParsingError, match="valid UTF-8 JSON") as error:
        read_manifest(path)

    assert error.value.text == "manifest JSON"
    assert isinstance(error.value.__cause__, UnicodeDecodeError)


def test_read_manifest_rejects_non_object_json(tmp_path: Path) -> None:
    path = tmp_path / "list.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")

    with pytest.raises(DataValidationError, match="decode to an object") as error:
        read_manifest(path)

    assert error.value.field == "manifest"
    assert error.value.value == "list"


def test_write_manifest_classifies_output_failure(tmp_path: Path) -> None:
    blocked = tmp_path / "blocked"
    blocked.write_text("not a directory", encoding="utf-8")

    with pytest.raises(FileWriteError) as error:
        write_manifest(
            pipeline_name="demo",
            run_id="r1",
            source="input.csv",
            output="output.csv",
            row_count=1,
            manifest_dir=blocked / "manifests",
        )

    assert isinstance(error.value.__cause__, OSError)
