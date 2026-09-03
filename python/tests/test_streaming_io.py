"""DataExcept coverage for streaming file boundaries."""

from pathlib import Path

import pytest
from dataexcept import FileReadError, FileWriteError

from data_platform_lab.streaming.processor import process_stream


def test_process_stream_classifies_missing_input(tmp_path: Path) -> None:
    missing = tmp_path / "missing.jsonl"

    with pytest.raises(FileReadError) as error:
        process_stream(missing, tmp_path / "out")

    assert error.value.path == str(missing)
    assert isinstance(error.value.__cause__, OSError)


def test_process_stream_classifies_output_directory_failure(tmp_path: Path) -> None:
    input_path = tmp_path / "input.jsonl"
    input_path.write_text("", encoding="utf-8")
    output_path = tmp_path / "output"
    output_path.write_text("not a directory", encoding="utf-8")

    with pytest.raises(FileWriteError) as error:
        process_stream(input_path, output_path)

    assert error.value.path == str(output_path)
    assert isinstance(error.value.__cause__, OSError)
