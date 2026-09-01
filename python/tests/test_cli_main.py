"""Tests for the unified Python CLI dispatcher."""

from __future__ import annotations

from unittest.mock import Mock, patch

from data_platform_lab.cli import main as cli_main_module


def test_main_without_command_prints_help(capsys: object) -> None:
    """Calling the root CLI without a command shows discoverable help."""
    cli_main_module.main([])
    captured = capsys.readouterr()  # type: ignore[attr-defined]

    assert "Unified entry point" in captured.out
    assert "benchmark" in captured.out
    assert "storage" in captured.out
    assert "stream" in captured.out
    assert "warehouse" in captured.out


def test_main_dispatches_remaining_arguments() -> None:
    """Child workflow arguments are forwarded unchanged."""
    handler = Mock()

    with patch.dict(cli_main_module._COMMANDS, {"stream": handler}, clear=True):
        cli_main_module.main(["stream", "--input", "events.jsonl", "--output-dir", "out"])

    handler.assert_called_once_with(
        ["--input", "events.jsonl", "--output-dir", "out"],
    )
