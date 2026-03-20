"""Tests for the config loader module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from data_platform_lab.config import (
    ConfigError,
    load_config,
    merge_config,
    validate_config,
)

# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------


def test_load_config_valid(tmp_path: Path) -> None:
    """Write a valid JSON config file, load it, verify dict with expected keys."""
    cfg = {"input_dir": "/data/raw", "output_dir": "/data/out", "batch_size": 100}
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")

    result = load_config(p)

    assert isinstance(result, dict)
    assert result["input_dir"] == "/data/raw"
    assert result["output_dir"] == "/data/out"
    assert result["batch_size"] == 100


def test_load_config_file_not_found(tmp_path: Path) -> None:
    """Nonexistent path raises ConfigError with 'not found' in message."""
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "does_not_exist.json")


def test_load_config_invalid_json(tmp_path: Path) -> None:
    """File with invalid JSON raises ConfigError with 'Invalid JSON'."""
    p = tmp_path / "bad.json"
    p.write_text("{not valid json!!!", encoding="utf-8")

    with pytest.raises(ConfigError, match="Invalid JSON"):
        load_config(p)


def test_load_config_not_object(tmp_path: Path) -> None:
    """JSON array raises ConfigError with 'JSON object'."""
    p = tmp_path / "array.json"
    p.write_text(json.dumps([1, 2, 3]), encoding="utf-8")

    with pytest.raises(ConfigError, match="JSON object"):
        load_config(p)


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


def test_validate_config_no_errors() -> None:
    """Valid config with all required keys returns empty error list."""
    data = {"input_dir": "/data", "output_dir": "/out"}
    errors = validate_config(data, required=["input_dir", "output_dir"])

    assert errors == []


def test_validate_config_missing_required() -> None:
    """Missing required keys appear in error messages."""
    data = {"input_dir": "/data"}
    errors = validate_config(data, required=["input_dir", "output_dir", "batch_size"])

    assert len(errors) == 2
    assert any("output_dir" in e for e in errors)
    assert any("batch_size" in e for e in errors)


def test_validate_config_unknown_keys_warns() -> None:
    """Unknown keys produce warnings, not errors."""
    data = {"input_dir": "/data", "mystery_key": 42}
    errors = validate_config(data, known=["input_dir"])

    assert errors == []


# ---------------------------------------------------------------------------
# merge_config
# ---------------------------------------------------------------------------


def test_merge_config_defaults_only() -> None:
    """Merge with only defaults returns the defaults."""
    defaults = {"input_dir": "/default", "batch_size": 50}

    result = merge_config(defaults)

    assert result == defaults
    # Must be a new dict, not the same object
    assert result is not defaults


def test_merge_config_overrides_defaults() -> None:
    """Config values override default values."""
    defaults = {"input_dir": "/default", "batch_size": 50}
    config = {"batch_size": 200}

    result = merge_config(defaults, config=config)

    assert result["input_dir"] == "/default"
    assert result["batch_size"] == 200


def test_merge_config_cli_overrides_config() -> None:
    """CLI overrides override config values."""
    defaults = {"input_dir": "/default", "batch_size": 50}
    config = {"batch_size": 200}
    cli = {"batch_size": 999}

    result = merge_config(defaults, config=config, overrides=cli)

    assert result["batch_size"] == 999


def test_merge_config_none_override_ignored() -> None:
    """CLI override with None does not replace config value."""
    defaults = {"input_dir": "/default", "batch_size": 50}
    config = {"batch_size": 200}
    cli = {"batch_size": None}

    result = merge_config(defaults, config=config, overrides=cli)

    assert result["batch_size"] == 200


def test_merge_config_full_precedence() -> None:
    """Full precedence: defaults < config < CLI."""
    defaults = {"a": 1, "b": 2, "c": 3}
    config = {"a": 10, "b": 20}
    cli = {"a": 100}

    result = merge_config(defaults, config=config, overrides=cli)

    assert result["a"] == 100  # CLI wins
    assert result["b"] == 20  # config wins over default
    assert result["c"] == 3  # default survives
