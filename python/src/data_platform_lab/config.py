"""Lightweight JSON config loader for pipeline workflows.

Supports a simple precedence model:
  defaults < config file < CLI flags

Config files are plain JSON with snake_case keys matching CLI parameters.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ConfigError(ValueError):
    """Raised when a config file is invalid or missing required fields."""


def load_config(path: str | Path) -> dict[str, Any]:
    """Load and parse a JSON config file.

    Raises ConfigError if the file is not valid JSON.
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    try:
        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON in config file {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"Config file must contain a JSON object, got {type(data).__name__}")

    logger.info("Loaded config from %s", path)
    return data


def validate_config(
    data: dict[str, Any],
    required: list[str] | None = None,
    known: list[str] | None = None,
) -> list[str]:
    """Validate a config dict.

    Parameters
    ----------
    data : dict
        The loaded config.
    required : list[str] or None
        Keys that must be present. Returns errors for missing keys.
    known : list[str] or None
        Keys that are recognised. Logs warnings for unknown keys.

    Returns
    -------
    list[str]
        List of validation error messages (empty if valid).
    """
    errors: list[str] = []

    if required:
        for key in required:
            if key not in data:
                errors.append(f"Missing required config key: {key}")

    if known:
        for key in data:
            if key not in known:
                logger.warning("Unknown config key: %s (will be ignored)", key)

    return errors


def merge_config(
    defaults: dict[str, Any],
    config: dict[str, Any] | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Merge configuration with standard precedence.

    defaults < config file < CLI overrides

    Only non-None override values replace config values.
    """
    result = dict(defaults)
    if config:
        for k, v in config.items():
            if k in result:
                result[k] = v
    if overrides:
        for k, v in overrides.items():
            if v is not None and k in result:
                result[k] = v
    return result
