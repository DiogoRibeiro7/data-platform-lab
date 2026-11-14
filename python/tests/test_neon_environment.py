"""Standalone tests for Neon connection resolution without database dependencies."""

from __future__ import annotations

from pathlib import Path

import pytest

from data_platform_lab.serving.publish_cli import (
    _ConfigurationError,
    _neon_connection_urls,
    _postgres_address,
)

SAMPLE_SECRET = "postgresql://user:private-password@private.example.org/database"


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ({"DATABASE_URL": "pooled"}, "pooled"),
        ({"DATABASE_URL": "pooled", "DATABASE_URL_UNPOOLED": "direct"}, "direct"),
        ({"ONS_PAY_POSTGRES_DSN": "override", "DATABASE_URL_UNPOOLED": "direct"}, "override"),
    ],
)
def test_environment_precedence(settings: dict[str, str], expected: str) -> None:
    """Explicit publisher DSNs win; otherwise prefer the direct Neon URL."""
    assert _postgres_address(settings, None) == expected


def test_neon_file_reads_only_urls_and_prefers_direct(tmp_path: Path) -> None:
    """Ignore credentials for unrelated Neon services, including Auth."""
    path = tmp_path / ".env.local"
    path.write_text(
        '# Neon-generated\nexport DATABASE_URL="pooled"\n'
        "DATABASE_URL_UNPOOLED='direct'\nNEON_AUTH_JWKS_URL=private-auth-value\n",
        encoding="utf-8",
    )
    assert _neon_connection_urls(path) == {
        "DATABASE_URL": "pooled",
        "DATABASE_URL_UNPOOLED": "direct",
    }
    assert _postgres_address({}, path) == "direct"


def test_process_environment_wins_and_skips_file_read(tmp_path: Path) -> None:
    """Do not open the env file when the process has a usable connection setting."""
    missing = tmp_path / "not-present.env"
    assert _postgres_address({"ONS_PAY_POSTGRES_DSN": SAMPLE_SECRET}, missing) == SAMPLE_SECRET
    assert not missing.exists()


@pytest.mark.parametrize(
    "contents",
    [
        'DATABASE_URL="unterminated',
        "DATABASE_URL=first\nDATABASE_URL=second\n",
        "NEON_AUTH_BASE_URL=present-without-a-database-url\n",
    ],
)
def test_invalid_or_missing_urls_never_leak_secrets(tmp_path: Path, contents: str) -> None:
    """Malformed values must not surface in operator-facing error messages."""
    path = tmp_path / ".env.local"
    path.write_text(contents + SAMPLE_SECRET, encoding="utf-8")
    with pytest.raises(_ConfigurationError) as error:
        _postgres_address({}, path)
    assert SAMPLE_SECRET not in str(error.value)
    assert str(path) not in str(error.value)


def test_missing_file_has_sanitized_error(tmp_path: Path) -> None:
    """A missing env file is a configuration error, never a driver error."""
    with pytest.raises(_ConfigurationError) as error:
        _postgres_address({}, tmp_path / ".env.local")
    assert "Cannot read" in str(error.value)
    assert str(tmp_path) not in str(error.value)
