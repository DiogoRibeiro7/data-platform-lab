"""Opt-in atomic Prometheus textfile exports for a trusted local operator.

The normal monitoring command remains read-only. This helper is only called
when the operator explicitly selects a ``.prom`` destination for a scraper.
"""

from __future__ import annotations

import os
import tempfile
from contextlib import suppress
from pathlib import Path


class TextfileExportError(ValueError):
    """An explicitly requested metrics textfile could not be published safely."""


def write_prometheus_textfile(*, path: Path, metrics: str) -> None:
    """Atomically replace one requested ``.prom`` file with a complete report.

    The parent directory must already exist and contain no symlink component.
    A temporary file in that directory is flushed and renamed on success; a
    failed write preserves any earlier complete report for staleness alerts.
    It is the caller's responsibility to schedule only one writer per path.
    """
    if path.suffix != ".prom" or path.name.startswith("."):
        raise TextfileExportError("metrics output must be a named .prom file")

    # Normalize relative paths and ``..`` without resolving any symlink: the
    # common CLI invocation starts in ``python/`` and writes under ``../data``.
    target = Path(os.path.abspath(path))
    parent = target.parent
    try:
        if parent.resolve(strict=True) != parent or not parent.is_dir():
            raise TextfileExportError("metrics output parent must be a real directory")
        if target.is_symlink() or (target.exists() and not target.is_file()):
            raise TextfileExportError("metrics output must not be a symlink or directory")
    except (OSError, RuntimeError) as exc:
        raise TextfileExportError("cannot inspect metrics output path") from exc

    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=parent,
            prefix=f".{target.stem}-",
            suffix=".tmp",
            delete=False,
        ) as output:
            temporary = Path(output.name)
            output.write(metrics)
            output.flush()
            os.fsync(output.fileno())
        # node_exporter may scrape as another local user; no secrets are emitted.
        os.chmod(temporary, 0o644)
        if target.is_symlink():
            raise TextfileExportError("metrics output became a symlink")
        os.replace(temporary, target)
    except OSError as exc:
        raise TextfileExportError("cannot atomically publish metrics textfile") from exc
    finally:
        if temporary is not None:
            with suppress(OSError):
                # Do not hide the original publish error during cleanup.
                temporary.unlink(missing_ok=True)
