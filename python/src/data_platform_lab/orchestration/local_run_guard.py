"""Nonblocking, process-scoped guards for cooperating local refresh writers."""

from __future__ import annotations

import errno
import os
import stat
import sys
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from pathlib import Path


class LocalRunGuardError(ValueError):
    """A destination guard could not be acquired safely."""


class LocalRunBusyError(LocalRunGuardError):
    """Another process already holds one of the requested destination guards."""


def _open_guard(path: Path) -> int:
    """Open persistent metadata without following links or changing its contents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        before = path.lstat()
    except FileNotFoundError:
        before = None
    if before is not None and (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1):
        raise LocalRunGuardError("Destination guard must be a regular file with one link.")
    descriptor = os.open(
        path,
        os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0),
        0o600,
    )
    try:
        opened, current = os.fstat(descriptor), path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(current.st_mode)
            or opened.st_nlink != 1
            or current.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
        ):
            raise LocalRunGuardError("Destination guard changed while opening.")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _lock(descriptor: int) -> None:
    """Acquire immediately, using the same byte on Windows and flock on POSIX."""
    if sys.platform == "win32":
        import msvcrt

        os.lseek(descriptor, 0, os.SEEK_SET)
        msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
    else:
        import fcntl

        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)


@contextmanager
def guard_local_roots(roots: Sequence[Path]) -> Iterator[None]:
    """Hold local destination guards until exit, or fail without waiting.

    Locks live beside resolved roots, outside stored object namespaces. Files
    persist to preserve lock identity; closing descriptors (including process
    termination) releases ownership. This is not a distributed storage lock.
    """
    try:
        resolved = sorted({root.resolve() for root in roots})
    except (OSError, ValueError, RuntimeError) as exc:
        raise LocalRunGuardError("Unable to resolve destination guards.") from exc
    with ExitStack() as cleanup:
        for root in resolved:
            try:
                path = root.with_name(f".{root.name}.ons-pay-refresh.lock")
                descriptor = _open_guard(path)
            except (OSError, ValueError) as exc:
                raise LocalRunGuardError("Unable to open destination guard.") from exc
            cleanup.callback(os.close, descriptor)
            try:
                _lock(descriptor)
            except OSError as exc:
                if exc.errno in {errno.EACCES, errno.EAGAIN}:
                    raise LocalRunBusyError("A destination is already in use.") from exc
                raise LocalRunGuardError("Unable to lock destination guard.") from exc
            except ImportError as exc:
                raise LocalRunGuardError(
                    "Destination guards are unsupported on this platform."
                ) from exc
        yield
