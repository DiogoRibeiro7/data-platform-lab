"""Real-process contention and lifetime checks for both native lock backends."""

from __future__ import annotations

import errno
import os
import queue
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from data_platform_lab.orchestration import local_run_guard as guard

_CHILD = """
import sys
from pathlib import Path
from data_platform_lab.orchestration.local_run_guard import guard_local_roots, LocalRunBusyError
try:
    with guard_local_roots([Path(value) for value in sys.argv[2:]]):
        print('acquired', flush=True)
        if sys.argv[1] == 'hold':
            sys.stdin.readline()
except LocalRunBusyError:
    print('busy', flush=True)
    sys.exit(3)
"""


def _child_options() -> dict[str, Any]:
    return {
        "env": {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        "creationflags": subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0,
        "text": True,
    }


@contextmanager
def _holding_roots(*roots: Path) -> Iterator[subprocess.Popen[str]]:
    """Wait for a child to own the guards, then release it deterministically."""
    process = subprocess.Popen(
        [sys.executable, "-c", _CHILD, "hold", *map(str, roots)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **_child_options(),
    )
    ready: queue.Queue[str] = queue.Queue()
    assert process.stdout is not None
    thread = threading.Thread(target=lambda: ready.put(process.stdout.readline()), daemon=True)
    thread.start()
    try:
        assert ready.get(timeout=15).strip() == "acquired"
        yield process
    finally:
        try:
            process.communicate(input="\n" if process.poll() is None else None, timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=10)
            pytest.fail("Guard holder did not stop after release")
        thread.join(timeout=1)


def _probe(*roots: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", _CHILD, "probe", *map(str, roots)],
        capture_output=True,
        timeout=15,
        **_child_options(),
    )


@pytest.mark.parametrize("kill", [False, True])
def test_process_exit_releases_persistent_guard(tmp_path: Path, kill: bool) -> None:
    root = tmp_path / "objects"
    with _holding_roots(root) as process:
        assert _probe(root).returncode == 3
        assert _probe(tmp_path / "unrelated").returncode == 0
        if kill:
            process.kill()
            process.wait(timeout=10)
            assert _probe(root).returncode == 0
    assert _probe(root).returncode == 0
    assert (tmp_path / ".objects.ons-pay-refresh.lock").read_bytes() == b""
    assert not root.exists()


def test_exception_releases_guard_without_rewriting_metadata(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    metadata = tmp_path / ".objects.ons-pay-refresh.lock"
    metadata.write_bytes(b"existing metadata remains untouched")
    before = metadata.stat()
    with pytest.raises(RuntimeError, match="pipeline failure"), guard.guard_local_roots([root]):
        assert _probe(root).returncode == 3
        raise RuntimeError("pipeline failure")
    assert _probe(root).returncode == 0
    assert metadata.read_bytes() == b"existing metadata remains untouched"
    assert metadata.stat().st_ino == before.st_ino
    assert metadata.stat().st_mtime_ns == before.st_mtime_ns


def test_partial_acquisition_releases_earlier_destination(tmp_path: Path) -> None:
    first, busy = tmp_path / "a", tmp_path / "z"
    with _holding_roots(busy):
        with pytest.raises(guard.LocalRunBusyError), guard.guard_local_roots([busy, first]):
            pytest.fail("Contended batch entered")
        assert _probe(first).returncode == 0
        assert _probe(busy).returncode == 3


def test_duplicate_roots_and_normalized_paths_share_one_guard(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    alias = tmp_path / "unused" / ".." / "objects"
    with guard.guard_local_roots([root, alias, root]):
        assert _probe(alias).returncode == 3
    assert _probe(alias).returncode == 0


def test_symlinked_destination_uses_real_root_guard(tmp_path: Path) -> None:
    root, alias = tmp_path / "objects", tmp_path / "alias"
    root.mkdir()
    try:
        alias.symlink_to(root, target_is_directory=True)
    except OSError:
        pytest.skip("Creating symlinks requires platform privileges")
    with _holding_roots(root):
        assert _probe(alias).returncode == 3
    assert not (tmp_path / ".alias.ons-pay-refresh.lock").exists()


@pytest.mark.parametrize("kind", ["directory", "symlink", "hardlink"])
def test_unsafe_guard_metadata_is_rejected_without_modification(tmp_path: Path, kind: str) -> None:
    root, metadata = tmp_path / "objects", tmp_path / ".objects.ons-pay-refresh.lock"
    target = tmp_path / "unrelated"
    target.write_bytes(b"do not modify")
    if kind == "directory":
        metadata.mkdir()
    elif kind == "hardlink":
        metadata.hardlink_to(target)
    else:
        try:
            metadata.symlink_to(target)
        except OSError:
            pytest.skip("Creating symlinks requires platform privileges")
    with pytest.raises(guard.LocalRunGuardError), guard.guard_local_roots([root]):
        pytest.fail("Unsafe guard accepted")
    assert target.read_bytes() == b"do not modify"
    assert not root.exists()


@pytest.mark.parametrize("stage", ["open", "lock"])
def test_guard_io_errors_are_sanitized_and_release_acquired_roots(
    tmp_path: Path, monkeypatch: Any, stage: str
) -> None:
    first, second = tmp_path / "a", tmp_path / "z"
    target = "_open_guard" if stage == "open" else "_lock"
    original = getattr(guard, target)
    calls = 0

    def fail_second(*args: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError(errno.EIO, "secret storage diagnostic")
        return original(*args)

    monkeypatch.setattr(guard, target, fail_second)
    with (
        pytest.raises(guard.LocalRunGuardError) as caught,
        guard.guard_local_roots([first, second]),
    ):
        pytest.fail("Failed guard accepted")
    assert not isinstance(caught.value, guard.LocalRunBusyError)
    assert "secret" not in str(caught.value)
    assert _probe(first, second).returncode == 0


def test_filesystem_root_fails_with_guard_error(tmp_path: Path) -> None:
    with pytest.raises(guard.LocalRunGuardError), guard.guard_local_roots([Path(tmp_path.anchor)]):
        pytest.fail("A filesystem root has no sibling guard location")
