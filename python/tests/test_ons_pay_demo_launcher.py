"""Check launcher ownership and cleanup paths without starting Docker."""

from __future__ import annotations

import json
import runpy
import subprocess
from pathlib import Path
from typing import Any

import pytest


@pytest.mark.parametrize("failure", [None, "build", "up", "run", "logs", "down"])
def test_launcher_scopes_every_command_and_always_attempts_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str | None
) -> None:
    """Retain prior files and tear down even when setup, execution or diagnostics fail."""
    script = Path(__file__).resolve().parents[2] / "scripts/ons_pay_publishing_demo.py"
    launch = runpy.run_path(str(script))["launch"]
    sentinel = tmp_path / "keep.txt"
    sentinel.write_text("prior unrelated output")
    monkeypatch.setenv("COMPOSE_FILE", "unrelated-compose.yaml")
    monkeypatch.setenv("DOCKER_HOST", "tcp://external.invalid:2375")
    calls = []

    def docker(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        environment = kwargs["env"]
        assert "DOCKER_HOST" not in environment and "COMPOSE_FILE" not in environment
        assert args[:4] == ["docker", "--context", "default", "compose"]
        command = args[args.index("--file") + 2]
        if failure == "logs" and command == "logs":
            raise OSError("cannot collect diagnostic output")
        if command == "run" and failure != "run":
            output = Path(environment["ONS_PAY_DEMO_OUTPUT"])
            (output / "report.json").write_text(
                json.dumps(
                    {
                        "status": "success",
                        "source": {"synthetic": True},
                        "elapsed_seconds": 1.0,
                    }
                )
            )
        return subprocess.CompletedProcess(args, 1 if command == failure else 0)

    monkeypatch.setattr(subprocess, "run", docker)
    result = launch(tmp_path, exercise_recovery=True)
    assert sentinel.read_text() == "prior unrelated output"
    assert result["compose_project"].startswith("ons-pay-demo-")
    assert all(
        args[args.index("--project-name") + 1] == result["compose_project"] for args in calls
    )
    assert calls[-1][-4:] == ["down", "--volumes", "--rmi", "local"]
    assert result["cleanup"] == ("failed" if failure == "down" else "success")
    assert result["status"] == ("success" if failure in {None, "logs"} else "failed")
    assert json.loads((Path(result["artifacts"]) / "launcher.json").read_text()) == result
    assert len(list(tmp_path.glob("run-*"))) == 1
