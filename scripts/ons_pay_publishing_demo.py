"""Run the synthetic ONS pay walkthrough in a private, disposable Compose stack.

Only Python 3.11-3.14 and Docker Compose are needed on the host. Each invocation
creates a fresh output child and a random Compose project, preserving earlier
artifacts and leaving unrelated services, files and databases alone.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from contextlib import suppress
from pathlib import Path
from uuid import uuid4


def launch(output_dir: Path, *, exercise_recovery: bool = False) -> dict[str, object]:
    """Build, run and tear down this invocation's services, retaining diagnostics."""
    repository = Path(__file__).resolve().parents[1]
    run_id = uuid4().hex
    project = f"ons-pay-demo-{run_id}"
    output = output_dir.resolve() / f"run-{run_id}"
    output.mkdir(parents=True, exist_ok=False)
    # Do not inherit a user's Compose file/profile/project overrides or Docker
    # remote-daemon target. The demo always uses the local default Docker context.
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("COMPOSE_", "DOCKER_", "ONS_PAY_DEMO_"))
    }
    environment.update(
        ONS_PAY_DEMO_OUTPUT=str(output),
        ONS_PAY_DEMO_UID=str(os.getuid() if hasattr(os, "getuid") else 0),
        ONS_PAY_DEMO_GID=str(os.getgid() if hasattr(os, "getgid") else 0),
    )
    base = [
        "docker",
        "--context",
        "default",
        "compose",
        "--env-file",
        os.devnull,
        "--project-name",
        project,
        "--file",
        str(repository / "infra/ons-pay-demo/compose.yaml"),
    ]

    def compose(arguments: list[str], log_name: str) -> int:
        with (output / log_name).open("w", encoding="utf-8") as log:
            return subprocess.run(
                [*base, *arguments],
                cwd=repository,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            ).returncode

    result: dict[str, object] = {
        "status": "failed",
        "compose_project": project,
        "artifacts": str(output),
        "report": str(output / "report.json"),
    }
    try:
        for command, log in (
            (["build", "runner"], "build.log"),
            (["up", "-d", "--wait", "postgres", "mongodb"], "services.log"),
        ):
            if compose(command, log):
                result["error"] = f"setup failed; inspect {log}"
                return result
        arguments = ["run", "--rm", "--no-deps", "-T", "runner"]
        if exercise_recovery:
            arguments.append("--exercise-recovery")
        code = compose(arguments, "demo.log")
        if code:
            result["error"] = "walkthrough failed; inspect report.json and demo.log"
        else:
            report = json.loads((output / "report.json").read_text(encoding="utf-8"))
            if report["status"] != "success":
                result["error"] = "walkthrough report did not confirm success"
            else:
                result.update(
                    status="success",
                    source=report["source"],
                    elapsed_seconds=report["elapsed_seconds"],
                )
    except (OSError, ValueError, KeyError):
        result["error"] = "demo could not run; inspect the retained logs"
    finally:
        # Diagnostics must never prevent teardown of our services.
        with suppress(OSError):
            compose(["logs", "--no-color", "postgres", "mongodb"], "database.log")
        try:
            cleanup_code = compose(["down", "--volumes", "--rmi", "local"], "cleanup.log")
            result["cleanup"] = "success" if cleanup_code == 0 else "failed"
        except OSError:
            result["cleanup"] = "failed"
        if result["cleanup"] != "success":
            result["status"] = "failed"
            result["error"] = "service cleanup failed; inspect cleanup.log for this Compose project"
        (output / "launcher.json").write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return result


def main(argv: list[str] | None = None) -> None:
    """Provide one host entry point with explicit authorization for the recovery exercise."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data/ons-pay-demo"))
    parser.add_argument(
        "--exercise-recovery",
        action="store_true",
        help="Also mutate synthetic sink records, require rejection and explicitly replay.",
    )
    args = parser.parse_args(argv)
    if not (3, 11) <= sys.version_info[:2] < (3, 15):
        parser.error("use Python 3.11 through 3.14")
    try:
        result = launch(args.output_dir, exercise_recovery=args.exercise_recovery)
    except OSError:
        result = {"status": "failed", "error": "cannot create an isolated output directory"}
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
