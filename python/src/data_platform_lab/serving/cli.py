"""Explicit, local-by-default CLI for the optional ONS pay serving extra."""

from __future__ import annotations

import argparse
from importlib import import_module
from pathlib import Path
from typing import Any


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-serve",
        description="Serve SHA-pinned ONS pay Gold tables read-only from local Parquet.",
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--host", default="127.0.0.1", help="Loopback by default (no auth).")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")

    # Optional serving dependencies are imported only when this command is run.
    try:
        module: Any = import_module("data_platform_lab.serving.api")
        uvicorn: Any = import_module("uvicorn")
    except ModuleNotFoundError as exc:
        parser.error("install the serving extra first: poetry install --extras serving")
        raise AssertionError("unreachable") from exc
    uvicorn.run(module.create_app(args.data_root), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
