"""Explicit, local-by-default CLI for the optional ONS pay serving extra."""

from __future__ import annotations

import argparse
import ipaddress
import os
from importlib import import_module
from pathlib import Path
from typing import Any


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="data-platform-lab ons-pay-serve",
        description="Serve SHA-pinned ONS Gold from Parquet or accepted PostgreSQL publications.",
    )
    parser.add_argument("--data-root", type=Path, default=Path("../data"))
    parser.add_argument("--host", default="127.0.0.1", help="Loopback by default (no auth).")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--backend", choices=("parquet", "postgresql"), default="parquet")
    parser.add_argument("--target-id", help="Exact publication target; required for PostgreSQL.")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    settings: dict[str, Any] = {}
    if args.backend == "postgresql":
        try:
            loopback = args.host == "localhost" or ipaddress.ip_address(args.host).is_loopback
        except ValueError:
            loopback = False
        if not loopback:
            parser.error(
                "PostgreSQL serving requires loopback until HTTP authentication is configured"
            )
        reader_dsn = os.environ.get("ONS_PAY_POSTGRES_READER_DSN")
        if not reader_dsn or not reader_dsn.strip() or args.target_id is None:
            parser.error("Set ONS_PAY_POSTGRES_READER_DSN and --target-id for PostgreSQL")
        from data_platform_lab.serving.publish_evidence import (
            PublicationEvidenceError,
            validate_target_id,
        )

        try:
            validate_target_id(args.target_id)
        except PublicationEvidenceError:
            parser.error("Invalid publication target ID")
        settings = {
            "backend": "postgresql",
            "postgres_dsn": reader_dsn,
            "target_id": args.target_id,
        }
    elif args.target_id is not None:
        parser.error("--target-id requires --backend postgresql")

    # Optional serving dependencies are imported only when this command is run.
    try:
        module: Any = import_module("data_platform_lab.serving.api")
        uvicorn: Any = import_module("uvicorn")
    except ModuleNotFoundError as exc:
        parser.error("install the serving extra first: poetry install --extras serving")
        raise AssertionError("unreachable") from exc
    uvicorn.run(module.create_app(args.data_root, **settings), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
