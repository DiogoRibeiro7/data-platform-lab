"""Unified command-line entry point for Data Platform Lab."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from data_platform_lab.benchmark.cli import main as benchmark_main
from data_platform_lab.metadata.cli import main as metadata_main
from data_platform_lab.storage.cli import main as storage_main
from data_platform_lab.streaming.cli import main as streaming_main
from data_platform_lab.warehouse.cli import main as warehouse_main

CommandHandler = Callable[[list[str] | None], None]

_COMMANDS: dict[str, CommandHandler] = {
    "benchmark": benchmark_main,
    "metadata": metadata_main,
    "storage": storage_main,
    "stream": streaming_main,
    "warehouse": warehouse_main,
}


def _build_parser() -> argparse.ArgumentParser:
    """Build the top-level parser without duplicating child command options."""
    parser = argparse.ArgumentParser(
        prog="data-platform-lab",
        description="Unified entry point for Data Platform Lab workflows.",
    )
    parser.add_argument(
        "command",
        nargs="?",
        choices=sorted(_COMMANDS),
        help="Workflow to run. Remaining arguments are passed to that workflow.",
    )
    parser.add_argument(
        "arguments",
        nargs=argparse.REMAINDER,
        help=argparse.SUPPRESS,
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    """Dispatch to a workflow CLI while preserving its existing arguments."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return

    _COMMANDS[args.command](args.arguments)


if __name__ == "__main__":
    main()
