"""Unified command-line entry point for Data Platform Lab."""

from __future__ import annotations

import argparse
from collections.abc import Callable

from data_platform_lab.benchmark.cli import main as benchmark_main
from data_platform_lab.broker.cli import main as broker_main
from data_platform_lab.curation.ons_pay_cli import main as ons_pay_curate_main
from data_platform_lab.iceberg.cli import main as iceberg_main
from data_platform_lab.ingestion.registry_cli import main as registry_ingest_main
from data_platform_lab.metadata.cli import main as metadata_main
from data_platform_lab.observability.ons_pay_monitor_cli import main as ons_pay_monitor_main
from data_platform_lab.orchestration.ons_pay_refresh_cli import main as ons_pay_refresh_main
from data_platform_lab.recovery.cli import main as recovery_main
from data_platform_lab.serving.cli import main as ons_pay_serve_main
from data_platform_lab.serving.ons_pay_compare_cli import main as ons_pay_compare_main
from data_platform_lab.serving.release_cli import main as ons_pay_release_main
from data_platform_lab.storage.cli import main as storage_main
from data_platform_lab.streaming.cli import main as streaming_main
from data_platform_lab.warehouse.cli import main as warehouse_main

CommandHandler = Callable[[list[str] | None], None]

_COMMANDS: dict[str, CommandHandler] = {
    "benchmark": benchmark_main,
    "broker": broker_main,
    "iceberg": iceberg_main,
    "metadata": metadata_main,
    "ons-pay-compare": ons_pay_compare_main,
    "ons-pay-curate": ons_pay_curate_main,
    "ons-pay-monitor": ons_pay_monitor_main,
    "ons-pay-refresh": ons_pay_refresh_main,
    "ons-pay-release": ons_pay_release_main,
    "ons-pay-serve": ons_pay_serve_main,
    "recovery": recovery_main,
    "registry-ingest": registry_ingest_main,
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
