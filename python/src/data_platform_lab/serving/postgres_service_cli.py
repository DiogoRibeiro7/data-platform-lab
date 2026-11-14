"""Preview, explicitly apply, or read-only verify the persistent dataset schema."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from data_platform_lab.serving.postgres_service import (
    PostgresServiceError,
    apply_migrations,
    preview_service,
    verify_service,
)
from data_platform_lab.serving.publish_cli import (
    _ArgumentError,
    _ConfigurationError,
    _neon_connection_urls,
    _Parser,
)


def main(argv: list[str] | None = None, *, environ: Mapping[str, str] | None = None) -> None:
    """Return 0 for success, 1 for an unavailable/unready service, 2 for invalid input."""
    parser = _Parser(prog="data-platform-lab postgres-service", allow_abbrev=False)
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Explicit Neon .env.local for direct admin access during --apply/--verify.",
    )
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--apply", action="store_true", help="Apply pending schema migrations.")
    operation.add_argument(
        "--verify", action="store_true", help="Read-only connection/schema readiness."
    )
    try:
        args = parser.parse_args(argv)
    except _ArgumentError:
        parser.exit(2, "Invalid PostgreSQL service arguments.\n")
    try:
        if not args.apply and not args.verify:
            report = preview_service()
        else:
            settings = os.environ if environ is None else environ
            dsn = settings.get("DPL_POSTGRES_SERVICE_DSN")
            if not isinstance(dsn, str) or not dsn.strip():
                if args.env_file is not None:
                    try:
                        # Administrative DDL and migration SET commands must
                        # not use Neon's transaction-pooled DATABASE_URL.
                        dsn = _neon_connection_urls(args.env_file).get("DATABASE_URL_UNPOOLED")
                    except _ConfigurationError:
                        parser.exit(2, "Cannot read the Neon PostgreSQL environment file.\n")
                if not isinstance(dsn, str) or not dsn.strip():
                    parser.exit(
                        2,
                        "Set DPL_POSTGRES_SERVICE_DSN or provide an unpooled "
                        "Neon URL with --env-file.\n",
                    )
            import psycopg

            with psycopg.connect(dsn, autocommit=False, connect_timeout=10) as connection:
                report = apply_migrations(connection) if args.apply else verify_service(connection)
    except PostgresServiceError as error:
        parser.exit(1, f"{error}\n")
    except Exception:
        parser.exit(1, "PostgreSQL service operation failed; inspect configuration and schema.\n")
    print(json.dumps(report, indent=2))
    if report.get("status") == "not_ready":
        raise SystemExit(1)
