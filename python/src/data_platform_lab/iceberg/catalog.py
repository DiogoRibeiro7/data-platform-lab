"""PyIceberg SQL-catalog construction for the local platform."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any


@dataclass(frozen=True)
class IcebergCatalogConfig:
    """Connection settings for the local Iceberg catalog and S3 warehouse."""

    catalog_name: str
    postgres_dsn: str
    warehouse: str
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_region: str = "garage"


def _sqlalchemy_dsn(dsn: str) -> str:
    """Select SQLAlchemy's psycopg 3 dialect for ordinary PostgreSQL DSNs."""
    if dsn.startswith("postgresql+psycopg://"):
        return dsn
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


def build_catalog(config: IcebergCatalogConfig) -> Any:
    """Build a PyIceberg SQL catalog backed by PostgreSQL and S3-compatible storage."""
    try:
        catalog_module = import_module("pyiceberg.catalog")
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyIceberg is required for analytical table storage") from exc

    load_catalog = getattr(catalog_module, "load_catalog", None)
    if not callable(load_catalog):
        raise RuntimeError("PyIceberg installation does not expose load_catalog()")

    return load_catalog(
        config.catalog_name,
        type="sql",
        uri=_sqlalchemy_dsn(config.postgres_dsn),
        warehouse=config.warehouse,
        **{
            "s3.endpoint": config.s3_endpoint,
            "s3.access-key-id": config.s3_access_key_id,
            "s3.secret-access-key": config.s3_secret_access_key,
            "s3.region": config.s3_region,
            "s3.force-virtual-addressing": "false",
        },
    )
