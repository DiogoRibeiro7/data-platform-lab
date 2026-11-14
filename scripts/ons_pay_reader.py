"""Standalone typed HTTP consumer; Python standard library only, no ingestion imports."""

from __future__ import annotations

import argparse
import ipaddress
import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlencode, urlsplit
from urllib.request import ProxyHandler, build_opener


@dataclass(frozen=True)
class AcceptedVintage:
    registry_commit: str
    source_sha256: str
    model_version: int
    target_id: str
    geography_year_sha256: str
    local_authority_yoy_sha256: str


@dataclass(frozen=True)
class GeographyYearRow:
    geography: str
    pay_year: int
    areas_total: int
    areas_with_pay: int
    areas_without_pay: int
    min_area_weekly_pay_gbp: float | None
    median_area_weekly_pay_gbp: float | None
    max_area_weekly_pay_gbp: float | None
    source_sha256: str
    registry_commit: str


@dataclass(frozen=True)
class GeographyYearPage:
    vintage: AcceptedVintage
    total: int
    limit: int
    offset: int
    rows: tuple[GeographyYearRow, ...]


def _get(base_url: str, path: str, parameters: dict[str, str | int]) -> dict[str, Any]:
    address = urlsplit(base_url)
    if (
        address.scheme != "http"
        or address.username is not None
        or address.password is not None
        or address.path not in ("", "/")
        or address.query
        or address.fragment
        or not address.hostname
        or not ipaddress.ip_address(address.hostname).is_loopback
    ):
        raise ValueError("Use the local API's numeric loopback HTTP URL")
    request_url = base_url.rstrip("/") + path + "?" + urlencode(parameters)
    # Ignore ambient HTTP proxies for this local consumer; bound both reads and waits.
    with build_opener(ProxyHandler({})).open(request_url, timeout=30) as response:
        payload = response.read(2 * 1024 * 1024 + 1)
    if len(payload) > 2 * 1024 * 1024:
        raise ValueError("Response exceeded the example client's 2 MiB budget")
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise TypeError("Invalid API response")
    return decoded


def _text(value: object, pattern: str) -> str:
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise ValueError("Invalid response identity")
    return value


def _integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("Invalid response integer")
    return value


def _number(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
        raise ValueError("Invalid response pay value")
    return float(value)


def _vintage(value: dict[str, Any]) -> AcceptedVintage:
    if value["dataset_id"] != "ons-gross-median-weekly-pay" or value["model_version"] != 1:
        raise ValueError("Unexpected dataset or model")
    tables = {table["table"]: table["sink_snapshot_sha256"] for table in value["tables"]}
    if len(value["tables"]) != 2 or set(tables) != {
        "geography_year",
        "local_authority_yoy",
    }:
        raise ValueError("Both accepted tables are required")
    return AcceptedVintage(
        _text(value["registry_commit"], r"[0-9a-f]{40}"),
        _text(value["source_sha256"], r"[0-9a-f]{64}"),
        1,
        _text(value["target_id"], r"[a-z][a-z0-9_-]{0,63}"),
        _text(tables["geography_year"], r"[0-9a-f]{64}"),
        _text(tables["local_authority_yoy"], r"[0-9a-f]{64}"),
    )


def discover(base_url: str) -> tuple[AcceptedVintage, ...]:
    """List accepted identities without selecting an implicit latest vintage."""
    return tuple(
        _vintage(value) for value in _get(base_url, "/v1/ons-pay/vintages", {})["vintages"]
    )


def geography_year(
    base_url: str,
    vintage: AcceptedVintage,
    *,
    limit: int = 50,
    offset: int = 0,
) -> GeographyYearPage:
    """Read one bounded page and retain exact source and acceptance fingerprints."""
    if (
        type(limit) is not int
        or not 1 <= limit <= 100
        or type(offset) is not int
        or not 0 <= offset <= 100_000
    ):
        raise ValueError("Invalid pagination")
    page = _get(
        base_url,
        "/v1/ons-pay/geography-year",
        {
            "model_version": vintage.model_version,
            "registry_commit": vintage.registry_commit,
            "source_sha256": vintage.source_sha256,
            "limit": limit,
            "offset": offset,
        },
    )
    if _vintage(page["vintage"]) != vintage or page["limit"] != limit or page["offset"] != offset:
        raise ValueError("Response differs from the selected accepted vintage/page")
    rows = []
    for row in page["rows"]:
        if (
            row["registry_commit"] != vintage.registry_commit
            or row["source_sha256"] != vintage.source_sha256
        ):
            raise ValueError("Row differs from the selected source pins")
        rows.append(
            GeographyYearRow(
                _text(row["geography"], r".{1,100}"),
                _integer(row["pay_year"]),
                _integer(row["areas_total"]),
                _integer(row["areas_with_pay"]),
                _integer(row["areas_without_pay"]),
                _number(row["min_area_weekly_pay_gbp"]),
                _number(row["median_area_weekly_pay_gbp"]),
                _number(row["max_area_weekly_pay_gbp"]),
                vintage.source_sha256,
                vintage.registry_commit,
            )
        )
    total = _integer(page["total"])
    if len(rows) != min(limit, max(0, total - offset)):
        raise ValueError("Response contains an incomplete or oversized page")
    return GeographyYearPage(vintage, total, limit, offset, tuple(rows))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--registry-commit")
    parser.add_argument("--source-sha256")
    parser.add_argument("--target-id")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--offset", type=int, default=0)
    args = parser.parse_args()
    try:
        vintages = discover(args.base_url)
        pins = (args.registry_commit, args.source_sha256, args.target_id)
        if all(value is None for value in pins):
            print(json.dumps([asdict(vintage) for vintage in vintages], indent=2))
            return
        selected = [
            v for v in vintages if (v.registry_commit, v.source_sha256, v.target_id) == pins
        ]
        if len(selected) != 1:
            raise ValueError("Select one exact discovered vintage")
        print(
            json.dumps(
                asdict(
                    geography_year(args.base_url, selected[0], limit=args.limit, offset=args.offset)
                ),
                indent=2,
                allow_nan=False,
            )
        )
    except (OSError, ValueError, TypeError, LookupError):
        parser.exit(
            1,
            "Reader failed: check exact pins, API availability and accepted evidence.\n",
        )


if __name__ == "__main__":
    main()
