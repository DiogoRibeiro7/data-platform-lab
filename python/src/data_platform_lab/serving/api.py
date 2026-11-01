"""Read-only FastAPI contract for explicitly pinned ONS pay Gold tables."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi import Path as RoutePath
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, ValidationError

from data_platform_lab.serving.catalog import (
    CatalogError,
    CuratedCatalog,
    CuratedVintage,
    VintageKey,
    VintageNotFound,
)
from data_platform_lab.serving.release_store import ReleaseError, ReleaseStore

Commit = Annotated[str, Query(pattern=r"^[0-9a-f]{40}$")]
SourceHash = Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")]
ModelVersion = Annotated[int, Query(ge=1, le=1)]
PageLimit = Annotated[int, Query(ge=1, le=100)]
PageOffset = Annotated[int, Query(ge=0)]
Year = Annotated[int | None, Query(ge=1900, le=2100)]


class VintageResponse(BaseModel):
    model_version: int
    registry_commit: str
    source_sha256: str
    manifest_sha256: str
    source_rows: int
    rows_with_pay: int
    rows_without_pay: int
    source_publisher: str
    source_url: str
    source_snapshot: str
    source_retrieved_at: str
    citation: str
    license_name: str
    license_url: str


class VintagesResponse(BaseModel):
    vintages: list[VintageResponse]


class ActiveReleaseResponse(BaseModel):
    generation: int
    event_sha256: str
    vintage: VintageResponse


class _StrictRow(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class GeographyYearRow(_StrictRow):
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


class AreaTrendRow(_StrictRow):
    area_code: str
    area_name: str
    reference_date: date
    pay_year: int
    weekly_pay_gbp: float | None
    status_code: str | None
    previous_year: int | None
    previous_weekly_pay_gbp: float | None
    yoy_nominal_pct: float | None
    source_sha256: str
    registry_commit: str


class GeographyYearPage(BaseModel):
    vintage: VintageResponse
    total: int
    limit: int
    offset: int
    rows: list[GeographyYearRow]


class AreaTrendPage(BaseModel):
    vintage: VintageResponse
    area_code: str
    total: int
    limit: int
    offset: int
    rows: list[AreaTrendRow]


def _vintage_response(vintage: CuratedVintage) -> VintageResponse:
    return VintageResponse(
        model_version=vintage.key.model_version,
        registry_commit=vintage.key.registry_commit,
        source_sha256=vintage.key.source_sha256,
        manifest_sha256=vintage.manifest_sha256,
        source_rows=vintage.source_rows,
        rows_with_pay=vintage.rows_with_pay,
        rows_without_pay=vintage.rows_without_pay,
        source_publisher=vintage.source_publisher,
        source_url=vintage.source_url,
        source_snapshot=vintage.source_snapshot,
        source_retrieved_at=vintage.source_retrieved_at,
        citation=vintage.citation,
        license_name=vintage.license_name,
        license_url=vintage.license_url,
    )


def create_app(data_root: Path) -> FastAPI:
    """Build a local-only API backed by a trusted curated data directory.

    The factory does not create data, pick a default vintage, or open sockets.
    The CLI binds to loopback unless the operator explicitly overrides it.
    """
    catalog = CuratedCatalog(data_root)
    app = FastAPI(
        title="Data Platform Lab — ONS pay serving",
        version="1.0.0",
        description="Read-only, supplier-neutral, SHA-pinned ONS pay analytical tables.",
    )

    @app.exception_handler(CatalogError)
    def integrity_failure(_request: Request, _exc: CatalogError) -> JSONResponse:
        # Never disclose local absolute paths, hashes of unrelated files, or SQL.
        return JSONResponse(status_code=503, content={"detail": "Curated data integrity failure"})

    @app.exception_handler(VintageNotFound)
    def vintage_not_found(_request: Request, _exc: VintageNotFound) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": "Requested vintage not found"})

    @app.exception_handler(ReleaseError)
    def release_integrity_failure(_request: Request, _exc: ReleaseError) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "Active release integrity failure"})

    @app.get("/health")
    def health() -> dict[str, str]:
        """Process liveness; never claims the curated dataset is healthy."""
        return {"status": "ok"}

    @app.get("/v1/ons-pay/vintages")
    def vintages() -> VintagesResponse:
        """List exact version/commit/source pins; no implicit 'latest' choice."""
        return VintagesResponse(
            vintages=[_vintage_response(item) for item in catalog.list_vintages()]
        )

    @app.get("/v1/ons-pay/releases/active")
    def active_release() -> ActiveReleaseResponse:
        """Discover the manually activated pin; data queries still require pins."""
        state = ReleaseStore(data_root).status()
        if state is None:
            raise HTTPException(status_code=404, detail="No manually activated release")
        return ActiveReleaseResponse(
            generation=state.generation,
            event_sha256=state.event_sha256,
            vintage=_vintage_response(catalog.get_vintage(state.active)),
        )

    @app.get("/v1/ons-pay/geography-year")
    def geography_year(
        registry_commit: Commit,
        source_sha256: SourceHash,
        model_version: ModelVersion = 1,
        geography: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
        pay_year: Year = None,
        limit: PageLimit = 50,
        offset: PageOffset = 0,
    ) -> GeographyYearPage:
        """Unweighted geography-type/year distribution of area pay values."""
        vintage = catalog.get_vintage(VintageKey(model_version, registry_commit, source_sha256))
        page = catalog.geography_year(
            vintage, geography=geography, pay_year=pay_year, limit=limit, offset=offset
        )
        try:
            rows = [GeographyYearRow.model_validate(row) for row in page.rows]
        except ValidationError as exc:
            raise CatalogError("curated geography-year schema has drifted") from exc
        return GeographyYearPage(
            vintage=_vintage_response(vintage),
            total=page.total,
            limit=limit,
            offset=offset,
            rows=rows,
        )

    @app.get("/v1/ons-pay/areas/{area_code}/trend")
    def area_trend(
        area_code: Annotated[str, RoutePath(pattern=r"^[A-Za-z][A-Za-z0-9]{0,15}$")],
        registry_commit: Commit,
        source_sha256: SourceHash,
        model_version: ModelVersion = 1,
        start_year: Year = None,
        end_year: Year = None,
        limit: PageLimit = 50,
        offset: PageOffset = 0,
    ) -> AreaTrendPage:
        """Annual reported pay and strictly adjacent nominal YoY for one area."""
        if start_year is not None and end_year is not None and start_year > end_year:
            raise HTTPException(status_code=422, detail="start_year must be <= end_year")
        vintage = catalog.get_vintage(VintageKey(model_version, registry_commit, source_sha256))
        page = catalog.area_trend(
            vintage,
            area_code=area_code,
            start_year=start_year,
            end_year=end_year,
            limit=limit,
            offset=offset,
        )
        try:
            rows = [AreaTrendRow.model_validate(row) for row in page.rows]
        except ValidationError as exc:
            raise CatalogError("curated area trend schema has drifted") from exc
        return AreaTrendPage(
            vintage=_vintage_response(vintage),
            area_code=area_code,
            total=page.total,
            limit=limit,
            offset=offset,
            rows=rows,
        )

    return app
