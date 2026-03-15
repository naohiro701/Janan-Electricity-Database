from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response

from apps.api.repositories.timeseries_repository import TimeseriesRepository
from apps.api.services.query_service import build_tabular_response, get_repository


router = APIRouter(tags=["spot"])


@router.get("/spot")
def get_spot(
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    area: str | None = Query(default=None),
    asset: str | None = Query(default=None, description="Unused for spot endpoint"),
    limit: int = Query(default=500, ge=1, le=5000),
    format_name: Literal["json", "csv"] = Query(default="json", alias="format"),
    repo: TimeseriesRepository = Depends(get_repository),
) -> Response:
    del asset
    rows = repo.fetch_spot(from_ts=from_ts, to_ts=to_ts, area=area, limit=limit)
    return build_tabular_response(rows, format_name=format_name, filename="spot.csv")
