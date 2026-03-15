from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query
from fastapi.responses import Response

from apps.api.repositories.timeseries_repository import TimeseriesRepository
from apps.api.services.query_service import build_tabular_response, get_repository


router = APIRouter(tags=["intertie-flow"])


@router.get("/intertie-flow")
def get_intertie_flow(
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    area: str | None = Query(default=None, description="Unused for intertie endpoint"),
    asset: str | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    format_name: Literal["json", "csv"] = Query(default="json", alias="format"),
    repo: TimeseriesRepository = Depends(get_repository),
) -> Response:
    del area
    rows = repo.fetch_intertie_flow(from_ts=from_ts, to_ts=to_ts, asset=asset, limit=limit)
    return build_tabular_response(rows, format_name=format_name, filename="intertie-flow.csv")
