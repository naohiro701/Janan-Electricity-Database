from __future__ import annotations

from csv import DictWriter
from io import StringIO
from typing import Any

from fastapi.encoders import jsonable_encoder
from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response

from config import DATABASE_URL
from apps.api.repositories.timeseries_repository import TimeseriesRepository


def get_repository() -> TimeseriesRepository:
    if not DATABASE_URL:
        raise HTTPException(status_code=500, detail="DATABASE_URL is not configured")
    return TimeseriesRepository(DATABASE_URL)


def build_tabular_response(rows: list[dict[str, Any]], *, format_name: str, filename: str) -> Response:
    if format_name == "csv":
        return Response(
            content=_rows_to_csv(rows),
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    return JSONResponse(content=jsonable_encoder(rows))


def _rows_to_csv(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    buffer = StringIO()
    writer = DictWriter(buffer, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()
