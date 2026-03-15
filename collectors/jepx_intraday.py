from __future__ import annotations

from datetime import date

from collectors.base import FetchResponse, fetch_http
from config import SourceConfig


def collect(source: SourceConfig, biz_date: date, mode: str) -> FetchResponse:
    fiscal_year = biz_date.year if biz_date.month >= 4 else biz_date.year - 1
    filename = f"intraday_{fiscal_year}.csv"
    response = fetch_http(
        source.raw_paths["url"],
        method=source.raw_paths["method"],
        headers={
            "Referer": source.page_url,
            "User-Agent": "power-data-platform-poc/0.1",
        },
        data={"dir": source.raw_paths["dir"], "file": filename},
    )
    response.filename = filename
    response.encoding = source.encoding
    response.metadata.update({"fiscal_year": fiscal_year, "requested_mode": mode})
    return response
