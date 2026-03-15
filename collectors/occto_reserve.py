from __future__ import annotations

from datetime import date

from collectors.base import FetchResponse, fetch_http
from config import SourceConfig


def collect(source: SourceConfig, biz_date: date, mode: str) -> FetchResponse:
    response = fetch_http(
        source.raw_paths["url"],
        method=source.raw_paths["method"],
        params={"range": source.raw_paths["range"], "fromDate": biz_date.strftime("%Y/%m/%d")},
        headers={"User-Agent": "power-data-platform-poc/0.1"},
    )
    response.filename = f"reserve_ratio_{biz_date:%Y%m%d}.zip"
    response.encoding = source.encoding
    response.metadata.update({"download_range": source.raw_paths["range"], "requested_mode": mode})
    return response
