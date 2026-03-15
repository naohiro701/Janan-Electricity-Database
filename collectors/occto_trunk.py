from __future__ import annotations

from datetime import date
from typing import Any

from collectors.base import FetchResponse, fetch_browser_csv, materialize_download
from config import SourceConfig


FLOW_AREAS = (
    ("01", "北海道"),
    ("02", "東北"),
    ("03", "東京"),
    ("04", "中部"),
    ("05", "北陸"),
    ("06", "関西"),
    ("07", "中国"),
    ("08", "四国"),
    ("09", "九州"),
    ("10", "沖縄"),
)


def collect(source: SourceConfig, biz_date: date, mode: str) -> list[FetchResponse]:
    def interaction(context: Any, page: Any) -> list[FetchResponse]:
        page.locator("#menu1-2").click()
        with context.expect_page() as popup_info:
            page.locator(source.raw_paths["menu_selector"]).click()
        flow_page = popup_info.value
        flow_page.wait_for_load_state("domcontentloaded")

        results: list[FetchResponse] = []
        for area_code, area_name in FLOW_AREAS:
            flow_page.fill(source.raw_paths["target_date_selector"], biz_date.strftime("%Y/%m/%d"))
            flow_page.select_option(source.raw_paths["area_selector"], area_code)
            flow_page.locator(source.raw_paths["search_selector"]).click()
            flow_page.wait_for_selector(f'{source.raw_paths["csv_selector"]}:not([disabled])', timeout=120_000)
            with flow_page.expect_download(timeout=60_000) as download_info:
                flow_page.locator(source.raw_paths["csv_selector"]).click()
                flow_page.get_by_role("button", name="OK").click()
            download = download_info.value
            results.append(
                materialize_download(
                    download,
                    f"trunk-{biz_date:%Y%m%d}-{area_code}-{area_name}.csv",
                    metadata={"area_code": area_code, "area_name": area_name, "requested_mode": mode},
                )
            )
        return results

    raw = fetch_browser_csv(source.raw_paths["login_url"], interaction)
    return list(raw) if isinstance(raw, list) else [raw]
