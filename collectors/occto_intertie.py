from __future__ import annotations

from datetime import date
from typing import Any

from collectors.base import FetchResponse, fetch_browser_csv, materialize_download
from config import SourceConfig


def collect(source: SourceConfig, biz_date: date, mode: str) -> list[FetchResponse]:
    def interaction(context: Any, page: Any) -> list[FetchResponse]:
        page.locator("#menu1-1").click()
        with context.expect_page() as popup_info:
            page.locator(source.raw_paths["menu_selector"]).click()
        intertie_page = popup_info.value
        intertie_page.wait_for_load_state("domcontentloaded")
        intertie_page.wait_for_selector(source.raw_paths["line_selector"], timeout=60_000)

        options = intertie_page.evaluate(
            """
            (selector) => Array.from(document.querySelectorAll(`${selector} option`))
              .map((option) => ({ value: option.value.trim(), label: (option.textContent || '').trim() }))
              .filter((option) => option.value.length > 0)
            """,
            source.raw_paths["line_selector"],
        )

        results: list[FetchResponse] = []
        for index, option in enumerate(options, start=1):
            intertie_page.fill(source.raw_paths["target_date_selector"], biz_date.strftime("%Y/%m/%d"))
            intertie_page.select_option(source.raw_paths["line_selector"], option["value"])
            intertie_page.locator(source.raw_paths["search_selector"]).click()
            intertie_page.wait_for_timeout(700)
            csv_button = intertie_page.locator(source.raw_paths["csv_selector"])
            if not csv_button.is_enabled():
                continue
            with intertie_page.expect_download(timeout=30_000) as download_info:
                csv_button.click()
                ok_button = intertie_page.locator('.ui-dialog-buttonset button:has-text("OK")').first
                try:
                    ok_button.click(timeout=10_000)
                except Exception:
                    pass
            download = download_info.value
            safe_name = _safe_file_part(option["label"])
            results.append(
                materialize_download(
                    download,
                    f"intertie-{biz_date:%Y%m%d}-{index:02d}-{safe_name}.csv",
                    metadata={"intertie_name": option["label"], "requested_mode": mode},
                )
            )
            intertie_page.wait_for_timeout(300)
        if not results:
            raise RuntimeError(f"no intertie csv available for {biz_date.isoformat()}")
        return results

    raw = fetch_browser_csv(source.raw_paths["login_url"], interaction)
    return list(raw) if isinstance(raw, list) else [raw]


def _safe_file_part(value: str) -> str:
    cleaned = "".join(char if char not in '\\/:*?"<>|' else "_" for char in value)
    return cleaned.replace(" ", "_").strip("_") or "intertie"
