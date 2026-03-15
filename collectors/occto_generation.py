from __future__ import annotations

from datetime import date
from typing import Any

from collectors.base import FetchResponse, fetch_browser_csv, materialize_download
from config import SourceConfig


def collect(source: SourceConfig, biz_date: date, mode: str) -> FetchResponse:
    def interaction(context: Any, page: Any) -> FetchResponse:
        agreed_checkbox = page.locator("#agreed")
        if agreed_checkbox.is_visible():
            agreed_checkbox.check()
            with page.expect_navigation(url="**/info/home", timeout=60_000):
                page.locator("#next").click()
        page.goto(source.raw_paths["search_page"], wait_until="domcontentloaded")
        page.locator(source.raw_paths["from_date_selector"]).fill(biz_date.strftime("%Y/%m/%d"))
        page.locator(source.raw_paths["to_date_selector"]).fill(biz_date.strftime("%Y/%m/%d"))
        page.locator(source.raw_paths["search_selector"]).click()

        state = page.wait_for_function(
            """
            (csvSelector) => {
              const csvButton = document.querySelector(csvSelector);
              const bodyText = document.body.innerText;
              const ready = csvButton instanceof HTMLButtonElement
                ? !csvButton.disabled
                : !!csvButton && !csvButton.hasAttribute('disabled');
              if (ready) return 'ready';
              if (/対象データがありません。?/.test(bodyText)) return 'no-data';
              return null;
            }
            """,
            source.raw_paths["csv_selector"],
            timeout=120_000,
        )
        if state.json_value() == "no-data":
            raise RuntimeError(f"generation data not published for {biz_date.isoformat()}")

        with page.expect_download(timeout=60_000) as download_info:
            page.locator(source.raw_paths["csv_selector"]).click()
        download = download_info.value
        return materialize_download(
            download,
            f"generation-{biz_date:%Y%m%d}.csv",
            metadata={"requested_mode": mode},
        )

    raw = fetch_browser_csv(source.raw_paths["login_url"], interaction)
    if isinstance(raw, list):
        raise RuntimeError("generation collector received multiple downloads unexpectedly")
    return raw
