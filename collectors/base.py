from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo
import logging

import httpx

from config import RAW_ROOT, SourceConfig


JST = ZoneInfo("Asia/Tokyo")


@dataclass(slots=True)
class FetchResponse:
    body: bytes
    filename: str
    content_type: str | None = None
    http_status: int | None = None
    encoding: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CollectedRaw:
    source_name: str
    biz_date: date
    mode: str
    saved_path: Path
    raw_filename: str
    raw_sha256: str
    raw_size_bytes: int
    fetched_at: datetime
    encoding: str
    content_type: str | None
    http_status: int | None
    body: bytes = field(repr=False)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def raw_relpath(self) -> str:
        return self.saved_path.as_posix()


def build_logger(source_name: str, biz_date: date, mode: str) -> logging.Logger:
    logger = logging.getLogger(f"collector.{source_name}")
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s",
        )
    return logger


def sha256_bytes(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def fetch_http(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    timeout_seconds: float = 120.0,
) -> FetchResponse:
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True) as client:
        response = client.request(method=method, url=url, headers=headers, params=params, data=data)
        response.raise_for_status()
        if not response.content:
            raise RuntimeError(f"empty response body from {url}")
        filename = _filename_from_headers(response.headers) or "download.bin"
        return FetchResponse(
            body=response.content,
            filename=filename,
            content_type=response.headers.get("content-type"),
            http_status=response.status_code,
            encoding=response.encoding or None,
        )


def fetch_browser_csv(
    start_url: str,
    interaction: Callable[[Any, Any], FetchResponse | list[FetchResponse]],
) -> FetchResponse | list[FetchResponse]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("playwright is required for browser-based collectors") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        try:
            page.goto(start_url, wait_until="domcontentloaded")
            result = interaction(context, page)
            if isinstance(result, list) and not result:
                raise RuntimeError(f"no downloadable CSV returned from {start_url}")
            return result
        finally:
            context.close()
            browser.close()


def materialize_download(download: Any, filename_hint: str, metadata: dict[str, Any] | None = None) -> FetchResponse:
    with NamedTemporaryFile(delete=True) as tmp:
        download.save_as(tmp.name)
        payload = Path(tmp.name).read_bytes()
    if not payload:
        raise RuntimeError(f"download produced an empty file: {filename_hint}")
    return FetchResponse(body=payload, filename=filename_hint, metadata=metadata or {})


def save_raw(
    source_name: str,
    biz_date: date,
    mode: str,
    fetched: FetchResponse,
    *,
    root_dir: Path = RAW_ROOT,
) -> CollectedRaw:
    biz_date_dir = root_dir / source_name / biz_date.isoformat()
    biz_date_dir.mkdir(parents=True, exist_ok=True)
    saved_path = biz_date_dir / fetched.filename
    saved_path.write_bytes(fetched.body)
    fetched_at = datetime.now(tz=JST)
    return CollectedRaw(
        source_name=source_name,
        biz_date=biz_date,
        mode=mode,
        saved_path=saved_path,
        raw_filename=fetched.filename,
        raw_sha256=sha256_bytes(fetched.body),
        raw_size_bytes=len(fetched.body),
        fetched_at=fetched_at,
        encoding=fetched.encoding or "",
        content_type=fetched.content_type,
        http_status=fetched.http_status,
        body=fetched.body,
        metadata=fetched.metadata,
    )


def run_collector(
    source: SourceConfig,
    biz_date: date,
    mode: str,
    collector: Callable[[SourceConfig, date, str], FetchResponse | Iterable[FetchResponse]],
) -> list[CollectedRaw]:
    logger = build_logger(source.source_name, biz_date, mode)
    logger.info(
        "collector_started source_name=%s biz_date=%s mode=%s",
        source.source_name,
        biz_date.isoformat(),
        mode,
    )
    fetched = collector(source, biz_date, mode)
    if isinstance(fetched, FetchResponse):
        fetched_items = [fetched]
    else:
        fetched_items = list(fetched)
    if not fetched_items:
        raise RuntimeError(f"collector returned no raw payloads for {source.source_name} {biz_date.isoformat()}")
    collected = [save_raw(source.source_name, biz_date, mode, item) for item in fetched_items]
    logger.info(
        "collector_finished source_name=%s biz_date=%s mode=%s payload_count=%s",
        source.source_name,
        biz_date.isoformat(),
        mode,
        len(collected),
    )
    return collected


def _filename_from_headers(headers: httpx.Headers) -> str | None:
    content_disposition = headers.get("content-disposition", "")
    if "filename=" not in content_disposition:
        return None
    parts = content_disposition.split("filename=", 1)[1].strip()
    return parts.strip('"')
