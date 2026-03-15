from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import os

import yaml


ROOT_DIR = Path(__file__).resolve().parent
RAW_ROOT = Path(os.getenv("RAW_DATA_ROOT", ROOT_DIR / "data" / "raw"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SOURCES_PATH = Path(os.getenv("SOURCES_PATH", ROOT_DIR / "sources.yaml"))


@dataclass(slots=True)
class SourceConfig:
    source_name: str
    domain: str
    page_url: str
    fetch_mode: str
    file_format: str
    encoding: str
    grain: str
    expected_columns: list[str]
    primary_key: list[str]
    validations: dict[str, Any]
    schedule: dict[str, Any]
    retention: dict[str, Any]
    raw_paths: dict[str, Any]
    parser: dict[str, Any]
    loader: dict[str, Any]
    schema_patterns: dict[str, str] | None = None


def load_sources(path: Path | None = None) -> dict[str, SourceConfig]:
    target = path or SOURCES_PATH
    data = yaml.safe_load(target.read_text(encoding="utf-8"))
    items = {}
    for record in data["sources"]:
        source = SourceConfig(**record)
        items[source.source_name] = source
    return items
