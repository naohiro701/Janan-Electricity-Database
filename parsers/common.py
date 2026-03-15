from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from io import BytesIO
from typing import Any
from zipfile import ZipFile
import re
import unicodedata

import polars as pl


NUMBER_PATTERN = re.compile(r"[,\s]")


@dataclass(slots=True)
class ParseResult:
    frame: pl.DataFrame
    raw_columns: list[str]
    encoding: str
    metadata: dict[str, Any] = field(default_factory=dict)


def detect_encoding(raw: bytes) -> str:
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    for candidate in ("utf-8", "cp932", "shift_jis", "euc_jp"):
        try:
            raw.decode(candidate)
            return candidate
        except UnicodeDecodeError:
            continue
    return "utf-8"


def read_csv_bytes(
    raw: bytes,
    *,
    encoding: str | None = None,
    skip_rows: int = 0,
) -> tuple[pl.DataFrame, str]:
    used_encoding = encoding or detect_encoding(raw)
    text = raw.decode(used_encoding, errors="replace").replace("\ufeff", "")
    frame = pl.read_csv(
        BytesIO(text.encode("utf-8")),
        skip_rows=skip_rows,
        null_values=["", "-", "－"],
        try_parse_dates=False,
        infer_schema_length=1000,
    )
    return frame, used_encoding


def normalize_columns(frame: pl.DataFrame, mapping: dict[str, str] | None = None) -> pl.DataFrame:
    renamed = {column: _clean_column_name(column) for column in frame.columns}
    normalized = frame.rename(renamed)
    if mapping:
        applicable = {key: value for key, value in mapping.items() if key in normalized.columns}
        if applicable:
            normalized = normalized.rename(applicable)
    return normalized


def normalize_jst(
    frame: pl.DataFrame,
    *,
    date_col: str,
    time_col: str | None = None,
    time_code_col: str | None = None,
    interval_minutes: int = 30,
    output_col: str = "market_ts_jst",
) -> pl.DataFrame:
    values: list[datetime] = []
    for row in frame.iter_rows(named=True):
        base_date = _parse_date(row[date_col])
        if time_col:
            values.append(_combine_date_time(base_date, str(row[time_col])))
        elif time_code_col:
            slot_code = int(row[time_code_col])
            values.append(datetime.combine(base_date, time.min) + timedelta(minutes=(slot_code - 1) * interval_minutes))
        else:
            raise ValueError("either time_col or time_code_col is required")
    return frame.with_columns(pl.Series(output_col, values, dtype=pl.Datetime))


def melt_area_columns(
    frame: pl.DataFrame,
    *,
    id_vars: list[str],
    area_column_map: dict[str, str],
    value_name: str,
    variable_name: str = "area_column",
) -> pl.DataFrame:
    inverse = {value: key for key, value in area_column_map.items()}
    melted = frame.melt(
        id_vars=id_vars,
        value_vars=list(area_column_map.values()),
        variable_name=variable_name,
        value_name=value_name,
    )
    return melted.with_columns(
        pl.col(variable_name).replace(inverse).alias("area_name")
    ).drop(variable_name)


def to_float_series(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    expressions = []
    for column in columns:
        if column in frame.columns:
            expressions.append(
                pl.col(column)
                .cast(pl.Utf8)
                .str.replace_all(NUMBER_PATTERN.pattern, "")
                .cast(pl.Float64, strict=False)
                .alias(column)
            )
    return frame.with_columns(expressions) if expressions else frame


def extract_first_matching_zip_member(raw_zip: bytes, header_hint: str) -> tuple[str, bytes]:
    with ZipFile(BytesIO(raw_zip)) as archive:
        for member in archive.namelist():
            payload = archive.read(member)
            text = payload.decode(detect_encoding(payload), errors="replace")
            if header_hint in text:
                return member, payload
    raise RuntimeError(f"zip member containing '{header_hint}' was not found")


def parse_local_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip().replace("\u3000", " ")
    for fmt in ("%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    return None


def _clean_column_name(value: str) -> str:
    return unicodedata.normalize("NFKC", value).replace("\ufeff", "").strip().strip('"')


def _parse_date(raw_value: Any) -> date:
    value = str(raw_value).strip()
    for fmt in ("%Y/%m/%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unsupported date value: {value}")


def _combine_date_time(base_date: date, raw_time: str) -> datetime:
    cleaned = raw_time.replace("[kWh]", "").strip()
    if not re.fullmatch(r"\d{2}:\d{2}", cleaned):
        raise ValueError(f"unsupported time value: {raw_time}")
    hour, minute = map(int, cleaned.split(":"))
    if hour == 24 and minute == 0:
        return datetime.combine(base_date + timedelta(days=1), time.min)
    return datetime.combine(base_date, time(hour=hour, minute=minute))
