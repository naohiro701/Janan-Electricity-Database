from __future__ import annotations

import re
from datetime import date

import polars as pl

from parsers.common import ParseResult, normalize_columns, normalize_jst, read_csv_bytes, to_float_series


SLOT_PATTERN = re.compile(r"^\d{2}:\d{2}$")


def parse(raw: bytes, *, biz_date: date) -> ParseResult:
    frame, encoding = read_csv_bytes(raw, encoding="cp932")
    frame = normalize_columns(frame)
    raw_columns = list(frame.columns)
    slot_columns = [column for column in frame.columns if SLOT_PATTERN.fullmatch(column)]
    if not slot_columns:
        raise RuntimeError("trunk flow CSV does not contain slot columns")
    frame = to_float_series(frame, slot_columns)
    melted = frame.melt(
        id_vars=["対象年月日", "対象エリア", "電圧", "送電線名", "潮流方向(正方向)"],
        value_vars=slot_columns,
        variable_name="slot_label",
        value_name="flow_mw",
    )
    melted = normalize_jst(melted, date_col="対象年月日", time_col="slot_label")
    normalized = melted.rename(
        {
            "対象エリア": "area_name",
            "電圧": "voltage_kv",
            "送電線名": "asset_name",
            "潮流方向(正方向)": "positive_direction_label",
        }
    )
    normalized = normalized.with_columns(
        pl.concat_str(
            [
                pl.col("area_name"),
                pl.col("voltage_kv"),
                pl.col("asset_name"),
            ],
            separator="|",
        ).alias("asset_code")
    )
    return ParseResult(
        frame=normalized.select(
            ["market_ts_jst", "area_name", "asset_code", "asset_name", "voltage_kv", "positive_direction_label", "flow_mw"]
        ),
        raw_columns=raw_columns,
        encoding=encoding,
        metadata={"biz_date": biz_date.isoformat()},
    )
