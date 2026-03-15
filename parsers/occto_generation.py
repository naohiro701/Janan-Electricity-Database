from __future__ import annotations

import re
from datetime import date

import polars as pl

from parsers.common import ParseResult, normalize_columns, normalize_jst, parse_local_datetime, read_csv_bytes, to_float_series


SLOT_PATTERN = re.compile(r"^\d{2}:\d{2}\[kWh\]$")


def parse(raw: bytes, *, biz_date: date) -> ParseResult:
    frame, encoding = read_csv_bytes(raw)
    frame = normalize_columns(frame)
    raw_columns = list(frame.columns)
    slot_columns = [column for column in frame.columns if SLOT_PATTERN.fullmatch(column)]
    if not slot_columns:
        raise RuntimeError("generation CSV does not contain 30m slot columns")
    numeric_columns = slot_columns + ["日量[kWh]"]
    frame = to_float_series(frame, numeric_columns)
    melted = frame.melt(
        id_vars=["発電所コード", "エリア", "発電所名", "ユニット名", "発電方式・燃種", "対象日", "日量[kWh]", "更新日時"],
        value_vars=slot_columns,
        variable_name="slot_label",
        value_name="generation_kwh",
    )
    melted = melted.with_columns(pl.col("slot_label").str.replace(r"\[kWh\]", "").alias("slot_label"))
    melted = normalize_jst(melted, date_col="対象日", time_col="slot_label")
    updated_values = [parse_local_datetime(value) for value in melted["更新日時"].to_list()]
    normalized = melted.rename(
        {
            "発電所コード": "plant_code",
            "エリア": "area_name",
            "発電所名": "plant_name",
            "ユニット名": "unit_name",
            "発電方式・燃種": "source_type",
            "日量[kWh]": "daily_total_kwh",
        }
    ).with_columns(
        [
            pl.concat_str([pl.col("plant_code"), pl.col("unit_name")], separator="|").alias("asset_code"),
            pl.col("unit_name").alias("asset_name"),
            pl.Series("updated_at_jst", updated_values, dtype=pl.Datetime),
        ]
    )
    return ParseResult(
        frame=normalized.select(
            [
                "market_ts_jst",
                "area_name",
                "asset_code",
                "asset_name",
                "plant_code",
                "plant_name",
                "unit_name",
                "source_type",
                "generation_kwh",
                "daily_total_kwh",
                "updated_at_jst",
            ]
        ),
        raw_columns=raw_columns,
        encoding=encoding,
    )
