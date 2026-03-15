from __future__ import annotations

from datetime import date

import polars as pl

from parsers.common import ParseResult, normalize_columns, normalize_jst, read_csv_bytes, to_float_series


def parse(raw: bytes, *, biz_date: date) -> ParseResult:
    frame, encoding = read_csv_bytes(raw, encoding="cp932")
    frame = normalize_columns(frame)
    raw_columns = list(frame.columns)
    filtered = frame.filter(pl.col("年月日") == biz_date.strftime("%Y/%m/%d"))
    if filtered.is_empty():
        raise RuntimeError(f"jepx intraday raw file does not contain {biz_date.isoformat()}")
    filtered = to_float_series(
        filtered,
        [
            "始値(円/kWh)",
            "高値(円/kWh)",
            "安値(円/kWh)",
            "終値(円/kWh)",
            "平均(円/kWh)",
            "約定量合計(kWh)",
            "約定件数",
        ],
    )
    filtered = normalize_jst(
        filtered,
        date_col="年月日",
        time_code_col="時刻コード",
        interval_minutes=30,
    )
    normalized = filtered.rename(
        {
            "始値(円/kWh)": "open_price_jpy_per_kwh",
            "高値(円/kWh)": "high_price_jpy_per_kwh",
            "安値(円/kWh)": "low_price_jpy_per_kwh",
            "終値(円/kWh)": "close_price_jpy_per_kwh",
            "平均(円/kWh)": "average_price_jpy_per_kwh",
            "約定量合計(kWh)": "total_contracted_volume_kwh",
            "約定件数": "contract_count",
        }
    )
    return ParseResult(
        frame=normalized.select(
            [
                "market_ts_jst",
                "open_price_jpy_per_kwh",
                "high_price_jpy_per_kwh",
                "low_price_jpy_per_kwh",
                "close_price_jpy_per_kwh",
                "average_price_jpy_per_kwh",
                "total_contracted_volume_kwh",
                "contract_count",
            ]
        ),
        raw_columns=raw_columns,
        encoding=encoding,
    )
