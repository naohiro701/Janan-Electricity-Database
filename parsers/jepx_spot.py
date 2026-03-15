from __future__ import annotations

from datetime import date

import polars as pl

from parsers.common import ParseResult, melt_area_columns, normalize_columns, normalize_jst, read_csv_bytes, to_float_series


AREA_PRICE_COLUMNS = {
    "北海道": "エリアプライス北海道(円/kWh)",
    "東北": "エリアプライス東北(円/kWh)",
    "東京": "エリアプライス東京(円/kWh)",
    "中部": "エリアプライス中部(円/kWh)",
    "北陸": "エリアプライス北陸(円/kWh)",
    "関西": "エリアプライス関西(円/kWh)",
    "中国": "エリアプライス中国(円/kWh)",
    "四国": "エリアプライス四国(円/kWh)",
    "九州": "エリアプライス九州(円/kWh)",
}


def parse(raw: bytes, *, biz_date: date) -> ParseResult:
    frame, encoding = read_csv_bytes(raw, encoding="cp932")
    frame = normalize_columns(frame)
    raw_columns = list(frame.columns)
    filtered = frame.filter(pl.col("受渡日") == biz_date.strftime("%Y/%m/%d"))
    if filtered.is_empty():
        raise RuntimeError(f"jepx spot raw file does not contain {biz_date.isoformat()}")
    filtered = to_float_series(
        filtered,
        [
            "売り入札量(kWh)",
            "買い入札量(kWh)",
            "約定総量(kWh)",
            "システムプライス(円/kWh)",
            *AREA_PRICE_COLUMNS.values(),
            "売りブロック入札総量(kWh)",
            "売りブロック約定総量(kWh)",
            "買いブロック入札総量(kWh)",
            "買いブロック約定総量(kWh)",
        ],
    )
    filtered = normalize_jst(
        filtered,
        date_col="受渡日",
        time_code_col="時刻コード",
        interval_minutes=30,
    )
    filtered = filtered.rename(
        {
            "売り入札量(kWh)": "sell_bid_volume_kwh",
            "買い入札量(kWh)": "buy_bid_volume_kwh",
            "約定総量(kWh)": "contracted_volume_kwh",
            "システムプライス(円/kWh)": "system_price_jpy_per_kwh",
            "売りブロック入札総量(kWh)": "sell_block_bid_volume_kwh",
            "売りブロック約定総量(kWh)": "sell_block_contract_volume_kwh",
            "買いブロック入札総量(kWh)": "buy_block_bid_volume_kwh",
            "買いブロック約定総量(kWh)": "buy_block_contract_volume_kwh",
        }
    )
    melted = melt_area_columns(
        filtered,
        id_vars=[
            "market_ts_jst",
            "system_price_jpy_per_kwh",
            "sell_bid_volume_kwh",
            "buy_bid_volume_kwh",
            "contracted_volume_kwh",
            "sell_block_bid_volume_kwh",
            "sell_block_contract_volume_kwh",
            "buy_block_bid_volume_kwh",
            "buy_block_contract_volume_kwh",
        ],
        area_column_map=AREA_PRICE_COLUMNS,
        value_name="area_price_jpy_per_kwh",
    )
    return ParseResult(
        frame=melted.select(
            [
                "market_ts_jst",
                "area_name",
                "system_price_jpy_per_kwh",
                "area_price_jpy_per_kwh",
                "sell_bid_volume_kwh",
                "buy_bid_volume_kwh",
                "contracted_volume_kwh",
                "sell_block_bid_volume_kwh",
                "sell_block_contract_volume_kwh",
                "buy_block_bid_volume_kwh",
                "buy_block_contract_volume_kwh",
            ]
        ),
        raw_columns=raw_columns,
        encoding=encoding,
    )
