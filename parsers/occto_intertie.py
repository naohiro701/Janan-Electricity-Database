from __future__ import annotations

from datetime import date

import polars as pl

from parsers.common import ParseResult, normalize_columns, normalize_jst, read_csv_bytes, to_float_series


INTERTIE_AREA_MAP = {
    "北海道・本州間電力連系設備": ("北海道", "東北"),
    "相馬双葉幹線": ("東北", "東京"),
    "周波数変換設備": ("東京", "中部"),
    "三重東近江線": ("中部", "関西"),
    "南福光連系所・南福光変電所の連系設備": ("中部", "北陸"),
    "越前嶺南線": ("北陸", "関西"),
    "西播東岡山線・山崎智頭線": ("関西", "中国"),
    "阿南紀北直流幹線": ("四国", "関西"),
    "本四連系線": ("中国", "四国"),
    "関門連系線": ("中国", "九州"),
    "北陸フェンス": ("中部", "北陸"),
}


def parse(raw: bytes, *, biz_date: date) -> ParseResult:
    frame, encoding = read_csv_bytes(raw, encoding="cp932")
    frame = normalize_columns(frame)
    raw_columns = list(frame.columns)
    frame = to_float_series(frame, ["潮流実績"])
    frame = normalize_jst(frame, date_col="対象日付", time_col="対象時刻", interval_minutes=5)
    normalized = frame.rename({"連系線": "asset_name", "潮流実績": "actual_flow_mw"})
    source_areas = []
    target_areas = []
    for value in normalized["asset_name"].to_list():
        source_area, target_area = INTERTIE_AREA_MAP.get(value, (None, None))
        source_areas.append(source_area)
        target_areas.append(target_area)
    normalized = normalized.with_columns(
        [
            pl.Series("source_area_name", source_areas),
            pl.Series("target_area_name", target_areas),
            pl.col("asset_name").alias("asset_code"),
        ]
    )
    return ParseResult(
        frame=normalized.select(
            ["market_ts_jst", "asset_code", "asset_name", "source_area_name", "target_area_name", "actual_flow_mw"]
        ),
        raw_columns=raw_columns,
        encoding=encoding,
        metadata={"biz_date": biz_date.isoformat()},
    )
