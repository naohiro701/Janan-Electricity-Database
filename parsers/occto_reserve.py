from __future__ import annotations

from datetime import date

from parsers.common import ParseResult, extract_first_matching_zip_member, normalize_columns, normalize_jst, read_csv_bytes, to_float_series


def parse(raw: bytes, *, biz_date: date) -> ParseResult:
    member_name, member_payload = extract_first_matching_zip_member(raw, "エリア名")
    update_line = member_payload.decode("utf-8-sig", errors="replace").splitlines()[0].replace('"', "")
    frame, encoding = read_csv_bytes(member_payload, skip_rows=1)
    frame = normalize_columns(frame)
    raw_columns = list(frame.columns)
    frame = to_float_series(
        frame,
        [
            "ブロックNo",
            "広域ブロック需要(MW)",
            "広域ブロック供給力(MW)",
            "広域ブロック予備力(MW)",
            "広域予備率(%)",
            "広域使用率(%)",
            "エリア需要(MW)",
            "エリア供給力(MW)",
            "エリア予備力(MW)",
        ],
    )
    frame = normalize_jst(frame, date_col="対象年月日", time_col="時刻")
    normalized = frame.rename(
        {
            "ブロックNo": "block_no",
            "エリア名": "area_name",
            "広域ブロック需要(MW)": "block_demand_mw",
            "広域ブロック供給力(MW)": "block_supply_mw",
            "広域ブロック予備力(MW)": "block_reserve_mw",
            "広域予備率(%)": "block_reserve_rate_pct",
            "広域使用率(%)": "block_usage_rate_pct",
            "エリア需要(MW)": "area_demand_mw",
            "エリア供給力(MW)": "area_supply_mw",
            "エリア予備力(MW)": "area_reserve_mw",
        }
    )
    return ParseResult(
        frame=normalized.select(
            [
                "market_ts_jst",
                "area_name",
                "block_no",
                "block_demand_mw",
                "block_supply_mw",
                "block_reserve_mw",
                "block_reserve_rate_pct",
                "block_usage_rate_pct",
                "area_demand_mw",
                "area_supply_mw",
                "area_reserve_mw",
            ]
        ),
        raw_columns=raw_columns,
        encoding=encoding,
        metadata={"zip_member": member_name, "update_line": update_line},
    )
