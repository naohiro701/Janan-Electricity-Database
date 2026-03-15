from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row


class TimeseriesRepository:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    def fetch_spot(
        self,
        *,
        from_ts: datetime | None,
        to_ts: datetime | None,
        area: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            s.market_ts_jst,
            a.area_name,
            s.system_price_jpy_per_kwh,
            s.area_price_jpy_per_kwh,
            s.sell_bid_volume_kwh,
            s.buy_bid_volume_kwh,
            s.contracted_volume_kwh
        FROM fact_spot_30m s
        JOIN dim_area a ON a.area_id = s.area_id
        JOIN dim_market m ON m.market_id = s.market_id
        WHERE m.market_code = 'jepx_spot'
          AND (%(from_ts)s IS NULL OR s.market_ts_jst >= %(from_ts)s)
          AND (%(to_ts)s IS NULL OR s.market_ts_jst <= %(to_ts)s)
          AND (%(area)s IS NULL OR a.area_name = %(area)s)
        ORDER BY s.market_ts_jst DESC, a.display_order
        LIMIT %(limit)s
        """
        return self._fetch(sql, {"from_ts": from_ts, "to_ts": to_ts, "area": area, "limit": limit})

    def fetch_intraday(
        self,
        *,
        from_ts: datetime | None,
        to_ts: datetime | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            market_ts_jst,
            open_price_jpy_per_kwh,
            high_price_jpy_per_kwh,
            low_price_jpy_per_kwh,
            close_price_jpy_per_kwh,
            average_price_jpy_per_kwh,
            total_contracted_volume_kwh,
            contract_count
        FROM fact_intraday_30m
        WHERE (%(from_ts)s IS NULL OR market_ts_jst >= %(from_ts)s)
          AND (%(to_ts)s IS NULL OR market_ts_jst <= %(to_ts)s)
        ORDER BY market_ts_jst DESC
        LIMIT %(limit)s
        """
        return self._fetch(sql, {"from_ts": from_ts, "to_ts": to_ts, "limit": limit})

    def fetch_reserve_ratio(
        self,
        *,
        from_ts: datetime | None,
        to_ts: datetime | None,
        area: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            r.market_ts_jst,
            a.area_name,
            r.block_no,
            r.block_reserve_rate_pct,
            r.block_reserve_mw,
            r.area_reserve_mw,
            r.area_demand_mw,
            r.area_supply_mw
        FROM fact_reserve_ratio_30m r
        JOIN dim_area a ON a.area_id = r.area_id
        WHERE (%(from_ts)s IS NULL OR r.market_ts_jst >= %(from_ts)s)
          AND (%(to_ts)s IS NULL OR r.market_ts_jst <= %(to_ts)s)
          AND (%(area)s IS NULL OR a.area_name = %(area)s)
        ORDER BY r.market_ts_jst DESC, a.display_order
        LIMIT %(limit)s
        """
        return self._fetch(sql, {"from_ts": from_ts, "to_ts": to_ts, "area": area, "limit": limit})

    def fetch_intertie_flow(
        self,
        *,
        from_ts: datetime | None,
        to_ts: datetime | None,
        asset: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            f.market_ts_jst,
            d.asset_name,
            fa.area_name AS from_area,
            ta.area_name AS to_area,
            f.actual_flow_mw
        FROM fact_intertie_flow_5m f
        JOIN dim_asset d ON d.asset_id = f.asset_id
        LEFT JOIN dim_area fa ON fa.area_id = d.from_area_id
        LEFT JOIN dim_area ta ON ta.area_id = d.to_area_id
        WHERE (%(from_ts)s IS NULL OR f.market_ts_jst >= %(from_ts)s)
          AND (%(to_ts)s IS NULL OR f.market_ts_jst <= %(to_ts)s)
          AND (%(asset)s IS NULL OR d.asset_name = %(asset)s OR d.asset_code = %(asset)s)
        ORDER BY f.market_ts_jst DESC, d.asset_name
        LIMIT %(limit)s
        """
        return self._fetch(sql, {"from_ts": from_ts, "to_ts": to_ts, "asset": asset, "limit": limit})

    def fetch_trunk_flow(
        self,
        *,
        from_ts: datetime | None,
        to_ts: datetime | None,
        area: str | None,
        asset: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            f.market_ts_jst,
            a.area_name,
            d.asset_name,
            d.voltage_kv,
            f.flow_mw
        FROM fact_trunk_flow_30m f
        JOIN dim_asset d ON d.asset_id = f.asset_id
        JOIN dim_area a ON a.area_id = f.area_id
        WHERE (%(from_ts)s IS NULL OR f.market_ts_jst >= %(from_ts)s)
          AND (%(to_ts)s IS NULL OR f.market_ts_jst <= %(to_ts)s)
          AND (%(area)s IS NULL OR a.area_name = %(area)s)
          AND (%(asset)s IS NULL OR d.asset_name = %(asset)s OR d.asset_code = %(asset)s)
        ORDER BY f.market_ts_jst DESC, a.display_order, d.asset_name
        LIMIT %(limit)s
        """
        return self._fetch(
            sql,
            {"from_ts": from_ts, "to_ts": to_ts, "area": area, "asset": asset, "limit": limit},
        )

    def fetch_generation_unit(
        self,
        *,
        from_ts: datetime | None,
        to_ts: datetime | None,
        area: str | None,
        asset: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            f.market_ts_jst,
            a.area_name,
            d.asset_code,
            d.asset_name,
            d.source_type,
            f.generation_kwh,
            f.daily_total_kwh,
            f.updated_at_jst
        FROM fact_generation_unit_30m f
        JOIN dim_asset d ON d.asset_id = f.asset_id
        JOIN dim_area a ON a.area_id = f.area_id
        WHERE (%(from_ts)s IS NULL OR f.market_ts_jst >= %(from_ts)s)
          AND (%(to_ts)s IS NULL OR f.market_ts_jst <= %(to_ts)s)
          AND (%(area)s IS NULL OR a.area_name = %(area)s)
          AND (%(asset)s IS NULL OR d.asset_name = %(asset)s OR d.asset_code = %(asset)s)
        ORDER BY f.market_ts_jst DESC, a.display_order, d.asset_name
        LIMIT %(limit)s
        """
        return self._fetch(
            sql,
            {"from_ts": from_ts, "to_ts": to_ts, "area": area, "asset": asset, "limit": limit},
        )

    def _fetch(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
            return list(conn.execute(sql, params).fetchall())
