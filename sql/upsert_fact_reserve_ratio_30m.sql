INSERT INTO fact_reserve_ratio_30m (
    market_ts_jst,
    snapshot_id,
    area_id,
    block_no,
    block_demand_mw,
    block_supply_mw,
    block_reserve_mw,
    block_reserve_rate_pct,
    block_usage_rate_pct,
    area_demand_mw,
    area_supply_mw,
    area_reserve_mw,
    fetched_at
) VALUES (
    %(market_ts_jst)s,
    %(snapshot_id)s,
    %(area_id)s,
    %(block_no)s,
    %(block_demand_mw)s,
    %(block_supply_mw)s,
    %(block_reserve_mw)s,
    %(block_reserve_rate_pct)s,
    %(block_usage_rate_pct)s,
    %(area_demand_mw)s,
    %(area_supply_mw)s,
    %(area_reserve_mw)s,
    %(fetched_at)s
)
ON CONFLICT (market_ts_jst, area_id)
DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    block_no = EXCLUDED.block_no,
    block_demand_mw = EXCLUDED.block_demand_mw,
    block_supply_mw = EXCLUDED.block_supply_mw,
    block_reserve_mw = EXCLUDED.block_reserve_mw,
    block_reserve_rate_pct = EXCLUDED.block_reserve_rate_pct,
    block_usage_rate_pct = EXCLUDED.block_usage_rate_pct,
    area_demand_mw = EXCLUDED.area_demand_mw,
    area_supply_mw = EXCLUDED.area_supply_mw,
    area_reserve_mw = EXCLUDED.area_reserve_mw,
    fetched_at = EXCLUDED.fetched_at;
