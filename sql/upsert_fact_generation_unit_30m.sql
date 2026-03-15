INSERT INTO fact_generation_unit_30m (
    market_ts_jst,
    snapshot_id,
    asset_id,
    area_id,
    generation_kwh,
    daily_total_kwh,
    updated_at_jst,
    fetched_at
) VALUES (
    %(market_ts_jst)s,
    %(snapshot_id)s,
    %(asset_id)s,
    %(area_id)s,
    %(generation_kwh)s,
    %(daily_total_kwh)s,
    %(updated_at_jst)s,
    %(fetched_at)s
)
ON CONFLICT (market_ts_jst, asset_id)
DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    area_id = EXCLUDED.area_id,
    generation_kwh = EXCLUDED.generation_kwh,
    daily_total_kwh = EXCLUDED.daily_total_kwh,
    updated_at_jst = EXCLUDED.updated_at_jst,
    fetched_at = EXCLUDED.fetched_at;
