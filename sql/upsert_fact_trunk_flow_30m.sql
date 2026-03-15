INSERT INTO fact_trunk_flow_30m (
    market_ts_jst,
    snapshot_id,
    asset_id,
    area_id,
    flow_mw,
    fetched_at
) VALUES (
    %(market_ts_jst)s,
    %(snapshot_id)s,
    %(asset_id)s,
    %(area_id)s,
    %(flow_mw)s,
    %(fetched_at)s
)
ON CONFLICT (market_ts_jst, asset_id)
DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    area_id = EXCLUDED.area_id,
    flow_mw = EXCLUDED.flow_mw,
    fetched_at = EXCLUDED.fetched_at;
