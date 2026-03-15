INSERT INTO fact_intertie_flow_5m (
    market_ts_jst,
    snapshot_id,
    asset_id,
    actual_flow_mw,
    fetched_at
) VALUES (
    %(market_ts_jst)s,
    %(snapshot_id)s,
    %(asset_id)s,
    %(actual_flow_mw)s,
    %(fetched_at)s
)
ON CONFLICT (market_ts_jst, asset_id)
DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    actual_flow_mw = EXCLUDED.actual_flow_mw,
    fetched_at = EXCLUDED.fetched_at;
