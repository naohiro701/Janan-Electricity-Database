INSERT INTO fact_intraday_30m (
    market_ts_jst,
    snapshot_id,
    market_id,
    open_price_jpy_per_kwh,
    high_price_jpy_per_kwh,
    low_price_jpy_per_kwh,
    close_price_jpy_per_kwh,
    average_price_jpy_per_kwh,
    total_contracted_volume_kwh,
    contract_count,
    fetched_at
) VALUES (
    %(market_ts_jst)s,
    %(snapshot_id)s,
    %(market_id)s,
    %(open_price_jpy_per_kwh)s,
    %(high_price_jpy_per_kwh)s,
    %(low_price_jpy_per_kwh)s,
    %(close_price_jpy_per_kwh)s,
    %(average_price_jpy_per_kwh)s,
    %(total_contracted_volume_kwh)s,
    %(contract_count)s,
    %(fetched_at)s
)
ON CONFLICT (market_ts_jst, market_id)
DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    open_price_jpy_per_kwh = EXCLUDED.open_price_jpy_per_kwh,
    high_price_jpy_per_kwh = EXCLUDED.high_price_jpy_per_kwh,
    low_price_jpy_per_kwh = EXCLUDED.low_price_jpy_per_kwh,
    close_price_jpy_per_kwh = EXCLUDED.close_price_jpy_per_kwh,
    average_price_jpy_per_kwh = EXCLUDED.average_price_jpy_per_kwh,
    total_contracted_volume_kwh = EXCLUDED.total_contracted_volume_kwh,
    contract_count = EXCLUDED.contract_count,
    fetched_at = EXCLUDED.fetched_at;
