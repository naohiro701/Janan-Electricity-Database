INSERT INTO fact_spot_30m (
    market_ts_jst,
    snapshot_id,
    market_id,
    area_id,
    system_price_jpy_per_kwh,
    area_price_jpy_per_kwh,
    sell_bid_volume_kwh,
    buy_bid_volume_kwh,
    contracted_volume_kwh,
    sell_block_bid_volume_kwh,
    sell_block_contract_volume_kwh,
    buy_block_bid_volume_kwh,
    buy_block_contract_volume_kwh,
    fetched_at
) VALUES (
    %(market_ts_jst)s,
    %(snapshot_id)s,
    %(market_id)s,
    %(area_id)s,
    %(system_price_jpy_per_kwh)s,
    %(area_price_jpy_per_kwh)s,
    %(sell_bid_volume_kwh)s,
    %(buy_bid_volume_kwh)s,
    %(contracted_volume_kwh)s,
    %(sell_block_bid_volume_kwh)s,
    %(sell_block_contract_volume_kwh)s,
    %(buy_block_bid_volume_kwh)s,
    %(buy_block_contract_volume_kwh)s,
    %(fetched_at)s
)
ON CONFLICT (market_ts_jst, market_id, area_id)
DO UPDATE SET
    snapshot_id = EXCLUDED.snapshot_id,
    system_price_jpy_per_kwh = EXCLUDED.system_price_jpy_per_kwh,
    area_price_jpy_per_kwh = EXCLUDED.area_price_jpy_per_kwh,
    sell_bid_volume_kwh = EXCLUDED.sell_bid_volume_kwh,
    buy_bid_volume_kwh = EXCLUDED.buy_bid_volume_kwh,
    contracted_volume_kwh = EXCLUDED.contracted_volume_kwh,
    sell_block_bid_volume_kwh = EXCLUDED.sell_block_bid_volume_kwh,
    sell_block_contract_volume_kwh = EXCLUDED.sell_block_contract_volume_kwh,
    buy_block_bid_volume_kwh = EXCLUDED.buy_block_bid_volume_kwh,
    buy_block_contract_volume_kwh = EXCLUDED.buy_block_contract_volume_kwh,
    fetched_at = EXCLUDED.fetched_at;
