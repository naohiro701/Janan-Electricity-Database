CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Run-level audit is intentionally kept inside source_snapshot for a small PoC.
CREATE TABLE IF NOT EXISTS source_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    biz_date DATE NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('now', 'daily', 'backfill')),
    domain TEXT NOT NULL,
    page_url TEXT NOT NULL,
    fetch_mode TEXT NOT NULL,
    file_format TEXT NOT NULL,
    raw_relpath TEXT NOT NULL,
    raw_filename TEXT NOT NULL,
    raw_sha256 CHAR(64) NOT NULL,
    raw_size_bytes BIGINT NOT NULL CHECK (raw_size_bytes >= 0),
    encoding TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    expected_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    observed_columns JSONB NOT NULL DEFAULT '[]'::jsonb,
    schema_signature CHAR(64) NOT NULL,
    http_status INTEGER,
    content_type TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_name, biz_date, raw_sha256, raw_relpath)
);

CREATE INDEX IF NOT EXISTS idx_source_snapshot_source_date
    ON source_snapshot (source_name, biz_date DESC, fetched_at DESC);

CREATE TABLE IF NOT EXISTS dim_area (
    area_id SMALLSERIAL PRIMARY KEY,
    area_code TEXT NOT NULL UNIQUE,
    area_name TEXT NOT NULL UNIQUE,
    display_order SMALLINT NOT NULL,
    is_market_area BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO dim_area (area_code, area_name, display_order, is_market_area) VALUES
    ('01', '北海道', 1, TRUE),
    ('02', '東北', 2, TRUE),
    ('03', '東京', 3, TRUE),
    ('04', '中部', 4, TRUE),
    ('05', '北陸', 5, TRUE),
    ('06', '関西', 6, TRUE),
    ('07', '中国', 7, TRUE),
    ('08', '四国', 8, TRUE),
    ('09', '九州', 9, TRUE),
    ('10', '沖縄', 10, FALSE)
ON CONFLICT (area_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_market (
    market_id SMALLSERIAL PRIMARY KEY,
    market_code TEXT NOT NULL UNIQUE,
    market_name TEXT NOT NULL UNIQUE,
    description TEXT
);

CREATE TABLE IF NOT EXISTS dim_asset (
    asset_id BIGSERIAL PRIMARY KEY,
    asset_type TEXT NOT NULL,
    asset_code TEXT NOT NULL,
    asset_name TEXT NOT NULL,
    area_id SMALLINT REFERENCES dim_area (area_id),
    from_area_id SMALLINT REFERENCES dim_area (area_id),
    to_area_id SMALLINT REFERENCES dim_area (area_id),
    voltage_kv TEXT,
    source_type TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (asset_type, asset_code)
);

CREATE INDEX IF NOT EXISTS idx_dim_asset_type_name
    ON dim_asset (asset_type, asset_name);

CREATE TABLE IF NOT EXISTS fact_intertie_flow_5m (
    market_ts_jst TIMESTAMP NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES source_snapshot (snapshot_id),
    asset_id BIGINT NOT NULL REFERENCES dim_asset (asset_id),
    actual_flow_mw DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market_ts_jst, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_intertie_snapshot
    ON fact_intertie_flow_5m (snapshot_id);

CREATE TABLE IF NOT EXISTS fact_trunk_flow_30m (
    market_ts_jst TIMESTAMP NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES source_snapshot (snapshot_id),
    asset_id BIGINT NOT NULL REFERENCES dim_asset (asset_id),
    area_id SMALLINT NOT NULL REFERENCES dim_area (area_id),
    flow_mw DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market_ts_jst, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_trunk_area_ts
    ON fact_trunk_flow_30m (area_id, market_ts_jst DESC);

CREATE TABLE IF NOT EXISTS fact_generation_unit_30m (
    market_ts_jst TIMESTAMP NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES source_snapshot (snapshot_id),
    asset_id BIGINT NOT NULL REFERENCES dim_asset (asset_id),
    area_id SMALLINT NOT NULL REFERENCES dim_area (area_id),
    generation_kwh DOUBLE PRECISION NOT NULL,
    daily_total_kwh DOUBLE PRECISION,
    updated_at_jst TIMESTAMP,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market_ts_jst, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_generation_area_ts
    ON fact_generation_unit_30m (area_id, market_ts_jst DESC);

CREATE TABLE IF NOT EXISTS fact_spot_30m (
    market_ts_jst TIMESTAMP NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES source_snapshot (snapshot_id),
    market_id SMALLINT NOT NULL REFERENCES dim_market (market_id),
    area_id SMALLINT NOT NULL REFERENCES dim_area (area_id),
    system_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    area_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    sell_bid_volume_kwh DOUBLE PRECISION NOT NULL,
    buy_bid_volume_kwh DOUBLE PRECISION NOT NULL,
    contracted_volume_kwh DOUBLE PRECISION NOT NULL,
    sell_block_bid_volume_kwh DOUBLE PRECISION NOT NULL,
    sell_block_contract_volume_kwh DOUBLE PRECISION NOT NULL,
    buy_block_bid_volume_kwh DOUBLE PRECISION NOT NULL,
    buy_block_contract_volume_kwh DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market_ts_jst, market_id, area_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_spot_area_ts
    ON fact_spot_30m (area_id, market_ts_jst DESC);

CREATE TABLE IF NOT EXISTS fact_intraday_30m (
    market_ts_jst TIMESTAMP NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES source_snapshot (snapshot_id),
    market_id SMALLINT NOT NULL REFERENCES dim_market (market_id),
    open_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    high_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    low_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    close_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    average_price_jpy_per_kwh DOUBLE PRECISION NOT NULL,
    total_contracted_volume_kwh DOUBLE PRECISION NOT NULL,
    contract_count INTEGER NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market_ts_jst, market_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_intraday_ts
    ON fact_intraday_30m (market_ts_jst DESC);

CREATE TABLE IF NOT EXISTS fact_reserve_ratio_30m (
    market_ts_jst TIMESTAMP NOT NULL,
    snapshot_id BIGINT NOT NULL REFERENCES source_snapshot (snapshot_id),
    area_id SMALLINT NOT NULL REFERENCES dim_area (area_id),
    block_no SMALLINT NOT NULL,
    block_demand_mw DOUBLE PRECISION NOT NULL,
    block_supply_mw DOUBLE PRECISION NOT NULL,
    block_reserve_mw DOUBLE PRECISION NOT NULL,
    block_reserve_rate_pct DOUBLE PRECISION NOT NULL,
    block_usage_rate_pct DOUBLE PRECISION NOT NULL,
    area_demand_mw DOUBLE PRECISION NOT NULL,
    area_supply_mw DOUBLE PRECISION NOT NULL,
    area_reserve_mw DOUBLE PRECISION NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (market_ts_jst, area_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_reserve_area_ts
    ON fact_reserve_ratio_30m (area_id, market_ts_jst DESC);

-- Hypertable policy:
-- 1. partition on market_ts_jst for all fact tables
-- 2. 30 day chunk interval for 30m tables, 14 day chunk interval for 5m table
-- 3. if TimescaleDB is unavailable, tables still work as normal PostgreSQL tables
SELECT create_hypertable('fact_intertie_flow_5m', 'market_ts_jst', if_not_exists => TRUE, chunk_time_interval => INTERVAL '14 days');
SELECT create_hypertable('fact_trunk_flow_30m', 'market_ts_jst', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
SELECT create_hypertable('fact_generation_unit_30m', 'market_ts_jst', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
SELECT create_hypertable('fact_spot_30m', 'market_ts_jst', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
SELECT create_hypertable('fact_intraday_30m', 'market_ts_jst', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');
SELECT create_hypertable('fact_reserve_ratio_30m', 'market_ts_jst', if_not_exists => TRUE, chunk_time_interval => INTERVAL '30 days');

-- Index strategy:
-- - fact tables use the UPSERT key as primary key
-- - per-dimension search indexes prioritize area/time and snapshot lookup
-- - source_snapshot keeps source/date/fetched_at for replay and audit traversal

-- UPSERT key design:
-- - fact_intertie_flow_5m: (market_ts_jst, asset_id)
-- - fact_trunk_flow_30m: (market_ts_jst, asset_id)
-- - fact_generation_unit_30m: (market_ts_jst, asset_id)
-- - fact_spot_30m: (market_ts_jst, market_id, area_id)
-- - fact_intraday_30m: (market_ts_jst, market_id)
-- - fact_reserve_ratio_30m: (market_ts_jst, area_id)
-- These keys allow re-runs to replace metric values while preserving the latest snapshot_id.

-- Retention policy:
-- - raw objects are kept on filesystem for 10 years in the PoC
-- - source_snapshot rows are kept for 10 years
-- - fact tables are kept for 10 years
-- - if TimescaleDB compression/retention is enabled later, keep snapshot rows longer than facts

-- Sample query 1: latest spot prices by area
-- SELECT a.area_name, s.market_ts_jst, s.area_price_jpy_per_kwh
-- FROM fact_spot_30m s
-- JOIN dim_area a ON a.area_id = s.area_id
-- JOIN dim_market m ON m.market_id = s.market_id
-- WHERE m.market_code = 'jepx_spot'
-- ORDER BY s.market_ts_jst DESC, a.display_order
-- LIMIT 48;

-- Sample query 2: intertie actual flow for one line
-- SELECT d.asset_name, f.market_ts_jst, f.actual_flow_mw
-- FROM fact_intertie_flow_5m f
-- JOIN dim_asset d ON d.asset_id = f.asset_id
-- WHERE d.asset_code = '北海道・本州間電力連系設備'
--   AND f.market_ts_jst >= TIMESTAMP '2026-03-15 00:00:00'
--   AND f.market_ts_jst < TIMESTAMP '2026-03-16 00:00:00'
-- ORDER BY f.market_ts_jst;

-- Sample query 3: reserve ratio with snapshot audit
-- SELECT a.area_name, r.market_ts_jst, r.block_reserve_rate_pct, s.raw_sha256, s.raw_relpath
-- FROM fact_reserve_ratio_30m r
-- JOIN dim_area a ON a.area_id = r.area_id
-- JOIN source_snapshot s ON s.snapshot_id = r.snapshot_id
-- WHERE a.area_name = '東京'
-- ORDER BY r.market_ts_jst DESC
-- LIMIT 96;
