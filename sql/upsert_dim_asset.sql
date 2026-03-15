INSERT INTO dim_asset (
    asset_type,
    asset_code,
    asset_name,
    area_id,
    from_area_id,
    to_area_id,
    voltage_kv,
    source_type,
    metadata
) VALUES (
    %(asset_type)s,
    %(asset_code)s,
    %(asset_name)s,
    %(area_id)s,
    %(from_area_id)s,
    %(to_area_id)s,
    %(voltage_kv)s,
    %(source_type)s,
    %(metadata)s
)
ON CONFLICT (asset_type, asset_code)
DO UPDATE SET
    asset_name = EXCLUDED.asset_name,
    area_id = COALESCE(EXCLUDED.area_id, dim_asset.area_id),
    from_area_id = COALESCE(EXCLUDED.from_area_id, dim_asset.from_area_id),
    to_area_id = COALESCE(EXCLUDED.to_area_id, dim_asset.to_area_id),
    voltage_kv = COALESCE(EXCLUDED.voltage_kv, dim_asset.voltage_kv),
    source_type = COALESCE(EXCLUDED.source_type, dim_asset.source_type),
    metadata = dim_asset.metadata || EXCLUDED.metadata,
    updated_at = NOW()
RETURNING asset_id;
