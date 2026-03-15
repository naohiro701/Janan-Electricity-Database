INSERT INTO dim_market (
    market_code,
    market_name,
    description
) VALUES (
    %(market_code)s,
    %(market_name)s,
    %(description)s
)
ON CONFLICT (market_code)
DO UPDATE SET
    market_name = EXCLUDED.market_name,
    description = EXCLUDED.description
RETURNING market_id;
