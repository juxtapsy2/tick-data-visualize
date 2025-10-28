-- Recreate continuous aggregate to group by f (f1, f2, f3, f4) instead of ticker
-- This allows querying by futures contract type dynamically

-- Drop existing continuous aggregate
DROP MATERIALIZED VIEW IF EXISTS futures_15s_cagg CASCADE;

-- Recreate with f column in GROUP BY
CREATE MATERIALIZED VIEW futures_15s_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 seconds', ts) AS bucket,
    f,
    AVG(last) as avg_last,
    AVG(total_f_buy_vol) as avg_total_f_buy_vol,
    AVG(total_f_sell_vol) as avg_total_f_sell_vol,
    AVG(total_bid) as avg_total_bid,
    AVG(total_ask) as avg_total_ask
FROM futures_table
WHERE last IS NOT NULL OR total_f_buy_vol IS NOT NULL OR total_f_sell_vol IS NOT NULL
      OR total_bid IS NOT NULL OR total_ask IS NOT NULL
GROUP BY bucket, f
WITH NO DATA;

-- Add retention policy (keep aggregated data for 90 days)
SELECT add_retention_policy('futures_15s_cagg', INTERVAL '90 days', if_not_exists => true);

-- Refresh the continuous aggregate to populate it with existing data
CALL refresh_continuous_aggregate('futures_15s_cagg', NULL, NULL);
