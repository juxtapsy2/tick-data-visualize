-- Create continuous aggregate for index_tick table
-- This provides 15-second aggregated averages for VN30 index data

CREATE MATERIALIZED VIEW IF NOT EXISTS index_tick_15s_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 seconds', ts) AS bucket,
    ticker,
    AVG(last) as avg_last
FROM index_tick
WHERE last IS NOT NULL
GROUP BY bucket, ticker
WITH NO DATA;

-- Add retention policy (keep aggregated data for 90 days)
SELECT add_retention_policy('index_tick_15s_cagg', INTERVAL '90 days', if_not_exists => true);
