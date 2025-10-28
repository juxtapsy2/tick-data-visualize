-- Migration: Add foreign positions and order book columns to futures_table
-- Date: 2025-10-28
-- Description: Adds total_f_buy_vol, total_f_sell_vol, total_bid, total_ask columns
--              to support Chart 2 (Foreign Long/Short) and Chart 3 (Total Bid/Ask)

-- Add new columns to futures_table
ALTER TABLE futures_table
ADD COLUMN IF NOT EXISTS total_f_buy_vol DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS total_f_sell_vol DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS total_bid DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS total_ask DOUBLE PRECISION;

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_futures_total_f_buy_vol ON futures_table(total_f_buy_vol) WHERE total_f_buy_vol IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_futures_total_f_sell_vol ON futures_table(total_f_sell_vol) WHERE total_f_sell_vol IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_futures_total_bid ON futures_table(total_bid) WHERE total_bid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_futures_total_ask ON futures_table(total_ask) WHERE total_ask IS NOT NULL;

-- Update continuous aggregate to include new columns (if exists)
-- Note: This may require dropping and recreating the continuous aggregate
-- depending on TimescaleDB version and aggregate complexity

-- Drop existing continuous aggregate (if it exists)
DROP MATERIALIZED VIEW IF EXISTS futures_15s_cagg CASCADE;

-- Recreate continuous aggregate with new fields
-- Use WITH NO DATA to avoid pipeline error, will refresh manually after
CREATE MATERIALIZED VIEW futures_15s_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 seconds', ts) AS bucket,
    ticker,
    AVG(last) as avg_last,
    AVG(total_f_buy_vol) as avg_total_f_buy_vol,
    AVG(total_f_sell_vol) as avg_total_f_sell_vol,
    AVG(total_bid) as avg_total_bid,
    AVG(total_ask) as avg_total_ask
FROM futures_table
WHERE last IS NOT NULL OR total_f_buy_vol IS NOT NULL OR total_f_sell_vol IS NOT NULL
      OR total_bid IS NOT NULL OR total_ask IS NOT NULL
GROUP BY bucket, ticker
WITH NO DATA;

-- Add retention policy (keep aggregated data for 90 days)
SELECT add_retention_policy('futures_15s_cagg', INTERVAL '90 days', if_not_exists => true);
