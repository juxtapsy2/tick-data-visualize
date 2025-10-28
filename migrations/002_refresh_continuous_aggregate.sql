-- Post-Migration: Refresh continuous aggregate after data import
-- Date: 2025-10-28
-- Description: Manually refresh the futures_15s_cagg continuous aggregate
--              Run this AFTER importing CSV data with the data-generator

-- Important: Run this only after CSV data has been imported!

-- Refresh the continuous aggregate to populate with existing data
-- This will aggregate all historical data into 15-second buckets
CALL refresh_continuous_aggregate('futures_15s_cagg', NULL, NULL);

-- Verify the aggregate was populated
SELECT
    COUNT(*) as total_buckets,
    MIN(bucket) as earliest_bucket,
    MAX(bucket) as latest_bucket,
    COUNT(DISTINCT ticker) as unique_tickers
FROM futures_15s_cagg;
