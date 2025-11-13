-- Migration: Create continuous aggregate for per-ticker bubble chart data
-- Purpose: Aggregate hose500_second data per ticker every 15 seconds for bubble charts
-- Date: 2025-01-11

-- Drop existing continuous aggregate if exists
DROP MATERIALIZED VIEW IF EXISTS ticker_bubble_15s_cagg CASCADE;

-- Create continuous aggregate for per-ticker bubble chart data (15-second buckets)
-- Calculates weighted value per row, then averages for accurate aggregation
CREATE MATERIALIZED VIEW ticker_bubble_15s_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 seconds', h.ts) AS bucket,
    h.ticker,
    h.order_type,
    SUM(h.matched_vol) AS total_matched_vol,
    AVG(h.last) AS avg_last,
    COUNT(*) AS trade_count,
    AVG((h.matched_vol * h.last) / w.weight * 100) AS avg_weighted_value
FROM hose500_second h
INNER JOIN vn30_weights w ON h.ticker = w.ticker
WHERE
    h.order_type IN ('Buy', 'Sell')
    AND h.matched_vol IS NOT NULL
    AND h.last IS NOT NULL
    AND h.matched_vol > 0
    AND h.last > 0
    AND w.weight > 0
    -- Market hours filter: 09:00:00 to 14:45:00, excluding 11:30:01 to 12:59:59
    AND (
        (EXTRACT(HOUR FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 9)
        OR (EXTRACT(HOUR FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 10)
        OR (
            EXTRACT(HOUR FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 11
            AND EXTRACT(MINUTE FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') < 30
        )
        OR (
            EXTRACT(HOUR FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 11
            AND EXTRACT(MINUTE FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 30
            AND EXTRACT(SECOND FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 0
        )
        OR (EXTRACT(HOUR FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 13)
        OR (
            EXTRACT(HOUR FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 14
            AND EXTRACT(MINUTE FROM h.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') <= 45
        )
    )
GROUP BY bucket, h.ticker, h.order_type
WITH NO DATA;

-- Add refresh policy: refresh every 15 seconds, covering last 1 hour
SELECT add_continuous_aggregate_policy('ticker_bubble_15s_cagg',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 second',
    schedule_interval => INTERVAL '15 seconds');

-- Create index on bucket and ticker for faster queries
CREATE INDEX idx_ticker_bubble_15s_cagg_bucket_ticker
ON ticker_bubble_15s_cagg (bucket DESC, ticker);

-- Create index on ticker for filtering
CREATE INDEX idx_ticker_bubble_15s_cagg_ticker
ON ticker_bubble_15s_cagg (ticker);

-- Initial refresh to populate data (last 24 hours)
CALL refresh_continuous_aggregate('ticker_bubble_15s_cagg', NOW() - INTERVAL '24 hours', NOW());
