SELECT remove_continuous_aggregate_policy('index_tick_15s_cagg', if_exists => true);
SELECT remove_continuous_aggregate_policy('futures_15s_cagg', if_exists => true);

-- Add new policies with 30-second start_offset for real-time data
-- This ensures data from market open (9:00 AM) is included immediately
SELECT add_continuous_aggregate_policy('index_tick_15s_cagg',
    start_offset => INTERVAL '30 seconds',  -- Only 30 seconds behind real-time
    end_offset => INTERVAL '0 seconds',     -- Up to current time
    schedule_interval => INTERVAL '15 seconds',  -- Refresh every 15 seconds
    if_not_exists => true
);

SELECT add_continuous_aggregate_policy('futures_15s_cagg',
    start_offset => INTERVAL '30 seconds',  -- Only 30 seconds behind real-time
    end_offset => INTERVAL '0 seconds',     -- Up to current time
    schedule_interval => INTERVAL '15 seconds',  -- Refresh every 15 seconds
    if_not_exists => true
);

-- VN30: Single-level 15-second aggregate (efficient)
-- Sum all VN30 data in each 15-second bucket, then divide by count to get average
DROP MATERIALIZED VIEW IF EXISTS vn30_15s_cagg CASCADE;

CREATE MATERIALIZED VIEW vn30_15s_cagg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 seconds', ts) AS bucket,
    -- Average = SUM / COUNT for all VN30 stocks in the 15-second bucket
    SUM(COALESCE(bid1 * bid1_vol, 0) + COALESCE(bid2 * bid2_vol, 0) + COALESCE(bid3 * bid3_vol, 0)) / NULLIF(COUNT(*), 0) AS total_buy_order,
    SUM(COALESCE(ask1 * ask1_vol, 0) + COALESCE(ask2 * ask2_vol, 0) + COALESCE(ask3 * ask3_vol, 0)) / NULLIF(COUNT(*), 0) AS total_sell_order,
    SUM(CASE WHEN order_type = 'Buy' THEN COALESCE(matched_vol * last, 0) ELSE 0 END) / NULLIF(COUNT(*), 0) AS total_buy_up,
    SUM(CASE WHEN order_type = 'Sell' THEN COALESCE(matched_vol * last, 0) ELSE 0 END) / NULLIF(COUNT(*), 0) AS total_sell_down,
    SUM(COALESCE(total_f_buy_val, 0)) / NULLIF(COUNT(*), 0) AS total_f_buy_val,
    SUM(COALESCE(total_f_sell_val, 0)) / NULLIF(COUNT(*), 0) AS total_f_sell_val,
    SUM(COALESCE(total_f_buy_val, 0) - COALESCE(total_f_sell_val, 0)) / NULLIF(COUNT(*), 0) AS foreign_net_val
FROM hose500_second
WHERE ticker IN (
    'ACB', 'BCM', 'BID', 'CTG', 'DGC', 'FPT', 'GAS', 'GVR', 'HDB', 'HPG',
    'LPB', 'MBB', 'MSN', 'MWG', 'PLX', 'SAB', 'SHB', 'SSB', 'SSI', 'STB',
    'TCB', 'TPB', 'VCB', 'VHM', 'VIB', 'VIC', 'VJC', 'VNM', 'VPB', 'VRE'
)
GROUP BY bucket
WITH NO DATA;

-- Add refresh policy
SELECT remove_continuous_aggregate_policy('vn30_15s_cagg', if_exists => true);
SELECT add_continuous_aggregate_policy('vn30_15s_cagg',
    start_offset => INTERVAL '30 seconds',
    end_offset => INTERVAL '0 seconds',
    schedule_interval => INTERVAL '15 seconds');

-- Create index
CREATE INDEX IF NOT EXISTS idx_vn30_15s_cagg_bucket ON vn30_15s_cagg (bucket DESC);

-- Refresh all continuous aggregates
CALL refresh_continuous_aggregate('index_tick_15s_cagg', NOW() - INTERVAL '24 hours', NOW());
CALL refresh_continuous_aggregate('futures_15s_cagg', NOW() - INTERVAL '24 hours', NOW());
CALL refresh_continuous_aggregate('vn30_15s_cagg', NOW() - INTERVAL '24 hours', NOW());