-- Fix continuous aggregate refresh policies to include real-time data
-- Change start_offset from 1 hour to 30 seconds for near real-time updates

-- Remove old policies
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

-- Immediately refresh to backfill today's data from 9:00 AM
CALL refresh_continuous_aggregate('index_tick_15s_cagg',
    (NOW()::date + INTERVAL '9 hours')::timestamptz,
    NOW()::timestamptz
);

CALL refresh_continuous_aggregate('futures_15s_cagg',
    (NOW()::date + INTERVAL '9 hours')::timestamptz,
    NOW()::timestamptz
);
