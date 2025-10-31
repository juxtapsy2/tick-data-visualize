
-- Add automatic refresh policies for continuous aggregates
-- These policies ensure the aggregates are automatically updated as new data arrives

-- Refresh index_tick continuous aggregate every 15 seconds
SELECT add_continuous_aggregate_policy('index_tick_15s_cagg',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '0 seconds',
    schedule_interval => INTERVAL '15 seconds',
    if_not_exists => true
);

-- Refresh futures continuous aggregate every 15 seconds
SELECT add_continuous_aggregate_policy('futures_15s_cagg',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '0 seconds',
    schedule_interval => INTERVAL '15 seconds',
    if_not_exists => true
);
