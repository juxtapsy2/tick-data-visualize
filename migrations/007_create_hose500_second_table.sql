-- Create hose500_second table for HOSE stock market second-level data
-- This table stores detailed order book and trading data for HOSE stocks

CREATE TABLE IF NOT EXISTS hose500_second (
    ts TIMESTAMPTZ NOT NULL,
    timestamp BIGINT NOT NULL,
    formatted_time TEXT,
    session TEXT,
    ticker TEXT NOT NULL,
    table_name TEXT,
    order_type TEXT, -- 'Buy', 'Sell', 'Unknown'
    last_vol DOUBLE PRECISION,
    matched_vol DOUBLE PRECISION,
    delta_bid1 DOUBLE PRECISION,
    delta_bid2 DOUBLE PRECISION,
    delta_bid3 DOUBLE PRECISION,
    delta_ask1 DOUBLE PRECISION,
    delta_ask2 DOUBLE PRECISION,
    delta_ask3 DOUBLE PRECISION,
    delta_ff_buy DOUBLE PRECISION,
    delta_ff_sell DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    last DOUBLE PRECISION,
    change DOUBLE PRECISION,
    pct_change TEXT,
    total_vol DOUBLE PRECISION,
    total_val DOUBLE PRECISION,
    average DOUBLE PRECISION,
    exchange TEXT,
    total_f_buy_vol DOUBLE PRECISION,
    total_f_buy_val DOUBLE PRECISION,
    total_f_sell_vol DOUBLE PRECISION,
    total_f_sell_val DOUBLE PRECISION,
    total_bid DOUBLE PRECISION,
    total_ask DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    bid1 DOUBLE PRECISION,
    bid1_vol DOUBLE PRECISION,
    bid2 DOUBLE PRECISION,
    bid2_vol DOUBLE PRECISION,
    bid3 DOUBLE PRECISION,
    bid3_vol DOUBLE PRECISION,
    ask1 DOUBLE PRECISION,
    ask1_vol DOUBLE PRECISION,
    ask2 DOUBLE PRECISION,
    ask2_vol DOUBLE PRECISION,
    ask3 DOUBLE PRECISION,
    ask3_vol DOUBLE PRECISION,
    ceiling DOUBLE PRECISION,
    floor DOUBLE PRECISION,
    ref DOUBLE PRECISION,
    maturity_date TEXT,
    f_room DOUBLE PRECISION,
    security_type TEXT,
    warnings TEXT,
    options_type TEXT,
    underlying TEXT,
    strike DOUBLE PRECISION,
    listed_shares DOUBLE PRECISION,
    issuer TEXT,
    batch_price DOUBLE PRECISION,
    batch_vol DOUBLE PRECISION,
    batch_change DOUBLE PRECISION,
    batch_pct_change DOUBLE PRECISION,
    last_session_timestamp BIGINT,
    timestamp2 BIGINT,
    category TEXT,
    CONSTRAINT hose500_second_unique UNIQUE (ts, ticker, timestamp)
);

-- Create hypertable
SELECT create_hypertable('hose500_second', 'ts', if_not_exists => TRUE);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_hose500_ticker_ts ON hose500_second (ticker, ts DESC);
CREATE INDEX IF NOT EXISTS idx_hose500_ts ON hose500_second (ts DESC);
CREATE INDEX IF NOT EXISTS idx_hose500_category ON hose500_second (category, ts DESC);
