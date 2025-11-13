-- Migration: Create VN30 weights table
-- Purpose: Store VN30 ticker weights for bubble chart normalization
-- Date: 2025-11-12

-- Drop existing table if exists
DROP TABLE IF EXISTS vn30_weights CASCADE;

-- Create VN30 weights table
CREATE TABLE vn30_weights (
    ticker VARCHAR(10) PRIMARY KEY,
    weight NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert VN30 weights from portfolio table
INSERT INTO vn30_weights (ticker, weight) VALUES
    ('ACB', 4.15),
    ('BCM', 0.20),
    ('BID', 0.36),
    ('CTG', 1.56),
    ('DGC', 1.68),
    ('FPT', 7.08),
    ('GAS', 0.55),
    ('GVR', 0.34),
    ('HDB', 3.00),
    ('HPG', 8.75),
    ('LPB', 5.26),
    ('MBB', 3.92),
    ('MSN', 4.68),
    ('MWG', 6.36),
    ('PLX', 0.31),
    ('SAB', 0.41),
    ('SHB', 1.95),
    ('SSB', 1.12),
    ('SSI', 1.99),
    ('STB', 3.99),
    ('TCB', 5.50),
    ('TPB', 1.05),
    ('VCB', 2.08),
    ('VHM', 5.52),
    ('VIB', 1.52),
    ('VIC', 11.86),
    ('VJC', 2.72),
    ('VNM', 3.96),
    ('VPB', 4.97),
    ('VRE', 2.21);

-- Create index on ticker for faster lookups
CREATE INDEX idx_vn30_weights_ticker ON vn30_weights (ticker);
