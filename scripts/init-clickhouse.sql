-- ClickHouse Initialization Script for Crypto Trading
CREATE DATABASE IF NOT EXISTS trading;

-- Table for trade logs (high performance inserts/analytics)
CREATE TABLE IF NOT EXISTS trading.trade_logs (
    timestamp DateTime,
    symbol String,
    direction String,
    size Float32,
    entry_price Float32,
    pnl Float32,
    reason String
) ENGINE = MergeTree()
ORDER BY (timestamp, symbol);
