-- TimescaleDB Initialization Script for Crypto Trading
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Table for OHLCV data
CREATE TABLE IF NOT EXISTS ohlcv (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION
);

-- Convert to hypertable
SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);

-- Create an index on symbol and time
CREATE INDEX IF NOT EXISTS ix_symbol_time ON ohlcv (symbol, time DESC);

-- Compression policy (compress data older than 7 days)
ALTER TABLE ohlcv SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol'
);
SELECT add_compression_policy('ohlcv', INTERVAL '7 days');
