-- ============================================================
-- MarketPulse Data Platform — STAGED Layer Tables
-- ============================================================
-- Cleaned, deduplicated, schema-normalized data.
-- Produced by Spark jobs from RAW layer.
-- ============================================================

USE DATABASE MARKETPULSE;
USE SCHEMA STAGED;

-- Deduplicated and normalized daily prices
CREATE TABLE IF NOT EXISTS stg_prices (
    asset_id        VARCHAR(100),
    symbol          VARCHAR(20),
    name            VARCHAR(200),
    date            DATE,
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          FLOAT,
    market_cap      FLOAT,
    _processed_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _batch_id       VARCHAR(100),

    -- Dedup: one row per asset per day
    CONSTRAINT uq_stg_prices UNIQUE (asset_id, date)
);

-- Cleaned sentiment scores
CREATE TABLE IF NOT EXISTS stg_sentiment (
    date            DATE,
    fear_greed_value INT,
    classification  VARCHAR(50),
    _processed_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _batch_id       VARCHAR(100),

    CONSTRAINT uq_stg_sentiment UNIQUE (date)
);
