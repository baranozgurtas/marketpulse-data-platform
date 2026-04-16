-- ============================================================
-- MarketPulse Data Platform — RAW Layer Tables
-- ============================================================
-- Append-only tables. Minimal transformation from source APIs.
-- Every record gets _ingested_at and _source metadata.
-- ============================================================

USE DATABASE MARKETPULSE;
USE SCHEMA RAW;

-- Daily OHLCV prices from CoinGecko
CREATE TABLE IF NOT EXISTS raw_prices (
    asset_id        VARCHAR(100),       -- CoinGecko asset ID (e.g., 'bitcoin')
    symbol          VARCHAR(20),        -- e.g., 'BTC'
    name            VARCHAR(200),       -- e.g., 'Bitcoin'
    date            DATE,               -- Price date
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          FLOAT,
    market_cap      FLOAT,
    _ingested_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source         VARCHAR(50) DEFAULT 'coingecko',
    _batch_id       VARCHAR(100)        -- Airflow run_id for lineage
);

-- Daily Fear & Greed Index from Alternative.me
CREATE TABLE IF NOT EXISTS raw_sentiment (
    date            DATE,
    value           INT,                -- 0-100 score
    classification  VARCHAR(50),        -- e.g., 'Fear', 'Greed', 'Extreme Fear'
    _ingested_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source         VARCHAR(50) DEFAULT 'alternative_me',
    _batch_id       VARCHAR(100)
);
