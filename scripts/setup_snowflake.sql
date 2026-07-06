-- ============================================================
-- MarketPulse Data Platform - Snowflake Environment Setup
-- ============================================================
-- Provisions EVERYTHING the pipeline and the dbt project need on a
-- fresh Snowflake account (e.g. a new 30-day trial):
--   warehouse COMPUTE_WH, database MARKETPULSE,
--   schemas RAW / STAGED / CURATED, and all tables.
--
-- All names match the existing repository configuration
-- (config/settings.example.py, dags/*, spark_jobs/*, dbt/).
--
-- Idempotent: every statement uses IF NOT EXISTS (or CREATE OR
-- REPLACE for procedures/views), so the script is safe to re-run.
--
-- Run in a Snowsight worksheet as ACCOUNTADMIN, or via SnowSQL:
--   snowsql -a <account> -u <user> -f scripts/setup_snowflake.sql
-- ============================================================

-- -----------------------------------------------
-- 0) Warehouse
-- -----------------------------------------------
-- Trial accounts usually ship with COMPUTE_WH already; this makes
-- the script self-sufficient either way. XSMALL + aggressive
-- auto-suspend keeps trial credit burn minimal.
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE COMPUTE_WH;

-- -----------------------------------------------
-- 1) Database + medallion schemas (from sql/01_schemas.sql)
-- -----------------------------------------------
CREATE DATABASE IF NOT EXISTS MARKETPULSE;

USE DATABASE MARKETPULSE;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGED;
CREATE SCHEMA IF NOT EXISTS CURATED;

-- -----------------------------------------------
-- 2) RAW layer (from sql/02_raw_tables.sql)
-- -----------------------------------------------
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS raw_prices (
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
    _ingested_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source         VARCHAR(50) DEFAULT 'coingecko',
    _batch_id       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS raw_sentiment (
    date            DATE,
    value           INT,
    classification  VARCHAR(50),
    _ingested_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source         VARCHAR(50) DEFAULT 'alternative_me',
    _batch_id       VARCHAR(100)
);

-- -----------------------------------------------
-- 3) STAGED layer (from sql/03_staged_tables.sql)
-- -----------------------------------------------
USE SCHEMA STAGED;

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
    CONSTRAINT uq_stg_prices UNIQUE (asset_id, date)
);

CREATE TABLE IF NOT EXISTS stg_sentiment (
    date             DATE,
    fear_greed_value INT,
    classification   VARCHAR(50),
    _processed_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _batch_id        VARCHAR(100),
    CONSTRAINT uq_stg_sentiment UNIQUE (date)
);

-- -----------------------------------------------
-- 4) CURATED layer (from sql/04_curated_tables.sql)
-- -----------------------------------------------
-- NOTE: dbt now owns this layer and will (re)create dim_asset,
-- fact_market_daily, and mart_volatility on `dbt run`. The DDL below
-- is kept so the legacy manual path (load_data.py, stored procedures)
-- still works on a fresh account before the first dbt run.
USE SCHEMA CURATED;

CREATE TABLE IF NOT EXISTS dim_asset (
    asset_id        VARCHAR(100) PRIMARY KEY,
    symbol          VARCHAR(20),
    name            VARCHAR(200),
    first_seen_date DATE,
    last_seen_date  DATE,
    _updated_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS fact_market_daily (
    asset_id        VARCHAR(100),
    date            DATE,
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          FLOAT,
    market_cap      FLOAT,
    return_1d       FLOAT,
    log_return_1d   FLOAT,
    high_low_range  FLOAT,
    fear_greed      INT,
    _updated_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_fact_market UNIQUE (asset_id, date)
);

-- Legacy stored procedures (superseded by dbt models, kept for the
-- manual path). CREATE OR REPLACE is idempotent.
CREATE OR REPLACE PROCEDURE sp_refresh_dim_asset()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.dim_asset tgt
    USING (
        SELECT
            asset_id,
            symbol,
            name,
            MIN(date) AS first_seen_date,
            MAX(date) AS last_seen_date
        FROM STAGED.stg_prices
        GROUP BY asset_id, symbol, name
    ) src
    ON tgt.asset_id = src.asset_id
    WHEN MATCHED THEN UPDATE SET
        tgt.symbol = src.symbol,
        tgt.name = src.name,
        tgt.last_seen_date = src.last_seen_date,
        tgt._updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (asset_id, symbol, name, first_seen_date, last_seen_date)
        VALUES (src.asset_id, src.symbol, src.name, src.first_seen_date, src.last_seen_date);
    RETURN 'dim_asset refreshed';
END;
$$;

CREATE OR REPLACE PROCEDURE sp_build_fact_market_daily()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO CURATED.fact_market_daily tgt
    USING (
        WITH prices_with_returns AS (
            SELECT
                p.asset_id,
                p.date,
                p.open, p.high, p.low, p.close,
                p.volume, p.market_cap,
                LAG(p.close) OVER (PARTITION BY p.asset_id ORDER BY p.date) AS prev_close,
                s.fear_greed_value AS fear_greed
            FROM STAGED.stg_prices p
            LEFT JOIN STAGED.stg_sentiment s ON p.date = s.date
        )
        SELECT
            asset_id, date, open, high, low, close, volume, market_cap,
            CASE WHEN prev_close > 0 THEN (close - prev_close) / prev_close ELSE NULL END AS return_1d,
            CASE WHEN prev_close > 0 THEN LN(close / prev_close) ELSE NULL END AS log_return_1d,
            CASE WHEN close > 0 THEN (high - low) / close ELSE NULL END AS high_low_range,
            fear_greed
        FROM prices_with_returns
    ) src
    ON tgt.asset_id = src.asset_id AND tgt.date = src.date
    WHEN MATCHED THEN UPDATE SET
        tgt.open = src.open, tgt.high = src.high, tgt.low = src.low, tgt.close = src.close,
        tgt.volume = src.volume, tgt.market_cap = src.market_cap,
        tgt.return_1d = src.return_1d, tgt.log_return_1d = src.log_return_1d,
        tgt.high_low_range = src.high_low_range, tgt.fear_greed = src.fear_greed,
        tgt._updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (asset_id, date, open, high, low, close, volume, market_cap,
         return_1d, log_return_1d, high_low_range, fear_greed)
        VALUES (src.asset_id, src.date, src.open, src.high, src.low, src.close,
                src.volume, src.market_cap, src.return_1d, src.log_return_1d,
                src.high_low_range, src.fear_greed);
    RETURN 'fact_market_daily built';
END;
$$;

-- Legacy view definition (dbt recreates the same view on `dbt run`).
CREATE OR REPLACE VIEW mart_volatility AS
WITH rolling AS (
    SELECT
        f.asset_id,
        a.symbol,
        a.name,
        f.date,
        f.close,
        f.return_1d,
        f.fear_greed,
        STDDEV(f.log_return_1d) OVER (
            PARTITION BY f.asset_id ORDER BY f.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volatility_7d,
        STDDEV(f.log_return_1d) OVER (
            PARTITION BY f.asset_id ORDER BY f.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        (f.close - MAX(f.close) OVER (
            PARTITION BY f.asset_id ORDER BY f.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )) / NULLIF(MAX(f.close) OVER (
            PARTITION BY f.asset_id ORDER BY f.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ), 0) AS max_drawdown_30d
    FROM CURATED.fact_market_daily f
    JOIN CURATED.dim_asset a ON f.asset_id = a.asset_id
)
SELECT
    *,
    CASE
        WHEN volatility_30d IS NULL THEN NULL
        WHEN volatility_30d < 0.02 THEN 'LOW'
        WHEN volatility_30d < 0.05 THEN 'NORMAL'
        WHEN volatility_30d < 0.08 THEN 'HIGH'
        ELSE 'EXTREME'
    END AS volatility_regime
FROM rolling;

-- -----------------------------------------------
-- 5) Verification
-- -----------------------------------------------
SHOW SCHEMAS IN DATABASE MARKETPULSE;
SHOW TABLES IN SCHEMA MARKETPULSE.RAW;
SHOW TABLES IN SCHEMA MARKETPULSE.STAGED;
SHOW TABLES IN SCHEMA MARKETPULSE.CURATED;

SELECT 'Setup complete' AS status;
