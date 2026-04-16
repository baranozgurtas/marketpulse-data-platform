-- ============================================================
-- MarketPulse Data Platform — CURATED Layer
-- ============================================================
-- Business-ready dimensional model + analytics marts.
-- ============================================================

USE DATABASE MARKETPULSE;
USE SCHEMA CURATED;

-- -----------------------------------------------
-- DIMENSION: Asset metadata
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS dim_asset (
    asset_id        VARCHAR(100) PRIMARY KEY,
    symbol          VARCHAR(20),
    name            VARCHAR(200),
    first_seen_date DATE,
    last_seen_date  DATE,
    _updated_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Refresh dim_asset from staged prices
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

-- -----------------------------------------------
-- FACT: Daily market data
-- -----------------------------------------------
CREATE TABLE IF NOT EXISTS fact_market_daily (
    asset_id        VARCHAR(100),
    date            DATE,
    open            FLOAT,
    high            FLOAT,
    low             FLOAT,
    close           FLOAT,
    volume          FLOAT,
    market_cap      FLOAT,
    -- Derived
    return_1d       FLOAT,          -- (close - prev_close) / prev_close
    log_return_1d   FLOAT,          -- LN(close / prev_close)
    high_low_range  FLOAT,          -- (high - low) / close
    fear_greed      INT,            -- Joined from sentiment
    _updated_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT uq_fact_market UNIQUE (asset_id, date)
);

-- Build fact_market_daily from staged data
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

-- -----------------------------------------------
-- MART: Volatility analytics
-- -----------------------------------------------
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
        -- Rolling volatility (std of log returns)
        STDDEV(f.log_return_1d) OVER (
            PARTITION BY f.asset_id ORDER BY f.date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volatility_7d,
        STDDEV(f.log_return_1d) OVER (
            PARTITION BY f.asset_id ORDER BY f.date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        -- Max drawdown over 30 days
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
