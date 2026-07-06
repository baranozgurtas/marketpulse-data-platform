-- ============================================================
-- Singular test: prices, volume, and market cap are non-negative
-- ============================================================
-- Crypto OHLCV values can never be negative. The Spark job already
-- filters close <= 0, so this guards the remaining numeric columns
-- against bad API payloads slipping through.
-- Test fails if any rows are returned.
-- ============================================================

select
    asset_id,
    date,
    open,
    high,
    low,
    close,
    volume,
    market_cap
from {{ ref('fact_market_daily') }}
where open < 0
   or high < 0
   or low < 0
   or close <= 0
   or volume < 0
   or market_cap < 0
