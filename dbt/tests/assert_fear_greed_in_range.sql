-- ============================================================
-- Singular test: Fear & Greed Index within its defined 0-100 range
-- ============================================================
-- Alternative.me defines the index on a 0-100 scale; anything
-- outside that range is a corrupted or misparsed payload.
-- Test fails if any rows are returned.
-- ============================================================

select
    date,
    fear_greed_value
from {{ ref('stg_market_sentiment') }}
where fear_greed_value < 0
   or fear_greed_value > 100
