-- ============================================================
-- Singular test: no future-dated market records
-- ============================================================
-- Price and sentiment records come from historical daily APIs, so a
-- date after today indicates a timestamp parsing bug in ingestion.
-- Test fails if any rows are returned.
-- ============================================================

select 'fact_market_daily' as source_model, asset_id, date
from {{ ref('fact_market_daily') }}
where date > current_date()

union all

select 'stg_market_sentiment' as source_model, null as asset_id, date
from {{ ref('stg_market_sentiment') }}
where date > current_date()
