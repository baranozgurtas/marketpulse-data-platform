-- ============================================================
-- CURATED.fact_market_daily
-- ============================================================
-- Daily market fact table. Migrated from sp_build_fact_market_daily()
-- in sql/04_curated_tables.sql. The MERGE upsert is replaced by a
-- full table rebuild; at current volumes (~10 assets x 365 days)
-- this is cheap and keeps the model idempotent and easy to reason
-- about. Switch to an incremental model if volume grows.
-- ============================================================

with prices_with_returns as (

    select
        p.asset_id,
        p.date,
        p.open,
        p.high,
        p.low,
        p.close,
        p.volume,
        p.market_cap,
        lag(p.close) over (
            partition by p.asset_id order by p.date
        ) as prev_close,
        s.fear_greed_value as fear_greed
    from {{ ref('stg_market_prices') }} p
    left join {{ ref('stg_market_sentiment') }} s
        on p.date = s.date

)

select
    asset_id,
    date,
    open,
    high,
    low,
    close,
    volume,
    market_cap,
    case
        when prev_close > 0 then (close - prev_close) / prev_close
    end as return_1d,
    case
        when prev_close > 0 and close > 0 then ln(close / prev_close)
    end as log_return_1d,
    case
        when close > 0 then (high - low) / close
    end as high_low_range,
    fear_greed,
    current_timestamp() as _updated_at
from prices_with_returns
