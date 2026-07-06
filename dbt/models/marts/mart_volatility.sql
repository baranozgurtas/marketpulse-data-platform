-- ============================================================
-- CURATED.mart_volatility
-- ============================================================
-- Volatility analytics mart. Migrated 1:1 from the
-- mart_volatility view in sql/04_curated_tables.sql.
-- Kept as a view (same as before) so it always reflects the
-- latest fact_market_daily without an extra build step.
-- Consumed by the Streamlit dashboard.
-- ============================================================

{{ config(materialized='view') }}

with rolling as (

    select
        f.asset_id,
        a.symbol,
        a.name,
        f.date,
        f.close,
        f.return_1d,
        f.fear_greed,
        -- Rolling volatility (std of log returns)
        stddev(f.log_return_1d) over (
            partition by f.asset_id order by f.date
            rows between 6 preceding and current row
        ) as volatility_7d,
        stddev(f.log_return_1d) over (
            partition by f.asset_id order by f.date
            rows between 29 preceding and current row
        ) as volatility_30d,
        -- Max drawdown over 30 days
        (f.close - max(f.close) over (
            partition by f.asset_id order by f.date
            rows between 29 preceding and current row
        )) / nullif(max(f.close) over (
            partition by f.asset_id order by f.date
            rows between 29 preceding and current row
        ), 0) as max_drawdown_30d
    from {{ ref('fact_market_daily') }} f
    join {{ ref('dim_asset') }} a
        on f.asset_id = a.asset_id

)

select
    *,
    case
        when volatility_30d is null then null
        when volatility_30d < 0.02 then 'LOW'
        when volatility_30d < 0.05 then 'NORMAL'
        when volatility_30d < 0.08 then 'HIGH'
        else 'EXTREME'
    end as volatility_regime
from rolling
