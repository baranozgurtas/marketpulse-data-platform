-- ============================================================
-- Singular test: fact_market_daily composite key uniqueness
-- ============================================================
-- The fact table grain is one row per (asset_id, date) - the same
-- constraint as uq_fact_market in sql/04_curated_tables.sql.
-- Written as a singular test to avoid a package dependency for
-- composite-key uniqueness. Test fails if any rows are returned.
-- ============================================================

select
    asset_id,
    date,
    count(*) as row_count
from {{ ref('fact_market_daily') }}
group by asset_id, date
having count(*) > 1
