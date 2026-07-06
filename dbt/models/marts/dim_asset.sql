-- ============================================================
-- CURATED.dim_asset
-- ============================================================
-- Asset dimension. Migrated from sp_refresh_dim_asset() in
-- sql/04_curated_tables.sql. The MERGE upsert is replaced by a
-- full table rebuild: the source is small (one row per tracked
-- asset) so a full refresh is simpler and equally correct.
-- ============================================================

select
    asset_id,
    symbol,
    name,
    min(date) as first_seen_date,
    max(date) as last_seen_date,
    current_timestamp() as _updated_at
from {{ ref('stg_market_prices') }}
group by asset_id, symbol, name
