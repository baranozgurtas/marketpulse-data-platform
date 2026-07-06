-- ============================================================
-- Staging: prices
-- ============================================================
-- Thin view over the Spark-produced STAGED.stg_prices table.
-- No business logic here - just an explicit column contract so
-- downstream marts never depend on source columns directly.
-- ============================================================

select
    asset_id,
    symbol,
    name,
    date,
    open,
    high,
    low,
    close,
    volume,
    market_cap,
    _batch_id
from {{ source('staged', 'stg_prices') }}
-- Guard against bad source records (e.g. deprecated assets reported
-- with zero prices by the API). Mirrors the Spark job's close > 0
-- filter for records loaded via the manual path (load_data.py).
where close > 0
