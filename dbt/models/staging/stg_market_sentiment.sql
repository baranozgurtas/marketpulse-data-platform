-- ============================================================
-- Staging: sentiment
-- ============================================================
-- Thin view over the Spark-produced STAGED.stg_sentiment table.
-- ============================================================

select
    date,
    fear_greed_value,
    classification,
    _batch_id
from {{ source('staged', 'stg_sentiment') }}
