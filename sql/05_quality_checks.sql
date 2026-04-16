-- ============================================================
-- MarketPulse Data Platform — Data Quality Checks
-- ============================================================
-- These queries are called by Airflow quality check tasks.
-- Each returns a pass/fail result.
-- ============================================================

USE DATABASE MARKETPULSE;

-- -----------------------------------------------
-- CHECK 1: Null check on RAW prices
-- Fails if any critical column is null in today's batch
-- -----------------------------------------------
SELECT
    COUNT(*) AS null_count
FROM RAW.raw_prices
WHERE _ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
  AND (asset_id IS NULL OR date IS NULL OR close IS NULL);
-- Expected: 0

-- -----------------------------------------------
-- CHECK 2: Duplicate check on STAGED prices
-- Fails if dedup didn't work properly
-- -----------------------------------------------
SELECT
    asset_id, date, COUNT(*) AS cnt
FROM STAGED.stg_prices
GROUP BY asset_id, date
HAVING cnt > 1;
-- Expected: 0 rows

-- -----------------------------------------------
-- CHECK 3: Freshness check
-- Fails if latest raw data is older than 48 hours
-- -----------------------------------------------
SELECT
    CASE
        WHEN MAX(_ingested_at) < DATEADD(hour, -48, CURRENT_TIMESTAMP())
        THEN 'STALE'
        ELSE 'FRESH'
    END AS freshness_status
FROM RAW.raw_prices;
-- Expected: 'FRESH'

-- -----------------------------------------------
-- CHECK 4: Volume anomaly detection
-- Flags if today's row count deviates >50% from 7-day avg
-- -----------------------------------------------
WITH daily_counts AS (
    SELECT
        DATE(_ingested_at) AS ingest_date,
        COUNT(*) AS row_count
    FROM RAW.raw_prices
    WHERE _ingested_at >= DATEADD(day, -8, CURRENT_TIMESTAMP())
    GROUP BY DATE(_ingested_at)
),
stats AS (
    SELECT
        AVG(row_count) AS avg_count,
        MAX(CASE WHEN ingest_date = CURRENT_DATE() THEN row_count END) AS today_count
    FROM daily_counts
)
SELECT
    today_count,
    avg_count,
    CASE
        WHEN today_count < avg_count * 0.5 THEN 'LOW_VOLUME'
        WHEN today_count > avg_count * 1.5 THEN 'HIGH_VOLUME'
        ELSE 'NORMAL'
    END AS volume_status
FROM stats;
-- Expected: 'NORMAL'

-- -----------------------------------------------
-- CHECK 5: Sentiment data completeness
-- Ensures we have sentiment for recent dates
-- -----------------------------------------------
SELECT
    COUNT(*) AS missing_days
FROM (
    SELECT DATEADD(day, -seq4(), CURRENT_DATE()) AS expected_date
    FROM TABLE(GENERATOR(ROWCOUNT => 7))
) dates
LEFT JOIN RAW.raw_sentiment s ON dates.expected_date = s.date
WHERE s.date IS NULL;
-- Expected: 0 or 1 (today might not be available yet)
