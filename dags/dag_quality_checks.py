"""
MarketPulse — Data Quality Checks DAG
Runs comprehensive quality validation across all warehouse layers.

Schedule: Daily at 08:00 UTC (after ingestion + processing complete)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    "owner": "marketpulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketpulse_quality_checks",
    default_args=default_args,
    description="Run data quality checks across all warehouse layers",
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketpulse", "quality"],
) as dag:

    check_raw_nulls = SnowflakeOperator(
        task_id="check_raw_prices_nulls",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1/0
                ELSE 1
            END
            FROM MARKETPULSE.RAW.raw_prices
            WHERE _ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
              AND (asset_id IS NULL OR date IS NULL OR close IS NULL);
        """,
    )

    check_staged_duplicates = SnowflakeOperator(
        task_id="check_staged_duplicates",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1/0
                ELSE 1
            END
            FROM (
                SELECT asset_id, date, COUNT(*) AS cnt
                FROM MARKETPULSE.STAGED.stg_prices
                GROUP BY asset_id, date
                HAVING cnt > 1
            );
        """,
    )

    check_freshness = SnowflakeOperator(
        task_id="check_data_freshness",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT CASE
                WHEN MAX(_ingested_at) < DATEADD(hour, -48, CURRENT_TIMESTAMP())
                THEN 1/0
                ELSE 1
            END
            FROM MARKETPULSE.RAW.raw_prices;
        """,
    )

    check_sentiment_coverage = SnowflakeOperator(
        task_id="check_sentiment_coverage",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 3 THEN 1/0  -- Allow max 3 missing days in last week
                ELSE 1
            END
            FROM (
                SELECT DATEADD(day, -seq4(), CURRENT_DATE()) AS expected_date
                FROM TABLE(GENERATOR(ROWCOUNT => 7))
            ) dates
            LEFT JOIN MARKETPULSE.RAW.raw_sentiment s
                ON dates.expected_date = s.date
            WHERE s.date IS NULL;
        """,
    )

    # All checks run in parallel — they're independent
    [check_raw_nulls, check_staged_duplicates, check_freshness, check_sentiment_coverage]
