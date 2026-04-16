"""
MarketPulse — Sentiment Ingestion DAG
Fetches daily Fear & Greed Index from Alternative.me
and loads into Snowflake RAW layer.

Schedule: Daily at 06:30 UTC (after prices DAG starts)
"""

from datetime import datetime, timedelta
import logging

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

logger = logging.getLogger(__name__)

FEAR_GREED_URL = "https://api.alternative.me/fng/"
SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    "owner": "marketpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def fetch_sentiment(**context):
    """
    Fetch Fear & Greed Index. Pulls last 30 days to handle backfill
    and ensure no gaps. Dedup handled downstream.
    """
    run_id = context["run_id"]

    try:
        response = requests.get(
            FEAR_GREED_URL,
            params={"limit": 30, "format": "json"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        records = []
        for entry in data.get("data", []):
            timestamp = int(entry["timestamp"])
            date_str = datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d")
            records.append({
                "date": date_str,
                "value": int(entry["value"]),
                "classification": entry["value_classification"],
                "batch_id": run_id,
            })

        context["ti"].xcom_push(key="sentiment_records", value=records)
        logger.info(f"Fetched {len(records)} sentiment records")

    except requests.RequestException as e:
        logger.error(f"Failed to fetch Fear & Greed Index: {e}")
        raise


def load_sentiment_to_snowflake(**context):
    """Load sentiment records to Snowflake RAW layer."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    records = context["ti"].xcom_pull(key="sentiment_records")
    if not records:
        logger.warning("No sentiment records to load")
        return

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    insert_sql = """
        INSERT INTO MARKETPULSE.RAW.raw_sentiment
            (date, value, classification, _batch_id)
        VALUES (%(date)s, %(value)s, %(classification)s, %(batch_id)s)
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        for record in records:
            cursor.execute(insert_sql, record)
        conn.commit()
        logger.info(f"Loaded {len(records)} sentiment records")
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="marketpulse_ingest_sentiment",
    default_args=default_args,
    description="Ingest daily Fear & Greed Index to Snowflake RAW",
    schedule_interval="30 6 * * *",  # Daily at 06:30 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketpulse", "ingestion", "sentiment"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_fear_greed_index",
        python_callable=fetch_sentiment,
    )

    load = PythonOperator(
        task_id="load_sentiment_to_snowflake",
        python_callable=load_sentiment_to_snowflake,
    )

    quality_check = SnowflakeOperator(
        task_id="quality_check_raw_sentiment",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1/0
                ELSE 1
            END
            FROM MARKETPULSE.RAW.raw_sentiment
            WHERE _ingested_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
              AND (date IS NULL OR value IS NULL);
        """,
    )

    fetch >> load >> quality_check
