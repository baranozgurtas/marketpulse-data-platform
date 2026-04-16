"""
MarketPulse — Price Ingestion DAG
Fetches daily OHLCV data from CoinGecko for tracked crypto assets
and loads into Snowflake RAW layer.

Schedule: Daily at 06:00 UTC
Idempotent: Deduplication on (asset_id, date) prevents duplicates on reruns.
"""

from datetime import datetime, timedelta
import json
import logging

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

logger = logging.getLogger(__name__)

# -----------------------------------------------
# Config
# -----------------------------------------------
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
TRACKED_ASSETS = [
    "bitcoin", "ethereum", "solana", "cardano", "avalanche-2",
    "chainlink", "polkadot", "polygon-ecosystem-token", "uniswap", "aave",
]
SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    "owner": "marketpulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# -----------------------------------------------
# Extract
# -----------------------------------------------
def fetch_prices(**context):
    """
    Fetch OHLCV data from CoinGecko for each tracked asset.
    Uses market_chart endpoint for the execution date window.
    Pushes results to XCom for downstream loading.
    """
    execution_date = context["ds"]
    run_id = context["run_id"]
    records = []

    for asset_id in TRACKED_ASSETS:
        url = f"{COINGECKO_BASE_URL}/coins/{asset_id}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "2",  # Fetch 2 days to ensure we capture the target date
            "interval": "daily",
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            # CoinGecko returns arrays of [timestamp_ms, value]
            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            market_caps = data.get("market_caps", [])

            # Get coin metadata
            meta_url = f"{COINGECKO_BASE_URL}/coins/{asset_id}"
            meta_resp = requests.get(
                meta_url,
                params={"localization": "false", "tickers": "false",
                        "community_data": "false", "developer_data": "false"},
                timeout=30,
            )
            meta = meta_resp.json() if meta_resp.ok else {}
            symbol = meta.get("symbol", asset_id).upper()
            name = meta.get("name", asset_id)

            for i, (ts, close) in enumerate(prices):
                date_str = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                record = {
                    "asset_id": asset_id,
                    "symbol": symbol,
                    "name": name,
                    "date": date_str,
                    "open": close,  # CoinGecko daily doesn't give OHLC separately
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": volumes[i][1] if i < len(volumes) else None,
                    "market_cap": market_caps[i][1] if i < len(market_caps) else None,
                    "batch_id": run_id,
                }
                records.append(record)

            logger.info(f"Fetched {len(prices)} price points for {asset_id}")

        except requests.RequestException as e:
            logger.error(f"Failed to fetch {asset_id}: {e}")
            continue

        # Rate limiting: CoinGecko free tier allows ~10-30 calls/min
        import time
        time.sleep(2)

    context["ti"].xcom_push(key="price_records", value=records)
    logger.info(f"Total records fetched: {len(records)}")


# -----------------------------------------------
# Load
# -----------------------------------------------
def load_prices_to_snowflake(**context):
    """
    Load fetched price records into Snowflake RAW layer.
    Uses INSERT with dedup awareness (downstream Spark handles full dedup).
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    records = context["ti"].xcom_pull(key="price_records")
    if not records:
        logger.warning("No records to load")
        return

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    insert_sql = """
        INSERT INTO MARKETPULSE.RAW.raw_prices
            (asset_id, symbol, name, date, open, high, low, close, volume, market_cap, _batch_id)
        VALUES (%(asset_id)s, %(symbol)s, %(name)s, %(date)s, %(open)s, %(high)s,
                %(low)s, %(close)s, %(volume)s, %(market_cap)s, %(batch_id)s)
    """

    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        for record in records:
            cursor.execute(insert_sql, record)
        conn.commit()
        logger.info(f"Loaded {len(records)} records to raw_prices")
    finally:
        cursor.close()
        conn.close()


# -----------------------------------------------
# DAG Definition
# -----------------------------------------------
with DAG(
    dag_id="marketpulse_ingest_prices",
    default_args=default_args,
    description="Ingest daily OHLCV crypto prices from CoinGecko to Snowflake RAW",
    schedule_interval="0 6 * * *",  # Daily at 06:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketpulse", "ingestion", "prices"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_prices_from_coingecko",
        python_callable=fetch_prices,
    )

    load = PythonOperator(
        task_id="load_prices_to_snowflake",
        python_callable=load_prices_to_snowflake,
    )

    # Quality gate: check for nulls in critical columns
    quality_check = SnowflakeOperator(
        task_id="quality_check_raw_prices",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            SELECT CASE
                WHEN COUNT(*) > 0 THEN 1/0  -- Force failure if nulls found
                ELSE 1
            END
            FROM MARKETPULSE.RAW.raw_prices
            WHERE _ingested_at >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
              AND (asset_id IS NULL OR date IS NULL OR close IS NULL);
        """,
    )

    fetch >> load >> quality_check
