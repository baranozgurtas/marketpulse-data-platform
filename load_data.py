"""
MarketPulse — Data Loader
Fetches crypto price data from CoinGecko and sentiment from Alternative.me,
then loads into Snowflake RAW → processes to STAGED → builds CURATED.

Usage:
    python load_data.py

Requires: pip install snowflake-connector-python requests
"""

import time
import json
from datetime import datetime, timedelta

import requests
import snowflake.connector

# ============================================================
# CONFIG — Update these with your Snowflake credentials
# ============================================================
SNOWFLAKE_ACCOUNT = "ch12697.eu-central-2.aws"
SNOWFLAKE_USER = "YOUR_USERNAME_HERE" # Replace with your username
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD_HERE"  # Replace with your password
SNOWFLAKE_DATABASE = "MARKETPULSE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

# Assets to track
TRACKED_ASSETS = [
    "bitcoin", "ethereum", "solana", "cardano", "avalanche-2",
    "chainlink", "polkadot", "matic-network", "uniswap", "aave",
]

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL = "https://api.alternative.me/fng/"
BATCH_ID = f"manual_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


def get_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )


def load_raw_prices(conn):
    print("\n=== STEP 1: Loading RAW Prices ===")
    cursor = conn.cursor()
    cursor.execute("USE SCHEMA RAW")
    total_records = 0

    for asset_id in TRACKED_ASSETS:
        print(f"  Fetching {asset_id}...")
        url = f"{COINGECKO_BASE}/coins/{asset_id}/market_chart"
        params = {"vs_currency": "usd", "days": "365", "interval": "daily"}

        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    ERROR fetching {asset_id}: {e}")
            continue

        prices = data.get("prices", [])
        volumes = data.get("total_volumes", [])
        market_caps = data.get("market_caps", [])

        try:
            meta_resp = requests.get(
                f"{COINGECKO_BASE}/coins/{asset_id}",
                params={"localization": "false", "tickers": "false",
                        "community_data": "false", "developer_data": "false"},
                timeout=30,
            )
            meta = meta_resp.json() if meta_resp.ok else {}
        except Exception:
            meta = {}

        symbol = meta.get("symbol", asset_id[:5]).upper()
        name = meta.get("name", asset_id.title())

        for i, (ts, close) in enumerate(prices):
            date_str = datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%d")
            vol = volumes[i][1] if i < len(volumes) else None
            mcap = market_caps[i][1] if i < len(market_caps) else None
            cursor.execute(
                """INSERT INTO raw_prices (asset_id, symbol, name, date, open, high, low, close, volume, market_cap, _batch_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (asset_id, symbol, name, date_str, close, close, close, close, vol, mcap, BATCH_ID)
            )
            total_records += 1

        print(f"    Loaded {len(prices)} records for {symbol}")
        time.sleep(6)

    conn.commit()
    print(f"  Total RAW price records: {total_records}")


def load_raw_sentiment(conn):
    print("\n=== STEP 2: Loading RAW Sentiment ===")
    cursor = conn.cursor()
    cursor.execute("USE SCHEMA RAW")

    try:
        resp = requests.get(FEAR_GREED_URL, params={"limit": 365}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ERROR: {e}")
        return

    records = data.get("data", [])
    for entry in records:
        ts = int(entry["timestamp"])
        date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        cursor.execute(
            """INSERT INTO raw_sentiment (date, value, classification, _batch_id)
               VALUES (%s, %s, %s, %s)""",
            (date_str, int(entry["value"]), entry["value_classification"], BATCH_ID)
        )

    conn.commit()
    print(f"  Loaded {len(records)} sentiment records")


def process_to_staged(conn):
    print("\n=== STEP 3: Processing RAW → STAGED ===")
    cursor = conn.cursor()
    cursor.execute("USE SCHEMA STAGED")

    cursor.execute("TRUNCATE TABLE IF EXISTS stg_prices")
    cursor.execute("""
        INSERT INTO stg_prices (asset_id, symbol, name, date, open, high, low, close, volume, market_cap, _batch_id)
        SELECT asset_id, symbol, name, date, open, high, low, close, volume, market_cap, _batch_id
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY asset_id, date ORDER BY _ingested_at DESC) AS rn
            FROM MARKETPULSE.RAW.raw_prices
        )
        WHERE rn = 1
    """)
    result = cursor.execute("SELECT COUNT(*) FROM stg_prices").fetchone()
    print(f"  stg_prices: {result[0]} records")

    cursor.execute("TRUNCATE TABLE IF EXISTS stg_sentiment")
    cursor.execute("""
        INSERT INTO stg_sentiment (date, fear_greed_value, classification, _batch_id)
        SELECT date, value, classification, _batch_id
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY date ORDER BY _ingested_at DESC) AS rn
            FROM MARKETPULSE.RAW.raw_sentiment
        )
        WHERE rn = 1
    """)
    result = cursor.execute("SELECT COUNT(*) FROM stg_sentiment").fetchone()
    print(f"  stg_sentiment: {result[0]} records")

    conn.commit()


def build_curated(conn):
    print("\n=== STEP 4: Building CURATED Layer ===")
    cursor = conn.cursor()
    cursor.execute("USE SCHEMA CURATED")

    cursor.execute("TRUNCATE TABLE IF EXISTS dim_asset")
    cursor.execute("""
        INSERT INTO dim_asset (asset_id, symbol, name, first_seen_date, last_seen_date)
        SELECT asset_id, symbol, name, MIN(date), MAX(date)
        FROM MARKETPULSE.STAGED.stg_prices
        GROUP BY asset_id, symbol, name
    """)
    result = cursor.execute("SELECT COUNT(*) FROM dim_asset").fetchone()
    print(f"  dim_asset: {result[0]} assets")

    cursor.execute("TRUNCATE TABLE IF EXISTS fact_market_daily")
    cursor.execute("""
        INSERT INTO fact_market_daily (
            asset_id, date, open, high, low, close, volume, market_cap,
            return_1d, log_return_1d, high_low_range, fear_greed,
            volatility_7d, volatility_30d, max_drawdown_30d, volatility_regime
        )
        WITH prices_with_returns AS (
            SELECT
                p.asset_id, p.date, p.open, p.high, p.low, p.close, p.volume, p.market_cap,
                LAG(p.close) OVER (PARTITION BY p.asset_id ORDER BY p.date) AS prev_close,
                s.fear_greed_value AS fear_greed
            FROM MARKETPULSE.STAGED.stg_prices p
            LEFT JOIN MARKETPULSE.STAGED.stg_sentiment s ON p.date = s.date
        ),
        with_returns AS (
            SELECT *,
                CASE WHEN prev_close > 0 THEN (close - prev_close) / prev_close END AS return_1d,
                CASE WHEN prev_close > 0 AND close > 0 THEN LN(close / prev_close) END AS log_return_1d,
                CASE WHEN close > 0 THEN (high - low) / close END AS high_low_range
            FROM prices_with_returns
        ),
        with_volatility AS (
            SELECT *,
                STDDEV(log_return_1d) OVER (
                    PARTITION BY asset_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS volatility_7d,
                STDDEV(log_return_1d) OVER (
                    PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS volatility_30d,
                (close - MAX(close) OVER (
                    PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )) / NULLIF(MAX(close) OVER (
                    PARTITION BY asset_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 0) AS max_drawdown_30d
            FROM with_returns
        )
        SELECT
            asset_id, date, open, high, low, close, volume, market_cap,
            return_1d, log_return_1d, high_low_range, fear_greed,
            volatility_7d, volatility_30d, max_drawdown_30d,
            CASE
                WHEN volatility_30d IS NULL THEN NULL
                WHEN volatility_30d < 0.02 THEN 'LOW'
                WHEN volatility_30d < 0.05 THEN 'NORMAL'
                WHEN volatility_30d < 0.08 THEN 'HIGH'
                ELSE 'EXTREME'
            END AS volatility_regime
        FROM with_volatility
    """)

    result = cursor.execute("SELECT COUNT(*) FROM fact_market_daily").fetchone()
    print(f"  fact_market_daily: {result[0]} records")

    result = cursor.execute("SELECT COUNT(*) FROM mart_volatility").fetchone()
    print(f"  mart_volatility (view): {result[0]} records")

    conn.commit()


def verify(conn):
    print("\n=== STEP 5: Verification ===")
    cursor = conn.cursor()

    checks = [
        ("RAW.raw_prices", "SELECT COUNT(*) FROM MARKETPULSE.RAW.raw_prices"),
        ("RAW.raw_sentiment", "SELECT COUNT(*) FROM MARKETPULSE.RAW.raw_sentiment"),
        ("STAGED.stg_prices", "SELECT COUNT(*) FROM MARKETPULSE.STAGED.stg_prices"),
        ("STAGED.stg_sentiment", "SELECT COUNT(*) FROM MARKETPULSE.STAGED.stg_sentiment"),
        ("CURATED.dim_asset", "SELECT COUNT(*) FROM MARKETPULSE.CURATED.dim_asset"),
        ("CURATED.fact_market_daily", "SELECT COUNT(*) FROM MARKETPULSE.CURATED.fact_market_daily"),
        ("CURATED.mart_volatility", "SELECT COUNT(*) FROM MARKETPULSE.CURATED.mart_volatility"),
    ]

    for name, sql in checks:
        result = cursor.execute(sql).fetchone()
        status = "OK" if result[0] > 0 else "EMPTY"
        print(f"  {status} | {name}: {result[0]} rows")

    print("\n  Sample from mart_volatility:")
    cursor.execute("""
        SELECT asset_id, symbol, date, close, return_1d, volatility_30d, volatility_regime
        FROM MARKETPULSE.CURATED.mart_volatility
        WHERE volatility_regime IS NOT NULL
        ORDER BY date DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"    {row}")


if __name__ == "__main__":
    print("=" * 60)
    print("MarketPulse Data Platform — Data Loader")
    print(f"Batch ID: {BATCH_ID}")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    conn = get_connection()

    try:
        load_raw_prices(conn)
        load_raw_sentiment(conn)
        process_to_staged(conn)
        build_curated(conn)
        verify(conn)
        print("\n" + "=" * 60)
        print("Pipeline complete!")
        print("=" * 60)
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
    finally:
        conn.close()
