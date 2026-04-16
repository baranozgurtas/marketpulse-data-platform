"""
MarketPulse — Local Spark Runner
Exports data from Snowflake RAW, processes with PySpark locally,
and writes results back to Snowflake STAGED.

This demonstrates the Spark processing layer without needing
the Snowflake Spark connector (which requires JVM/JDBC setup).

Usage:
    spark-submit spark_jobs/run_local.py
    # or
    python spark_jobs/run_local.py
"""

import os
import csv
import tempfile
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, DateType, IntegerType
)

import snowflake.connector

# ============================================================
# CONFIG
# ============================================================
SNOWFLAKE_ACCOUNT = "ch12697.eu-central-2.aws"
SNOWFLAKE_USER = "YOUR_USERNAME" # Replace with your username
SNOWFLAKE_PASSWORD = "YOUR_PASSWORD"  # Replace with your password
SNOWFLAKE_DATABASE = "MARKETPULSE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"


def get_sf_connection():
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )


# ============================================================
# STEP 1: Export RAW data from Snowflake to local CSV
# ============================================================
def export_raw_data(tmp_dir):
    print("\n=== STEP 1: Exporting RAW data from Snowflake ===")
    conn = get_sf_connection()
    cursor = conn.cursor()

    # Export raw_prices
    cursor.execute("SELECT * FROM MARKETPULSE.RAW.raw_prices")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    prices_path = os.path.join(tmp_dir, "raw_prices.csv")
    with open(prices_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"  Exported {len(rows)} raw_prices records to CSV")

    # Export raw_sentiment
    cursor.execute("SELECT * FROM MARKETPULSE.RAW.raw_sentiment")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    sentiment_path = os.path.join(tmp_dir, "raw_sentiment.csv")
    with open(sentiment_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"  Exported {len(rows)} raw_sentiment records to CSV")

    conn.close()
    return prices_path, sentiment_path


# ============================================================
# STEP 2: Process with PySpark
# ============================================================
def process_with_spark(prices_path, sentiment_path, tmp_dir):
    print("\n=== STEP 2: Processing with PySpark ===")

    spark = (
        SparkSession.builder
        .appName("MarketPulse-LocalProcessing")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # --- Read raw prices ---
    raw_prices = spark.read.csv(prices_path, header=True, inferSchema=True)
    print(f"  Raw prices loaded: {raw_prices.count()} rows")

    # --- Read raw sentiment ---
    raw_sentiment = spark.read.csv(sentiment_path, header=True, inferSchema=True)
    print(f"  Raw sentiment loaded: {raw_sentiment.count()} rows")

    # -----------------------------------------------
    # TRANSFORM 1: Normalize + Clean prices
    # -----------------------------------------------
    print("  Normalizing schemas...")
    cleaned_prices = (
        raw_prices
        .withColumn("asset_id", F.trim(F.lower(F.col("ASSET_ID"))))
        .withColumn("symbol", F.trim(F.upper(F.col("SYMBOL"))))
        .withColumn("name", F.trim(F.col("NAME")))
        .withColumn("date", F.to_date(F.col("DATE")))
        .withColumn("open", F.col("OPEN").cast("double"))
        .withColumn("high", F.col("HIGH").cast("double"))
        .withColumn("low", F.col("LOW").cast("double"))
        .withColumn("close", F.col("CLOSE").cast("double"))
        .withColumn("volume", F.col("VOLUME").cast("double"))
        .withColumn("market_cap", F.col("MARKET_CAP").cast("double"))
        .withColumn("_batch_id", F.col("_BATCH_ID"))
        .withColumn("_ingested_at", F.col("_INGESTED_AT"))
        .filter(
            F.col("asset_id").isNotNull()
            & F.col("date").isNotNull()
            & F.col("close").isNotNull()
            & (F.col("close") > 0)
        )
    )

    # -----------------------------------------------
    # TRANSFORM 2: Deduplicate
    # -----------------------------------------------
    print("  Deduplicating (keeping latest per asset_id + date)...")
    window = Window.partitionBy("asset_id", "date").orderBy(F.col("_ingested_at").desc())
    deduped_prices = (
        cleaned_prices
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    print(f"  After dedup: {deduped_prices.count()} rows (was {cleaned_prices.count()})")

    # -----------------------------------------------
    # TRANSFORM 3: Deduplicate sentiment
    # -----------------------------------------------
    print("  Deduplicating sentiment...")
    cleaned_sentiment = (
        raw_sentiment
        .withColumn("date", F.to_date(F.col("DATE")))
        .withColumn("fear_greed_value", F.col("VALUE").cast("int"))
        .withColumn("classification", F.trim(F.col("CLASSIFICATION")))
        .withColumn("_ingested_at", F.col("_INGESTED_AT"))
        .filter(
            F.col("date").isNotNull()
            & F.col("fear_greed_value").isNotNull()
            & (F.col("fear_greed_value").between(0, 100))
        )
    )

    sent_window = Window.partitionBy("date").orderBy(F.col("_ingested_at").desc())
    deduped_sentiment = (
        cleaned_sentiment
        .withColumn("_row_num", F.row_number().over(sent_window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )
    print(f"  Sentiment after dedup: {deduped_sentiment.count()} rows")

    # -----------------------------------------------
    # TRANSFORM 4: Compute features
    # -----------------------------------------------
    print("  Computing features (returns, volatility, drawdown)...")
    asset_window = Window.partitionBy("asset_id").orderBy("date")

    features = (
        deduped_prices
        .withColumn("prev_close", F.lag("close").over(asset_window))
        .withColumn(
            "return_1d",
            F.when(F.col("prev_close") > 0,
                   (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
        )
        .withColumn(
            "log_return_1d",
            F.when((F.col("prev_close") > 0) & (F.col("close") > 0),
                   F.log(F.col("close") / F.col("prev_close")))
        )
        .withColumn(
            "high_low_range",
            F.when(F.col("close") > 0, (F.col("high") - F.col("low")) / F.col("close"))
        )
    )

    # Rolling volatility
    window_7d = Window.partitionBy("asset_id").orderBy("date").rowsBetween(-6, Window.currentRow)
    window_30d = Window.partitionBy("asset_id").orderBy("date").rowsBetween(-29, Window.currentRow)

    features = (
        features
        .withColumn("volatility_7d", F.stddev("log_return_1d").over(window_7d))
        .withColumn("volatility_30d", F.stddev("log_return_1d").over(window_30d))
        .withColumn("rolling_max_30d", F.max("close").over(window_30d))
        .withColumn(
            "max_drawdown_30d",
            (F.col("close") - F.col("rolling_max_30d")) / F.col("rolling_max_30d")
        )
        .withColumn(
            "volatility_regime",
            F.when(F.col("volatility_30d").isNull(), None)
            .when(F.col("volatility_30d") < 0.02, "LOW")
            .when(F.col("volatility_30d") < 0.05, "NORMAL")
            .when(F.col("volatility_30d") < 0.08, "HIGH")
            .otherwise("EXTREME")
        )
    )

    # Join sentiment
    sentiment_join = deduped_sentiment.select(
        F.col("date").alias("sent_date"),
        F.col("fear_greed_value").alias("fear_greed"),
    )

    features = features.join(
        sentiment_join,
        features["date"] == sentiment_join["sent_date"],
        "left",
    ).drop("sent_date")

    # -----------------------------------------------
    # OUTPUT: Select final columns
    # -----------------------------------------------
    staged_prices = deduped_prices.select(
        "asset_id", "symbol", "name", "date",
        "open", "high", "low", "close",
        "volume", "market_cap", "_batch_id"
    )

    fact_market = features.select(
        "asset_id", "date", "open", "high", "low", "close",
        "volume", "market_cap", "return_1d", "log_return_1d",
        "high_low_range", "fear_greed", "volatility_7d",
        "volatility_30d", "max_drawdown_30d", "volatility_regime"
    )

    # Save to local CSVs
    staged_prices_path = os.path.join(tmp_dir, "staged_prices")
    fact_market_path = os.path.join(tmp_dir, "fact_market_daily")

    staged_prices.coalesce(1).write.csv(staged_prices_path, header=True, mode="overwrite")
    fact_market.coalesce(1).write.csv(fact_market_path, header=True, mode="overwrite")

    print(f"\n  === Spark Processing Summary ===")
    print(f"  Staged prices:     {staged_prices.count()} rows")
    print(f"  Fact market daily: {fact_market.count()} rows")

    # Show sample
    print("\n  Sample from fact_market_daily:")
    fact_market.select(
        "asset_id", "date", "close", "return_1d",
        "volatility_30d", "volatility_regime"
    ).orderBy(F.col("date").desc()).show(10, truncate=False)

    # Show regime distribution
    print("  Volatility regime distribution:")
    fact_market.groupBy("volatility_regime").count().orderBy("count", ascending=False).show()

    # Show per-asset stats
    print("  Per-asset record counts:")
    staged_prices.groupBy("symbol").count().orderBy("symbol").show()

    spark.stop()
    return staged_prices_path, fact_market_path


# ============================================================
# STEP 3: Upload Spark results back to Snowflake
# ============================================================
def upload_to_snowflake(staged_prices_path, fact_market_path):
    print("\n=== STEP 3: Uploading Spark results to Snowflake ===")
    conn = get_sf_connection()
    cursor = conn.cursor()

    # Find the actual CSV file (Spark writes part-00000-*.csv)
    def find_csv(spark_output_dir):
        for f in os.listdir(spark_output_dir):
            if f.startswith("part-") and f.endswith(".csv"):
                return os.path.join(spark_output_dir, f)
        return None

    # Upload staged prices
    csv_file = find_csv(staged_prices_path)
    if csv_file:
        cursor.execute("USE SCHEMA STAGED")
        cursor.execute("TRUNCATE TABLE IF EXISTS stg_prices")

        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                cursor.execute(
                    """INSERT INTO stg_prices (asset_id, symbol, name, date, open, high, low, close, volume, market_cap, _batch_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (row["asset_id"], row["symbol"], row["name"], row["date"],
                     row["open"], row["high"], row["low"], row["close"],
                     row["volume"], row["market_cap"], row["_batch_id"])
                )
                count += 1
        conn.commit()
        print(f"  Uploaded {count} staged price records")

    # Upload fact_market_daily
    csv_file = find_csv(fact_market_path)
    if csv_file:
        cursor.execute("USE SCHEMA CURATED")
        cursor.execute("TRUNCATE TABLE IF EXISTS fact_market_daily")

        with open(csv_file, "r") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                cursor.execute(
                    """INSERT INTO fact_market_daily
                       (asset_id, date, open, high, low, close, volume, market_cap,
                        return_1d, log_return_1d, high_low_range, fear_greed,
                        volatility_7d, volatility_30d, max_drawdown_30d, volatility_regime)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (row["asset_id"], row["date"], row["open"], row["high"],
                     row["low"], row["close"], row["volume"], row["market_cap"],
                     row["return_1d"] if row["return_1d"] else None,
                     row["log_return_1d"] if row["log_return_1d"] else None,
                     row["high_low_range"] if row["high_low_range"] else None,
                     row["fear_greed"] if row["fear_greed"] else None,
                     row["volatility_7d"] if row["volatility_7d"] else None,
                     row["volatility_30d"] if row["volatility_30d"] else None,
                     row["max_drawdown_30d"] if row["max_drawdown_30d"] else None,
                     row["volatility_regime"] if row["volatility_regime"] else None)
                )
                count += 1
        conn.commit()
        print(f"  Uploaded {count} fact_market_daily records")

    # Verify
    cursor.execute("SELECT COUNT(*) FROM MARKETPULSE.STAGED.stg_prices")
    print(f"  Snowflake stg_prices: {cursor.fetchone()[0]} rows")
    cursor.execute("SELECT COUNT(*) FROM MARKETPULSE.CURATED.fact_market_daily")
    print(f"  Snowflake fact_market_daily: {cursor.fetchone()[0]} rows")
    cursor.execute("SELECT COUNT(*) FROM MARKETPULSE.CURATED.mart_volatility")
    print(f"  Snowflake mart_volatility: {cursor.fetchone()[0]} rows")

    conn.close()


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 60)
    print("MarketPulse — Spark Processing Pipeline")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    tmp_dir = tempfile.mkdtemp(prefix="marketpulse_")
    print(f"Working directory: {tmp_dir}")

    try:
        prices_path, sentiment_path = export_raw_data(tmp_dir)
        staged_path, fact_path = process_with_spark(prices_path, sentiment_path, tmp_dir)
        upload_to_snowflake(staged_path, fact_path)

        print("\n" + "=" * 60)
        print("Spark pipeline complete!")
        print("=" * 60)
    except Exception as e:
        print(f"\nERROR: {e}")
        raise
