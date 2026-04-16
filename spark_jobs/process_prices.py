"""
MarketPulse — Price Processing Spark Job
Reads from Snowflake RAW layer, normalizes, deduplicates,
and writes cleaned data to STAGED layer.

Usage:
    spark-submit --packages net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3 \
        spark_jobs/process_prices.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    return (
        SparkSession.builder
        .appName("MarketPulse-ProcessPrices")
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3")
        .getOrCreate()
    )


def get_snowflake_options():
    """Snowflake connection options for Spark connector."""
    return {
        "sfURL": "your_account.snowflakecomputing.com",
        "sfUser": "your_user",
        "sfPassword": "your_password",
        "sfDatabase": "MARKETPULSE",
        "sfWarehouse": "COMPUTE_WH",
    }


def read_raw_prices(spark, sf_options):
    """Read raw prices from Snowflake."""
    return (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "RAW")
        .option("dbtable", "raw_prices")
        .load()
    )


def normalize_schema(df):
    """
    Standardize column types and handle nulls.
    - Ensure numeric columns are DoubleType
    - Trim string columns
    - Filter out rows with null critical fields
    """
    return (
        df
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
        # Filter out invalid records
        .filter(
            F.col("asset_id").isNotNull()
            & F.col("date").isNotNull()
            & F.col("close").isNotNull()
            & (F.col("close") > 0)
        )
    )


def deduplicate(df):
    """
    Keep only the latest ingested record per (asset_id, date).
    This makes the pipeline idempotent — reruns don't create duplicates.
    """
    window = Window.partitionBy("asset_id", "date").orderBy(F.col("_INGESTED_AT").desc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def write_staged_prices(df, sf_options):
    """Write cleaned prices to Snowflake STAGED layer."""
    output_df = df.select(
        "asset_id", "symbol", "name", "date",
        "open", "high", "low", "close",
        "volume", "market_cap", "_batch_id",
    )

    (
        output_df.write.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "STAGED")
        .option("dbtable", "stg_prices")
        .mode("overwrite")  # Full refresh for MVP; switch to merge for incremental
        .save()
    )


def main():
    spark = create_spark_session()
    sf_options = get_snowflake_options()

    print("Reading raw prices from Snowflake...")
    raw_df = read_raw_prices(spark, sf_options)
    print(f"Raw record count: {raw_df.count()}")

    print("Normalizing schema...")
    normalized_df = normalize_schema(raw_df)

    print("Deduplicating...")
    deduped_df = deduplicate(normalized_df)
    print(f"Deduplicated record count: {deduped_df.count()}")

    print("Writing to STAGED layer...")
    write_staged_prices(deduped_df, sf_options)

    print("Price processing complete.")
    spark.stop()


if __name__ == "__main__":
    main()
