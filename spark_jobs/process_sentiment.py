"""
MarketPulse — Sentiment Processing Spark Job
Reads from Snowflake RAW sentiment, deduplicates, and writes to STAGED.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    return (
        SparkSession.builder
        .appName("MarketPulse-ProcessSentiment")
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3")
        .getOrCreate()
    )


def get_snowflake_options():
    return {
        "sfURL": "your_account.snowflakecomputing.com",
        "sfUser": "your_user",
        "sfPassword": "your_password",
        "sfDatabase": "MARKETPULSE",
        "sfWarehouse": "COMPUTE_WH",
    }


def process_sentiment(spark, sf_options):
    """Read, clean, dedup, and write sentiment data."""

    # Read raw
    raw_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "RAW")
        .option("dbtable", "raw_sentiment")
        .load()
    )

    print(f"Raw sentiment records: {raw_df.count()}")

    # Normalize
    cleaned_df = (
        raw_df
        .withColumn("date", F.to_date(F.col("DATE")))
        .withColumn("fear_greed_value", F.col("VALUE").cast("int"))
        .withColumn("classification", F.trim(F.col("CLASSIFICATION")))
        .withColumn("_batch_id", F.col("_BATCH_ID"))
        .filter(
            F.col("date").isNotNull()
            & F.col("fear_greed_value").isNotNull()
            & (F.col("fear_greed_value").between(0, 100))
        )
    )

    # Dedup: keep latest per date
    window = Window.partitionBy("date").orderBy(F.col("_INGESTED_AT").desc())
    deduped_df = (
        cleaned_df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )

    print(f"Deduplicated sentiment records: {deduped_df.count()}")

    # Write to staged
    output_df = deduped_df.select(
        "date", "fear_greed_value", "classification", "_batch_id"
    )

    (
        output_df.write.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "STAGED")
        .option("dbtable", "stg_sentiment")
        .mode("overwrite")
        .save()
    )

    print("Sentiment processing complete.")


def main():
    spark = create_spark_session()
    sf_options = get_snowflake_options()
    process_sentiment(spark, sf_options)
    spark.stop()


if __name__ == "__main__":
    main()
