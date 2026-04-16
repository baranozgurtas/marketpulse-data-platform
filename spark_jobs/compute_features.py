"""
MarketPulse — Feature Computation Spark Job
Computes derived features from staged data:
- Daily returns (simple + log)
- Rolling volatility (7d, 30d)
- High-low range
- Max drawdown

These feed into the CURATED fact table.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def create_spark_session():
    return (
        SparkSession.builder
        .appName("MarketPulse-ComputeFeatures")
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


def compute_features(spark, sf_options):
    """
    Read staged prices, compute derived features, write to a features table
    that feeds mart_volatility.
    """

    # Read staged prices
    prices_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "STAGED")
        .option("dbtable", "stg_prices")
        .load()
    )

    # Read staged sentiment
    sentiment_df = (
        spark.read.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "STAGED")
        .option("dbtable", "stg_sentiment")
        .load()
        .select(
            F.col("DATE").alias("sent_date"),
            F.col("FEAR_GREED_VALUE").alias("fear_greed"),
        )
    )

    # Compute returns
    asset_window = Window.partitionBy("ASSET_ID").orderBy("DATE")

    features_df = (
        prices_df
        .withColumn("prev_close", F.lag("CLOSE").over(asset_window))
        .withColumn(
            "return_1d",
            F.when(
                F.col("prev_close") > 0,
                (F.col("CLOSE") - F.col("prev_close")) / F.col("prev_close")
            )
        )
        .withColumn(
            "log_return_1d",
            F.when(
                F.col("prev_close") > 0,
                F.log(F.col("CLOSE") / F.col("prev_close"))
            )
        )
        .withColumn(
            "high_low_range",
            F.when(
                F.col("CLOSE") > 0,
                (F.col("HIGH") - F.col("LOW")) / F.col("CLOSE")
            )
        )
    )

    # Rolling volatility windows
    window_7d = (
        Window.partitionBy("ASSET_ID").orderBy("DATE")
        .rowsBetween(-6, Window.currentRow)
    )
    window_30d = (
        Window.partitionBy("ASSET_ID").orderBy("DATE")
        .rowsBetween(-29, Window.currentRow)
    )

    features_df = (
        features_df
        .withColumn("volatility_7d", F.stddev("log_return_1d").over(window_7d))
        .withColumn("volatility_30d", F.stddev("log_return_1d").over(window_30d))
        # Max drawdown: how far has price fallen from rolling 30d peak
        .withColumn("rolling_max_30d", F.max("CLOSE").over(window_30d))
        .withColumn(
            "max_drawdown_30d",
            (F.col("CLOSE") - F.col("rolling_max_30d")) / F.col("rolling_max_30d")
        )
        # Volatility regime classification
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
    features_df = features_df.join(
        sentiment_df,
        features_df["DATE"] == sentiment_df["sent_date"],
        "left",
    ).drop("sent_date")

    # Select final columns for fact table
    output_df = features_df.select(
        F.col("ASSET_ID").alias("asset_id"),
        F.col("DATE").alias("date"),
        F.col("OPEN").alias("open"),
        F.col("HIGH").alias("high"),
        F.col("LOW").alias("low"),
        F.col("CLOSE").alias("close"),
        F.col("VOLUME").alias("volume"),
        F.col("MARKET_CAP").alias("market_cap"),
        "return_1d",
        "log_return_1d",
        "high_low_range",
        "fear_greed",
        "volatility_7d",
        "volatility_30d",
        "max_drawdown_30d",
        "volatility_regime",
    )

    print(f"Feature records computed: {output_df.count()}")

    # Write to a staging area for the curated layer
    (
        output_df.write.format("snowflake")
        .options(**sf_options)
        .option("sfSchema", "CURATED")
        .option("dbtable", "fact_market_daily")
        .mode("overwrite")
        .save()
    )

    print("Feature computation complete.")


def main():
    spark = create_spark_session()
    sf_options = get_snowflake_options()
    compute_features(spark, sf_options)
    spark.stop()


if __name__ == "__main__":
    main()
