#!/usr/bin/env python3
"""Analytics for FX and crypto curated data.

Computes:
- Daily FX returns per currency.
- Daily crypto closes and returns per symbol.
- 63-day rolling correlation between BTCUSDT and USD/PLN returns.
- Monthly crypto facts per symbol.
"""

import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


FX_CURATED = "hdfs://hdfs-namenode:9000/data/finance/curated/nbp/fx_daily"
CRYPTO_CURATED = "hdfs://hdfs-namenode:9000/data/finance/curated/crypto/trades_agg_hourly"

FX_RET_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/fx_daily_returns"
CRYPTO_DAILY_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/crypto_daily"
CORR_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/corr_btc_usdpln_63d"
CRYPTO_MONTHLY_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/monthly/crypto_monthly_facts"


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="FX and crypto analytics")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose logging for debugging",
    )
    return parser.parse_args()


def compute_fx_returns(spark: SparkSession, debug: bool):
    df_fx = spark.read.parquet(FX_CURATED).select("fx_date", "code", "mid")

    if debug:
        print(f"[analytics] fx_daily rows: {df_fx.count()}")

    w = Window.partitionBy("code").orderBy("fx_date")

    df_fx_ret = (
        df_fx.withColumn("mid_lag", F.lag("mid").over(w))
        .withColumn(
            "ret_1d",
            F.when(
                F.col("mid_lag").isNotNull(),
                (F.col("mid") / F.col("mid_lag") - F.lit(1.0)).cast("double"),
            ),
        )
        .withColumn("year", F.year("fx_date"))
        .withColumn("month", F.month("fx_date"))
        .drop("mid_lag")
    )

    if debug:
        df_fx_ret.select("fx_date", "code", "mid", "ret_1d").orderBy(
            "fx_date", "code"
        ).show(10, truncate=False)

    (
        df_fx_ret.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(FX_RET_OUT)
    )


def compute_crypto_daily_returns(spark: SparkSession, debug: bool):
    df = spark.read.parquet(CRYPTO_CURATED)

    if debug:
        print(f"[analytics] trades_agg_hourly rows: {df.count()}")

    # Ensure date is a date type for ordering and joins
    df = df.withColumn("date", F.to_date("date"))

    # Daily close per symbol/date based on latest ts_max
    df_daily = (
        df.select("symbol", "date", "ts_max", "price_close")
        .where(F.col("date").isNotNull())
        .groupBy("symbol", "date")
        .agg(
            F.max(F.struct("ts_max", "price_close")).alias("m"),
        )
        .select(
            "symbol",
            "date",
            F.col("m.price_close").alias("close"),
            F.col("m.ts_max").alias("ts_max"),
        )
    )

    w = Window.partitionBy("symbol").orderBy("date")

    df_daily_ret = (
        df_daily.withColumn("close_lag", F.lag("close").over(w))
        .withColumn(
            "ret_1d",
            F.when(
                F.col("close_lag").isNotNull(),
                (F.col("close") / F.col("close_lag") - F.lit(1.0)).cast("double"),
            ),
        )
        .drop("close_lag")
    )

    if debug:
        df_daily_ret.orderBy("symbol", "date").show(10, truncate=False)

    (
        df_daily_ret.write.mode("overwrite")
        .partitionBy("symbol")
        .parquet(CRYPTO_DAILY_OUT)
    )


def compute_corr_btc_usdpln_63d(spark: SparkSession, debug: bool):
    # Load daily returns
    df_fx_ret = spark.read.parquet(FX_RET_OUT).select(
        "fx_date", "code", "ret_1d"
    )
    df_crypto_daily = spark.read.parquet(CRYPTO_DAILY_OUT).select(
        "symbol", "date", "ret_1d"
    )

    # Filter BTCUSDT and USD (USD/PLN)
    df_btc = df_crypto_daily.filter(F.col("symbol") == "BTCUSDT").select(
        F.col("date").alias("d"), F.col("ret_1d").alias("ret_btc")
    )
    df_usd = df_fx_ret.filter(F.col("code") == "USD").select(
        F.col("fx_date").alias("d"), F.col("ret_1d").alias("ret_usdpln")
    )

    df_joined = (
        df_btc.join(df_usd, on="d", how="inner")
        .orderBy("d")
        .withColumn("x", F.col("ret_btc"))
        .withColumn("y", F.col("ret_usdpln"))
    )

    if debug:
        print(f"[analytics] joined BTC/US D rows: {df_joined.count()}")

    w = Window.orderBy("d").rowsBetween(-62, 0)

    df_corr = (
        df_joined.withColumn("n", F.count("*").over(w))
        .withColumn("sum_x", F.sum("x").over(w))
        .withColumn("sum_y", F.sum("y").over(w))
        .withColumn("sum_x2", F.sum(F.col("x") * F.col("x")).over(w))
        .withColumn("sum_y2", F.sum(F.col("y") * F.col("y")).over(w))
        .withColumn("sum_xy", F.sum(F.col("x") * F.col("y")).over(w))
        .withColumn(
            "corr_btc_usdpln_63d",
            F.when(
                F.col("n") >= 2,
                (
                    F.col("n") * F.col("sum_xy")
                    - F.col("sum_x") * F.col("sum_y")
                )
                / F.sqrt(
                    (
                        F.col("n") * F.col("sum_x2")
                        - F.col("sum_x") * F.col("sum_x")
                    )
                    * (
                        F.col("n") * F.col("sum_y2")
                        - F.col("sum_y") * F.col("sum_y")
                    )
                ),
            ),
        )
        .select(F.col("d").alias("date"), "corr_btc_usdpln_63d")
    )

    if debug:
        df_corr.orderBy("date").show(10, truncate=False)

    df_corr.write.mode("overwrite").parquet(CORR_OUT)


def compute_crypto_monthly_facts(spark: SparkSession, debug: bool):
    df_daily = spark.read.parquet(CRYPTO_DAILY_OUT).select(
        "symbol", "date", "close", "ret_1d"
    )

    df_daily = df_daily.withColumn("year", F.year("date")).withColumn(
        "month", F.month("date")
    )

    # Monthly stats per symbol
    df_monthly = (
        df_daily.groupBy("symbol", "year", "month")
        .agg(
            F.avg("ret_1d").alias("avg_ret_1d"),
            F.stddev_pop("ret_1d").alias("vol_ret"),
            F.count(F.col("ret_1d")).alias("days_ret"),
            F.max(F.struct("date", "close")).alias("last_close_struct"),
        )
        .select(
            "symbol",
            "year",
            "month",
            "avg_ret_1d",
            "vol_ret",
            "days_ret",
            F.col("last_close_struct.close").alias("last_close"),
        )
    )

    if debug:
        df_monthly.orderBy("symbol", "year", "month").show(10, truncate=False)

    (
        df_monthly.write.mode("overwrite")
        .partitionBy("symbol")
        .parquet(CRYPTO_MONTHLY_OUT)
    )


def main() -> None:
    args = parse_args()
    spark = get_spark("analytics_fx_crypto")

    compute_fx_returns(spark, debug=args.debug)
    compute_crypto_daily_returns(spark, debug=args.debug)
    compute_corr_btc_usdpln_63d(spark, debug=args.debug)
    compute_crypto_monthly_facts(spark, debug=args.debug)

    spark.stop()


if __name__ == "__main__":
    main()

