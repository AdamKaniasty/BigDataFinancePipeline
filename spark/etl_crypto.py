#!/usr/bin/env python3
"""ETL for crypto trades: raw CSV (aggTrade) -> curated hourly aggregates."""

import argparse
from typing import List
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


RAW_BASE = "hdfs://hdfs-namenode:9000/data/finance/raw/crypto-trades"
CURATED_BASE = "hdfs://hdfs-namenode:9000/data/finance/curated/crypto/trades_agg_hourly"


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL crypto prices to hourly aggregates")
    parser.add_argument(
        "--date",
        help="Optional date filter YYYY-MM-DD; if omitted processes all available dates",
    )
    parser.add_argument(
        "--hour",
        type=int,
        help="Optional hour filter (0-23) to narrow processing window",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose logging for debugging",
    )
    return parser.parse_args()


def select_available(df, columns: List[str]):
    present = [c for c in columns if c in df.columns]
    return df.select(*present)


def main() -> None:
    args = parse_args()
    spark = get_spark("etl_crypto")

    if args.date and args.hour is not None:
        input_path = f"{RAW_BASE}/date={args.date}/hour={args.hour}/*.csv"
    elif args.date:
        input_path = f"{RAW_BASE}/date={args.date}/hour=*/*.csv"
    else:
        input_path = f"{RAW_BASE}/date=*/hour=*/*.csv"

    if args.debug:
        print(f"[etl_crypto] Input path: {input_path}")

    df_raw = (
        spark.read.option("header", "true")
        .option("mode", "DROPMALFORMED")
        .csv(input_path)
    )

    if args.debug:
        print(f"[etl_crypto] Raw row count: {df_raw.count()}")

    wanted = ["symbol", "price", "quantity", "event_time"]
    df_sel = select_available(df_raw, wanted)

    df_cast = (
        df_sel.withColumn("price", F.col("price").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("double"))
        .withColumn(
            "event_ts", F.to_timestamp((F.col("event_time").cast("double") / 1000.0))
        )
    ).dropna(subset=["symbol", "event_ts", "price", "quantity"])

    df_enriched = (
        df_cast.withColumn("date", F.to_date(F.col("event_ts")))
        .withColumn("hour", F.hour(F.col("event_ts")))
        .withColumn("hour_start_ts", F.date_trunc("hour", F.col("event_ts")))
        .withColumn("quote_qty", F.col("quantity") * F.col("price"))
    )

    agg = (
        df_enriched.groupBy("symbol", "date", "hour")
        .agg(
            F.avg("price").alias("price_avg"),
            F.expr("approx_percentile(price, 0.95)").alias("price_p95"),
            F.max("price").alias("price_high"),
            F.min("price").alias("price_low"),
            F.sum("quantity").alias("volume_base"),
            F.sum("quote_qty").alias("volume_quote"),
            F.count("*").alias("trade_count"),
            F.expr("min_by(price, event_ts)").alias("price_open"),
            F.expr("max_by(price, event_ts)").alias("price_close"),
            F.min("event_ts").alias("ts_min"),
            F.max("event_ts").alias("ts_max"),
        )
        .withColumn("hour_start_ts", F.date_trunc("hour", F.col("ts_min")))
        .withColumn("load_ts", F.current_timestamp())
    )

    if args.debug:
        print(f"[etl_crypto] Aggregated row count: {agg.count()}")

    df_out = agg.select(
        "symbol",
        "hour_start_ts",
        "ts_min",
        "ts_max",
        "price_open",
        "price_close",
        "price_high",
        "price_low",
        "price_avg",
        "price_p95",
        "volume_base",
        "volume_quote",
        "trade_count",
        "load_ts",
        "date",
        "hour",
    )

    (
        df_out.write.mode("overwrite")
        .partitionBy("date", "hour")
        .parquet(CURATED_BASE)
    )

    spark.stop()


if __name__ == "__main__":
    main()
