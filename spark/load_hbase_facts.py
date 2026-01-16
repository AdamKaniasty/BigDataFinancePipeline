#!/usr/bin/env python3
"""Prepare HBase load commands from Spark analytics outputs.

Reads:
- Daily crypto closes and returns (crypto_daily).
- Hourly curated crypto aggregates (trades_agg_hourly) for volume.
- 63-day BTC/USDPLN correlation series.

Produces:
- A text file in HDFS with HBase shell `put` commands to populate
  `finance:facts_daily` with a small subset of metrics suitable for demo:
  - metrics:ret_1d
  - price:avg
  - volume:sum
  - volume:count
  - correlation:btc_usdpln_63d (for BTC rows where available)
"""

import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


CRYPTO_DAILY = (
    "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/crypto_daily"
)
CRYPTO_CURATED = (
    "hdfs://hdfs-namenode:9000/data/finance/curated/crypto/trades_agg_hourly"
)
CORR_PATH = (
    "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/"
    "corr_btc_usdpln_63d"
)
HBASE_PUTS_DIR = (
    "hdfs://hdfs-namenode:9000/data/finance/hbase/facts_daily_puts"
)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("load_hbase_facts_precompute")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare HBase put commands from analytics outputs"
    )
    parser.add_argument(
        "--date",
        help="Optional date filter YYYY-MM-DD; if omitted, uses all available dates",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark()

    try:
        df_daily = spark.read.parquet(CRYPTO_DAILY).withColumn(
            "date", F.to_date(F.col("date"))
        )
        if args.date:
            df_daily = df_daily.filter(F.col("date") == F.lit(args.date))

        df_hourly = spark.read.parquet(CRYPTO_CURATED).withColumn(
            "date", F.to_date(F.col("date"))
        )
        if args.date:
            df_hourly = df_hourly.filter(F.col("date") == F.lit(args.date))

        df_vol = df_hourly.groupBy("symbol", "date").agg(
            F.sum("volume_base").alias("volume_sum"),
            F.sum("volume_quote").alias("volume_quote_sum"),
            F.sum("trade_count").alias("trade_count_sum"),
        )

        df_corr = spark.read.parquet(CORR_PATH).withColumn(
            "date", F.to_date(F.col("date"))
        )
        if args.date:
            df_corr = df_corr.filter(F.col("date") == F.lit(args.date))

        df_join = (
            df_daily.alias("d")
            .join(
                df_vol.alias("v"),
                on=["symbol", "date"],
                how="left",
            )
            .join(
                df_corr.alias("c"),
                on="date",
                how="left",
            )
            .select(
                "symbol",
                "date",
                "close",
                "ret_1d",
                "volume_sum",
                "volume_quote_sum",
                "trade_count_sum",
                "corr_btc_usdpln_63d",
            )
        )

        df_with_key = (
            df_join.withColumn("date_str", F.date_format("date", "yyyyMMdd"))
            .withColumn(
                "salt",
                (
                    F.abs(F.hash(F.col("symbol"), F.col("date_str")))
                    % F.lit(10)
                ).cast("int"),
            )
            .withColumn(
                "rowkey",
                F.concat_ws(
                    "|",
                    F.col("salt").cast("string"),
                    F.col("symbol"),
                    F.col("date_str"),
                ),
            )
        )

        base_prefix = F.lit("put 'finance:facts_daily', '")
        key_prefix = F.concat(base_prefix, F.col("rowkey"), F.lit("', '"))
        suffix = F.lit("'")

        df_cmd = (
            df_with_key.select(
                F.explode(
                    F.array(
                        F.when(
                            F.col("ret_1d").isNotNull(),
                            F.concat(
                                key_prefix,
                                F.lit("metrics:ret_1d', '"),
                                F.col("ret_1d").cast("string"),
                                suffix,
                            ),
                        ),
                        F.when(
                            F.col("close").isNotNull(),
                            F.concat(
                                key_prefix,
                                F.lit("price:avg', '"),
                                F.col("close").cast("string"),
                                suffix,
                            ),
                        ),
                        F.when(
                            F.col("volume_sum").isNotNull(),
                            F.concat(
                                key_prefix,
                                F.lit("volume:sum', '"),
                                F.col("volume_sum").cast("string"),
                                suffix,
                            ),
                        ),
                        F.when(
                            F.col("trade_count_sum").isNotNull(),
                            F.concat(
                                key_prefix,
                                F.lit("volume:count', '"),
                                F.col("trade_count_sum").cast("string"),
                                suffix,
                            ),
                        ),
                        F.when(
                            (F.col("symbol") == F.lit("BTCUSDT"))
                            & F.col("corr_btc_usdpln_63d").isNotNull(),
                            F.concat(
                                key_prefix,
                                F.lit("correlation:btc_usdpln_63d', '"),
                                F.col("corr_btc_usdpln_63d").cast("string"),
                                suffix,
                            ),
                        ),
                    )
                ).alias("value")
            )
            .filter(F.col("value").isNotNull())
            .coalesce(1)
            .cache()
        )

        cmd_count = df_cmd.count()
        print(f"[load_hbase_facts] Generated {cmd_count} HBase put commands.")
        if cmd_count == 0:
            return

        df_cmd.write.mode("overwrite").text(HBASE_PUTS_DIR)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
