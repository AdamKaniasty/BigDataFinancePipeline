#!/usr/bin/env python3
"""Sanity checks for analytics outputs.

Verifies that:
- Aggregated datasets exist and are non-empty.
- Basic numeric fields are within reasonable bounds (no obviously crazy values).
"""

import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


FX_RET_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/fx_daily_returns"
CRYPTO_DAILY_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/crypto_daily"
CORR_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/daily/corr_btc_usdpln_63d"
CRYPTO_MONTHLY_OUT = "hdfs://hdfs-namenode:9000/data/finance/aggregated/monthly/crypto_monthly_facts"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("check_analytics_sanity")
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .getOrCreate()
    )


def check_fx_returns(spark: SparkSession) -> None:
    df = spark.read.parquet(FX_RET_OUT)
    count = df.count()
    print(f"[check] fx_daily_returns rows: {count}")
    if count == 0:
        raise ValueError("fx_daily_returns is empty")

    # Ignore first-day null returns; focus on non-null ones.
    crazy = df.filter(
        F.col("ret_1d").isNotNull() & (F.abs(F.col("ret_1d")) > F.lit(10.0))
    ).count()
    if crazy > 0:
        raise ValueError(f"fx_daily_returns has {crazy} rows with |ret_1d| > 10.0")


def check_crypto_daily(spark: SparkSession) -> None:
    df = spark.read.parquet(CRYPTO_DAILY_OUT)
    count = df.count()
    print(f"[check] crypto_daily rows: {count}")
    if count == 0:
        raise ValueError("crypto_daily is empty")

    # Close prices should be positive
    non_positive = df.filter(F.col("close") <= F.lit(0.0)).count()
    if non_positive > 0:
        raise ValueError(f"crypto_daily has {non_positive} rows with non-positive close")

    crazy = df.filter(
        F.col("ret_1d").isNotNull() & (F.abs(F.col("ret_1d")) > F.lit(10.0))
    ).count()
    if crazy > 0:
        raise ValueError(f"crypto_daily has {crazy} rows with |ret_1d| > 10.0")


def check_corr(spark: SparkSession) -> None:
    df = spark.read.parquet(CORR_OUT)
    count = df.count()
    print(f"[check] corr_btc_usdpln_63d rows: {count}")
    if count == 0:
        raise ValueError("corr_btc_usdpln_63d is empty")

    # Correlations should be between -1 and 1 (allow small numerical slack)
    out_of_range = df.filter(
        (F.col("corr_btc_usdpln_63d") < F.lit(-1.0000001))
        | (F.col("corr_btc_usdpln_63d") > F.lit(1.0000001))
    ).count()
    if out_of_range > 0:
        raise ValueError(
            f"corr_btc_usdpln_63d has {out_of_range} rows with correlation outside [-1, 1]"
        )


def check_crypto_monthly(spark: SparkSession) -> None:
    df = spark.read.parquet(CRYPTO_MONTHLY_OUT)
    count = df.count()
    print(f"[check] crypto_monthly_facts rows: {count}")
    if count == 0:
        raise ValueError("crypto_monthly_facts is empty")

    negatives = df.filter(F.col("last_close") <= F.lit(0.0)).count()
    if negatives > 0:
        raise ValueError(
            f"crypto_monthly_facts has {negatives} rows with non-positive last_close"
        )

    crazy_avg = df.filter(
        F.col("avg_ret_1d").isNotNull() & (F.abs(F.col("avg_ret_1d")) > F.lit(10.0))
    ).count()
    if crazy_avg > 0:
        raise ValueError(
            f"crypto_monthly_facts has {crazy_avg} rows with |avg_ret_1d| > 10.0"
        )

    negative_vol = df.filter(F.col("vol_ret") < F.lit(0.0)).count()
    if negative_vol > 0:
        raise ValueError(
            f"crypto_monthly_facts has {negative_vol} rows with negative vol_ret"
        )


def main() -> None:
    spark = get_spark()
    try:
        check_fx_returns(spark)
        check_crypto_daily(spark)
        check_corr(spark)
        check_crypto_monthly(spark)
        print("[check] All analytics sanity checks passed.")
    except Exception as exc:  # noqa: BLE001
        print(f"[check] Sanity check FAILED: {exc}", file=sys.stderr)
        spark.stop()
        sys.exit(1)
    spark.stop()


if __name__ == "__main__":
    main()

