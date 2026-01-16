#!/usr/bin/env python3
"""ETL for NBP FX rates: raw JSON -> curated Parquet (fx_daily)."""

import argparse
import datetime as dt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


RAW_BASE = "hdfs://hdfs-namenode:9000/data/finance/raw/nbp"
CURATED_BASE = "hdfs://hdfs-namenode:9000/data/finance/curated/nbp/fx_daily"


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode:9000")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL NBP FX rates to curated Parquet")
    parser.add_argument(
        "--date",
        help="Optional date filter YYYY-MM-DD; if omitted processes all available dates",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable verbose logging for debugging",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = get_spark("etl_nbp")

    target_month = None
    if args.date:
        parsed = dt.date.fromisoformat(args.date)
        target_month = (parsed.year, parsed.month)
        input_path = f"{RAW_BASE}/date={args.date}/nbp_rate_???_*.json"
    else:
        input_path = f"{RAW_BASE}/date=*/nbp_rate_???_*.json"

    df_raw = spark.read.json(input_path)

    if args.debug:
        print(f"[etl_nbp] Input path: {input_path}")
        print(f"[etl_nbp] Raw row count: {df_raw.count()}")

    # Extract date from file name; filenames contain YYYYMMDD just before .json
    file_col = F.input_file_name()
    fx_date_digits = F.regexp_extract(file_col, r"([0-9]{8})", 1)
    fx_date = F.to_date(fx_date_digits, "yyyyMMdd")

    if args.debug:
        df_debug = df_raw.select(
            F.col("code").alias("code"),
            F.col("currency").alias("currency"),
            F.col("mid").alias("mid_raw"),
            fx_date_digits.alias("fx_date_digits"),
            fx_date.alias("fx_date_expr"),
            file_col.alias("file_name"),
        )
        sample_rows = df_debug.limit(5).collect()
        print("[etl_nbp] Sample raw rows with derived date:")
        for row in sample_rows:
            print(f"[etl_nbp]   {row}")

    df_curated = (
        df_raw.select(
            F.col("code").alias("code"),
            F.col("currency").alias("currency"),
            F.col("mid").cast("double").alias("mid"),
            fx_date.alias("fx_date"),
        )
        .withColumn("load_ts", F.current_timestamp())
        .withColumn("year", F.year(F.col("fx_date")))
        .withColumn("month", F.month(F.col("fx_date")))
        .dropna(subset=["fx_date", "code", "mid"])
    )

    if target_month is not None:
        year, month = target_month
        existing_path = f"{CURATED_BASE}/year={year}/month={month}"
        try:
            df_existing = (
                spark.read.option("basePath", CURATED_BASE)
                .parquet(existing_path)
                .select(
                    "code",
                    "currency",
                    "mid",
                    "fx_date",
                    "load_ts",
                    "year",
                    "month",
                )
            )
            df_curated = df_existing.unionByName(df_curated, allowMissingColumns=True)
        except Exception as exc:  # noqa: BLE001
            df_existing = None
            if args.debug:
                print(f"[etl_nbp] No existing curated partition at {existing_path}: {exc}")

    # Keep latest record per (fx_date, code) by load_ts so re-runs update correctly.
    w = Window.partitionBy("fx_date", "code").orderBy(F.col("load_ts").desc())
    df_curated = (
        df_curated.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    if args.debug:
        print(f"[etl_nbp] Curated row count: {df_curated.count()}")

    (
        df_curated.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(CURATED_BASE)
    )

    spark.stop()


if __name__ == "__main__":
    main()
