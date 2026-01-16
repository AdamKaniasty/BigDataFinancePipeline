#!/bin/bash

set -euo pipefail

SPARK_CONTAINER="big-data-project-spark-1"
SPARK_SUBMIT="/opt/spark/bin/spark-submit"
DEFAULT_MASTER="local[*]"
DEFAULT_FS="hdfs://hdfs-namenode:9000"
SPARK_JOB_TIMEOUT="${SPARK_JOB_TIMEOUT:-900}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-4}"
SPARK_ADAPTIVE_ENABLED="${SPARK_ADAPTIVE_ENABLED:-true}"
SPARK_ADAPTIVE_COALESCE="${SPARK_ADAPTIVE_COALESCE:-true}"

SPARK_CONF=(
    "--conf" "spark.hadoop.fs.defaultFS=${DEFAULT_FS}"
    "--conf" "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
    "--conf" "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}"
    "--conf" "spark.sql.adaptive.enabled=${SPARK_ADAPTIVE_ENABLED}"
    "--conf" "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_ADAPTIVE_COALESCE}"
)
BASE_CMD=("${SPARK_SUBMIT}" "--master" "${DEFAULT_MASTER}" "${SPARK_CONF[@]}")

cleanup_spark_job() {
    docker exec "${SPARK_CONTAINER}" bash -lc "pkill -f '[c]heck_analytics_sanity\\.py' || true" >/dev/null 2>&1 || true
}

on_interrupt() {
    echo "Interrupted. Cleaning up Spark job..."
    cleanup_spark_job
    exit 130
}

on_error() {
    echo "Error occurred. Cleaning up Spark job..."
    cleanup_spark_job
}

trap on_interrupt INT TERM
trap on_error ERR

echo "Running analytics sanity checks..."
docker exec "${SPARK_CONTAINER}" timeout --kill-after 30s "${SPARK_JOB_TIMEOUT}" "${BASE_CMD[@]}" "/opt/spark-apps/check_analytics_sanity.py"
echo "Analytics sanity checks completed successfully."
