#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPARK_CONTAINER="big-data-project-spark-1"
SPARK_SUBMIT="/opt/spark/bin/spark-submit"
DEFAULT_MASTER="local[*]"
DEFAULT_FS="hdfs://hdfs-namenode:9000"
SPARK_JOB_TIMEOUT="${SPARK_JOB_TIMEOUT:-900}"
SPARK_SHUFFLE_PARTITIONS="${SPARK_SHUFFLE_PARTITIONS:-4}"
SPARK_DEFAULT_PARALLELISM="${SPARK_DEFAULT_PARALLELISM:-4}"
SPARK_ADAPTIVE_ENABLED="${SPARK_ADAPTIVE_ENABLED:-true}"
SPARK_ADAPTIVE_COALESCE="${SPARK_ADAPTIVE_COALESCE:-true}"

DATE_ARG=""

usage() {
    echo "Usage: $0 [--date YYYY-MM-DD] [--debug]"
    exit 1
}

DEBUG_FLAG=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)
            shift
            DATE_ARG="${1:-}"
            if [[ -z "${DATE_ARG}" ]]; then
                echo "Error: --date requires a value" >&2
                usage
            fi
            ;;
        --debug)
            DEBUG_FLAG="--debug"
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage
            ;;
    esac
    shift
done

SPARK_CONF=(
    "--conf" "spark.hadoop.fs.defaultFS=${DEFAULT_FS}"
    "--conf" "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
    "--conf" "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}"
    "--conf" "spark.sql.adaptive.enabled=${SPARK_ADAPTIVE_ENABLED}"
    "--conf" "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_ADAPTIVE_COALESCE}"
)
BASE_CMD=("${SPARK_SUBMIT}" "--master" "${DEFAULT_MASTER}" "${SPARK_CONF[@]}")

cleanup_spark_jobs() {
    docker exec "${SPARK_CONTAINER}" bash -lc "pkill -f '[e]tl_nbp\\.py' || true; pkill -f '[e]tl_crypto\\.py' || true" >/dev/null 2>&1 || true
}

on_interrupt() {
    echo "Interrupted. Cleaning up Spark jobs..."
    cleanup_spark_jobs
    exit 130
}

on_error() {
    echo "Error occurred. Cleaning up Spark jobs..."
    cleanup_spark_jobs
}

trap on_interrupt INT TERM
trap on_error ERR

run_job() {
    local script_path="$1"
    shift
    echo "Running ETL: ${script_path} $*"
    docker exec "${SPARK_CONTAINER}" timeout --kill-after 30s "${SPARK_JOB_TIMEOUT}" "${BASE_CMD[@]}" "/opt/spark-apps/${script_path}" "$@"
}

nbp_args=()
crypto_args=()
if [[ -n "${DATE_ARG}" ]]; then
    nbp_args+=(--date "${DATE_ARG}")
    crypto_args+=(--date "${DATE_ARG}")
fi
if [[ -n "${DEBUG_FLAG}" ]]; then
    nbp_args+=("${DEBUG_FLAG}")
    crypto_args+=("${DEBUG_FLAG}")
fi

run_job "etl_nbp.py" "${nbp_args[@]}"
run_job "etl_crypto.py" "${crypto_args[@]}"

echo "All ETL jobs completed."
