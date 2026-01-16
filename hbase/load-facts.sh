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

NAMENODE_CONTAINER="big-data-project-hdfs-namenode-1"
HBASE_CONTAINER="$(docker ps --filter "ancestor=harisekhon/hbase:latest" --format "{{.Names}}" | head -1)"

HBASE_PUTS_HDFS_DIR="/data/finance/hbase/facts_daily_puts"

DATE_ARG=""

cleanup_spark_job() {
    docker exec "${SPARK_CONTAINER}" bash -lc "pkill -f '[l]oad_hbase_facts\\.py' || true" >/dev/null 2>&1 || true
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

usage() {
    echo "Usage: $0 [--date YYYY-MM-DD]"
    exit 1
}

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
        *)
            echo "Unknown argument: $1" >&2
            usage
            ;;
    esac
    shift
done

if [[ -z "${HBASE_CONTAINER}" ]]; then
    echo "Error: HBase container (harisekhon/hbase:latest) not running" >&2
    exit 1
fi

if ! docker ps | grep -q "${NAMENODE_CONTAINER}"; then
    echo "Error: HDFS NameNode container ${NAMENODE_CONTAINER} not running" >&2
    exit 1
fi

echo "=========================================="
echo "Loading analytics facts into HBase"
echo "Spark container:  ${SPARK_CONTAINER}"
echo "HDFS NameNode:    ${NAMENODE_CONTAINER}"
echo "HBase container:  ${HBASE_CONTAINER}"
echo "=========================================="
echo ""

echo "Step 1: Ensure HBase table 'finance:facts_daily' exists ..."
"${SCRIPT_DIR}/init-hbase.sh"

SPARK_CONF=(
    "--conf" "spark.hadoop.fs.defaultFS=${DEFAULT_FS}"
    "--conf" "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
    "--conf" "spark.default.parallelism=${SPARK_DEFAULT_PARALLELISM}"
    "--conf" "spark.sql.adaptive.enabled=${SPARK_ADAPTIVE_ENABLED}"
    "--conf" "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_ADAPTIVE_COALESCE}"
)
BASE_CMD=("${SPARK_SUBMIT}" "--master" "${DEFAULT_MASTER}" "${SPARK_CONF[@]}")

echo ""
echo "Step 2: Generating HBase put commands with Spark ..."
LOAD_ARGS=()
if [[ -n "${DATE_ARG}" ]]; then
    LOAD_ARGS+=(--date "${DATE_ARG}")
fi

docker exec "${SPARK_CONTAINER}" timeout --kill-after 30s "${SPARK_JOB_TIMEOUT}" "${BASE_CMD[@]}" "/opt/spark-apps/load_hbase_facts.py" "${LOAD_ARGS[@]}"

echo ""
echo "Step 3: Streaming commands from HDFS into HBase shell ..."
if ! docker exec "${NAMENODE_CONTAINER}" hdfs dfs -test -d "${HBASE_PUTS_HDFS_DIR}"; then
    echo "Error: HBase puts directory ${HBASE_PUTS_HDFS_DIR} not found in HDFS" >&2
    exit 1
fi

docker exec "${NAMENODE_CONTAINER}" hdfs dfs -cat "${HBASE_PUTS_HDFS_DIR}"/* \
    | docker exec -i "${HBASE_CONTAINER}" hbase shell

echo ""
echo "Step 4: Verifying a few rows in HBase ..."
printf "scan 'finance:facts_daily', {LIMIT => 10}\nexit\n" | docker exec -i "${HBASE_CONTAINER}" hbase shell

echo ""
echo "HBase facts load completed."
echo "=========================================="
