#!/bin/bash

# Script to read and format CSV files from HDFS crypto-prices directory
# Usage: ./hdfs/read-crypto-prices-formatted.sh [date] [hour]
# Example: ./hdfs/read-crypto-prices-formatted.sh 2025-11-23 12

set -e

# Default values
DATE="${1:-2025-11-23}"
HOUR="${2:-12}"

HDFS_PATH="/data/finance/raw/crypto-prices/date=${DATE}/hour=${HOUR}/"
HDFS_NAMENODE="big-data-project-hdfs-namenode-1"

# Check if directory exists
if ! docker exec "$HDFS_NAMENODE" hdfs dfs -test -d "$HDFS_PATH" 2>/dev/null; then
    echo "Directory does not exist: ${HDFS_PATH}"
    exit 1
fi

# Get list of CSV files
FILES=$(docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -E "\.csv$" | awk '{print $8}' || echo "")

if [ -z "$FILES" ]; then
    echo "No CSV files found in ${HDFS_PATH}"
    exit 0
fi

# Read and format CSV from all files
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
{
    for FILE in $FILES; do
        docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null
    done
} | python3 "${SCRIPT_DIR}/read_csv.py"

