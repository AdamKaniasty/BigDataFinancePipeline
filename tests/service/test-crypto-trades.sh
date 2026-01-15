#!/bin/bash

# Script to read CSV files from HDFS crypto-trades directory
# Usage: ./tests/service/test-crypto-trades.sh [date] [hour]
# Example: ./tests/service/test-crypto-trades.sh 2025-12-05 12

set -euo pipefail

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

DATE="${1:-$(date +%Y-%m-%d)}"
HOUR="${2:-}"

HDFS_DATE_PATH="/data/finance/raw/crypto-trades/date=${DATE}"
HDFS_PATH=""
HDFS_NAMENODE="big-data-project-hdfs-namenode-1"

echo -e "${BLUE}Reading CSV files from HDFS:${NC}"
echo ""

if ! docker exec "$HDFS_NAMENODE" hdfs dfs -test -d "$HDFS_DATE_PATH" 2>/dev/null; then
    echo -e "${YELLOW}Directory does not exist: ${HDFS_DATE_PATH}${NC}"
    echo ""
    echo "Available dates:"
    docker exec "$HDFS_NAMENODE" hdfs dfs -ls /data/finance/raw/crypto-trades/ 2>/dev/null | grep "^d" | awk '{print $8}' | sed 's|.*date=||' | sort -u || echo "  No dates found"
    exit 1
fi

if [[ -z "${HOUR}" ]]; then
    # Auto-pick the latest hour directory for the date (more reliable than local clock hour).
    HOUR="$(
        docker exec "$HDFS_NAMENODE" hdfs dfs -ls "${HDFS_DATE_PATH}" 2>/dev/null \
            | awk '{print $8}' \
            | sed -n 's#.*/hour=\([0-9]\{1,2\}\)$#\1#p' \
            | sort -n \
            | tail -n 1
    )"
    if [[ -z "${HOUR}" ]]; then
        echo -e "${YELLOW}No hour directories found under: ${HDFS_DATE_PATH}${NC}"
        echo ""
        echo "Directory listing:"
        docker exec "$HDFS_NAMENODE" hdfs dfs -ls "${HDFS_DATE_PATH}" 2>/dev/null || true
        exit 1
    fi
fi

HDFS_PATH="${HDFS_DATE_PATH}/hour=${HOUR}/"
echo -e "${GREEN}Path: ${HDFS_PATH}${NC}"
echo ""

if ! docker exec "$HDFS_NAMENODE" hdfs dfs -test -d "$HDFS_PATH" 2>/dev/null; then
    echo -e "${YELLOW}Directory does not exist: ${HDFS_PATH}${NC}"
    echo ""
    echo "Available hours for ${DATE}:"
    docker exec "$HDFS_NAMENODE" hdfs dfs -ls "${HDFS_DATE_PATH}" 2>/dev/null \
        | awk '{print $8}' \
        | sed -n 's#.*/hour=\([0-9]\{1,2\}\)$#\1#p' \
        | sort -n \
        | sed 's/^/  /' || true
    exit 1
fi

echo -e "${BLUE}Files in directory:${NC}"
docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -v "^Found" || true

echo ""
echo -e "${BLUE}Reading CSV files:${NC}"
echo ""

FILES=$(docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -E "\.csv$" | awk '{print $8}' || echo "")

if [ -z "$FILES" ]; then
    echo -e "${YELLOW}No CSV files found in ${HDFS_PATH}${NC}"
    exit 0
fi

FILE_COUNT=0
for FILE in $FILES; do
    FILE_COUNT=$((FILE_COUNT + 1))
    FILENAME=$(basename "$FILE")

    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}File ${FILE_COUNT}: ${FILENAME}${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    SIZE=$(docker exec "$HDFS_NAMENODE" hdfs dfs -du -h "$FILE" 2>/dev/null | awk '{print $1}')
    echo -e "${BLUE}Size: ${SIZE}${NC}"
    echo ""

    echo -e "${BLUE}Content:${NC}"
    docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null | head -20

    TOTAL_LINES=$(docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null | wc -l)
    if [ "$TOTAL_LINES" -gt 20 ]; then
        echo ""
        echo -e "${YELLOW}... (showing first 20 lines of ${TOTAL_LINES} total)${NC}"
    fi
    echo ""
done

echo -e "${GREEN}Total files read: ${FILE_COUNT}${NC}"
