#!/bin/bash

# Script to read CSV files from HDFS crypto-prices directory
# Usage: ./hdfs/read-crypto-prices.sh [date] [hour]
# Example: ./hdfs/read-crypto-prices.sh 2025-11-23 12

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
DATE="${1:-2025-11-23}"
HOUR="${2:-12}"

HDFS_PATH="/data/finance/raw/crypto-prices/date=${DATE}/hour=${HOUR}/"
HDFS_NAMENODE="big-data-project-hdfs-namenode-1"

echo -e "${BLUE}Reading CSV files from HDFS:${NC}"
echo -e "${GREEN}Path: ${HDFS_PATH}${NC}"
echo ""

# Check if directory exists
if ! docker exec "$HDFS_NAMENODE" hdfs dfs -test -d "$HDFS_PATH" 2>/dev/null; then
    echo -e "${YELLOW}Directory does not exist: ${HDFS_PATH}${NC}"
    echo ""
    echo "Available dates:"
    docker exec "$HDFS_NAMENODE" hdfs dfs -ls /data/finance/raw/crypto-prices/ 2>/dev/null | grep "^d" | awk '{print $8}' | sed 's|.*date=||' | sort -u || echo "  No dates found"
    exit 1
fi

# List files in directory
echo -e "${BLUE}Files in directory:${NC}"
docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | tail -n +2

echo ""
echo -e "${BLUE}Reading CSV files:${NC}"
echo ""

# Get list of CSV files
FILES=$(docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -E "\.csv$" | awk '{print $8}' || echo "")

if [ -z "$FILES" ]; then
    echo -e "${YELLOW}No CSV files found in ${HDFS_PATH}${NC}"
    exit 0
fi

# Read each CSV file
FILE_COUNT=0
for FILE in $FILES; do
    FILE_COUNT=$((FILE_COUNT + 1))
    FILENAME=$(basename "$FILE")
    
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}File ${FILE_COUNT}: ${FILENAME}${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Get file size
    SIZE=$(docker exec "$HDFS_NAMENODE" hdfs dfs -du -h "$FILE" 2>/dev/null | awk '{print $1}')
    echo -e "${BLUE}Size: ${SIZE}${NC}"
    echo ""
    
    # Read and display CSV content
    echo -e "${BLUE}Content:${NC}"
    docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null | head -50
    
    # Count total lines
    TOTAL_LINES=$(docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null | wc -l)
    if [ "$TOTAL_LINES" -gt 50 ]; then
        echo ""
        echo -e "${YELLOW}... (showing first 50 lines of ${TOTAL_LINES} total)${NC}"
    fi
    
    echo ""
    echo ""
done

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Total files read: ${FILE_COUNT}${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

