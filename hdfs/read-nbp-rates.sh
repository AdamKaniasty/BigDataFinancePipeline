#!/bin/bash

# Script to read NBP exchange rate files from HDFS
# Usage: ./hdfs/read-nbp-rates.sh [date]
# Example: ./hdfs/read-nbp-rates.sh 2025-11-23

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
DATE="${1:-$(date +%Y-%m-%d)}"

HDFS_PATH="/data/finance/raw/nbp/date=${DATE}/"
HDFS_NAMENODE="big-data-project-hdfs-namenode-1"

echo -e "${BLUE}Reading NBP exchange rate files from HDFS:${NC}"
echo -e "${GREEN}Path: ${HDFS_PATH}${NC}"
echo ""

# Check if directory exists
if ! docker exec "$HDFS_NAMENODE" hdfs dfs -test -d "$HDFS_PATH" 2>/dev/null; then
    echo -e "${YELLOW}Directory does not exist: ${HDFS_PATH}${NC}"
    echo ""
    echo "Available dates:"
    docker exec "$HDFS_NAMENODE" hdfs dfs -ls /data/finance/raw/nbp/ 2>/dev/null | grep "^d" | sed 's|.*date=||' | sed 's|/.*||' | sort -u || echo "  No dates found"
    exit 1
fi

# List files in directory
echo -e "${BLUE}Files in directory:${NC}"
docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -v "^Found" | tail -n +2

echo ""
echo -e "${BLUE}Reading JSON files:${NC}"
echo ""

# Get list of JSON files
FILES=$(docker exec "$HDFS_NAMENODE" hdfs dfs -ls "$HDFS_PATH" 2>/dev/null | grep -E "\.json$" | awk '{print $8}' || echo "")

if [ -z "$FILES" ]; then
    echo -e "${YELLOW}No JSON files found in ${HDFS_PATH}${NC}"
    exit 0
fi

# Read each JSON file
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
    
    # Read and display JSON content
    echo -e "${BLUE}Content:${NC}"
    docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null | python3 -m json.tool 2>/dev/null || docker exec "$HDFS_NAMENODE" hdfs dfs -cat "$FILE" 2>/dev/null
    
    echo ""
    echo ""
done

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Total files read: ${FILE_COUNT}${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

