#!/bin/bash
# Preview HDFS files from Docker

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

NAMENODE_CONTAINER="big-data-project-hdfs-namenode-1"
HDFS_PATH="${1:-/data/finance/raw/crypto-trades}"

echo "=========================================="
echo "Preview HDFS Files"
echo "=========================================="
echo ""
echo "Path: $HDFS_PATH"
echo ""

# List files
echo "=== Listing files ==="
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls -R "${HDFS_PATH}" 2>/dev/null | tail -20
echo ""

# Find the most recent file (recursively)
LATEST_FILE="$(docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls -R -t "${HDFS_PATH}" 2>/dev/null | grep -v "^d" | grep -v "^Found" | head -1 | awk '{print $8}')"

if [ -z "$LATEST_FILE" ] || [ "$LATEST_FILE" == "" ]; then
    echo -e "${YELLOW}⚠ No files found in $HDFS_PATH${NC}"
    echo ""
    echo "Usage:"
    echo "  ./hdfs/preview-hdfs-files.sh [HDFS_PATH]"
    echo ""
    echo "Examples:"
    echo "  ./hdfs/preview-hdfs-files.sh /data/finance/raw/crypto-trades"
    echo "  ./hdfs/preview-hdfs-files.sh /data/finance/raw/crypto-trades/date=2025-12-05/hour=13"
    echo "  ./hdfs/preview-hdfs-files.sh /data/finance/raw/crypto (legacy path)"
    exit 0
fi

echo "=== Most recent file ==="
echo "$LATEST_FILE"
echo ""

# Show file size
FILE_SIZE="$(docker exec "${NAMENODE_CONTAINER}" hdfs dfs -du "${LATEST_FILE}" 2>/dev/null | awk '{print $1}')"
echo "File size: $FILE_SIZE bytes"
echo ""

# Preview file content (first 50 lines)
echo "=== File content (first 50 lines) ==="
docker exec "${NAMENODE_CONTAINER}" hdfs dfs -cat "${LATEST_FILE}" 2>/dev/null | head -50
echo ""

# If file is large, show tail too
if [ "$FILE_SIZE" -gt 1000 ]; then
    echo "=== File content (last 10 lines) ==="
    docker exec "${NAMENODE_CONTAINER}" hdfs dfs -cat "${LATEST_FILE}" 2>/dev/null | tail -10
    echo ""
fi

echo "=========================================="
echo "Commands to use:"
echo "=========================================="
echo ""
echo "1. List all files:"
echo "   docker exec ${NAMENODE_CONTAINER} hdfs dfs -ls -R ${HDFS_PATH}"
echo ""
echo "2. View specific file:"
echo "   docker exec ${NAMENODE_CONTAINER} hdfs dfs -cat ${LATEST_FILE}"
echo ""
echo "3. View first N lines:"
echo "   docker exec ${NAMENODE_CONTAINER} hdfs dfs -cat ${LATEST_FILE} | head -20"
echo ""
echo "4. View last N lines:"
echo "   docker exec ${NAMENODE_CONTAINER} hdfs dfs -cat ${LATEST_FILE} | tail -20"
echo ""
echo "5. Download file to local:"
echo "   docker exec ${NAMENODE_CONTAINER} hdfs dfs -get ${LATEST_FILE} ./local-file.csv"
echo ""
echo "6. Count lines in file:"
echo "   docker exec ${NAMENODE_CONTAINER} hdfs dfs -cat ${LATEST_FILE} | wc -l"
echo ""
