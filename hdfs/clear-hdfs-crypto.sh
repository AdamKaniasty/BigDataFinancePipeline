#!/bin/bash
# Clear HDFS crypto data (non-interactive)

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

CONTAINER_NAME="big-data-project-hdfs-namenode-1"
CRYPTO_PATH="${1:-/data/finance/raw/crypto-trades}"

echo "=========================================="
echo "Clear HDFS Crypto Data"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${RED}✗ Container $CONTAINER_NAME is not running${NC}"
    exit 1
fi

# Check if path exists
if docker exec "$CONTAINER_NAME" hdfs dfs -test -d "$CRYPTO_PATH" 2>/dev/null; then
    echo "Path: $CRYPTO_PATH"
    
    # Get size before deletion
    SIZE=$(docker exec "$CONTAINER_NAME" hdfs dfs -du -s -h "$CRYPTO_PATH" 2>/dev/null | awk '{print $1 " " $2}' || echo "unknown")
    echo "Size: $SIZE"
    echo ""
    
    # Delete
    echo "Deleting..."
    docker exec "$CONTAINER_NAME" hdfs dfs -rm -r -skipTrash "$CRYPTO_PATH" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Deleted: $CRYPTO_PATH${NC}"
    else
        echo -e "${RED}✗ Failed to delete: $CRYPTO_PATH${NC}"
        exit 1
    fi
else
    echo "Directory does not exist: $CRYPTO_PATH"
    echo -e "${YELLOW}Nothing to delete${NC}"
fi

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Done!${NC}"
echo "=========================================="
