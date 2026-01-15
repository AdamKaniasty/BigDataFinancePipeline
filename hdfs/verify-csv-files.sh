#!/bin/bash
# Verify CSV files in HDFS and check for empty files

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

NAMENODE_CONTAINER="big-data-project-hdfs-namenode-1"
CRYPTO_PATH="${1:-/data/finance/raw/crypto-trades}"

echo "=========================================="
echo "HDFS CSV Files Verification"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps | grep -q "$NAMENODE_CONTAINER"; then
    echo -e "${RED}✗ Container $NAMENODE_CONTAINER is not running${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Container is running${NC}"
echo ""

# Find all CSV files
echo "Searching for CSV files in $CRYPTO_PATH..."
CSV_FILES=$(docker exec "$NAMENODE_CONTAINER" hdfs dfs -find "$CRYPTO_PATH" -name "*.csv" 2>/dev/null)

if [ -z "$CSV_FILES" ]; then
    echo -e "${YELLOW}⚠ No CSV files found${NC}"
    exit 0
fi

echo "Found $(echo "$CSV_FILES" | wc -l) CSV file(s)"
echo ""

EMPTY_COUNT=0
HAS_CONTENT_COUNT=0

# Check each file
while IFS= read -r file; do
    if [ -z "$file" ]; then
        continue
    fi
    
    echo "=== $file ==="
    
    # Get file size
    SIZE=$(docker exec "$NAMENODE_CONTAINER" hdfs dfs -du "$file" 2>/dev/null | awk '{print $1}')
    
    if [ -z "$SIZE" ] || [ "$SIZE" -eq 0 ]; then
        echo -e "${RED}⚠ EMPTY FILE (size: 0 bytes)${NC}"
        EMPTY_COUNT=$((EMPTY_COUNT + 1))
    else
        echo "Size: $SIZE bytes"
        
        # Try to read content
        CONTENT=$(docker exec "$NAMENODE_CONTAINER" bash -c "hdfs dfs -cat '$file' 2>/dev/null" | head -3)
        
        if [ -z "$CONTENT" ]; then
            echo -e "${RED}⚠ FILE APPEARS EMPTY (cannot read content)${NC}"
            EMPTY_COUNT=$((EMPTY_COUNT + 1))
        else
            echo -e "${GREEN}✓ Has content${NC}"
            echo "First 3 lines:"
            echo "$CONTENT" | sed 's/^/  /'
            HAS_CONTENT_COUNT=$((HAS_CONTENT_COUNT + 1))
        fi
    fi
    echo ""
done <<< "$CSV_FILES"

# Summary
echo "=========================================="
echo "Summary:"
echo "  Files with content: $HAS_CONTENT_COUNT"
if [ "$EMPTY_COUNT" -gt 0 ]; then
    echo -e "  ${RED}Empty files: $EMPTY_COUNT${NC}"
else
    echo -e "  ${GREEN}Empty files: 0${NC}"
fi
echo "=========================================="
