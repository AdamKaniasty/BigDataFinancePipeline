#!/bin/bash

# HBase Initialization Script
# Creates namespace 'finance' and table 'finance:facts_daily' with column families

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "HBase Initialization"
echo "=========================================="
echo ""

# Get HBase container name
HBASE_CONTAINER=$(docker ps --filter "ancestor=harisekhon/hbase:latest" --format "{{.Names}}" | head -1)

if [ -z "$HBASE_CONTAINER" ]; then
    echo -e "${RED}Error: HBase container not found${NC}"
    exit 1
fi

echo -e "${GREEN}Found HBase container: $HBASE_CONTAINER${NC}"
echo ""

# NOTE: Avoid `bash -c` inside the container. In this image, `hbase` is on PATH
# for direct exec, but not necessarily for login shells, which breaks scripts.
hbase_shell() {
    docker exec -i "$HBASE_CONTAINER" hbase shell
}

# Wait for HBase to be ready
echo "Waiting for HBase to be ready..."
for i in {1..30}; do
    if printf "list\nexit\n" | hbase_shell 2>/dev/null | grep "TABLE" >/dev/null; then
        echo -e "${GREEN}HBase is ready${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}Error: HBase is not ready after 30 attempts${NC}"
        exit 1
    fi
    sleep 2
done
echo ""

# Create namespace 'finance'
echo "Creating namespace 'finance'..."
NAMESPACE_CHECK=$(printf "list_namespace\nexit\n" | hbase_shell 2>/dev/null | grep -E "^finance|^NAMESPACE" | head -1)

if echo "$NAMESPACE_CHECK" | grep -q "finance"; then
    echo -e "${GREEN}✓ Namespace 'finance' already exists${NC}"
else
    printf "create_namespace 'finance'\nexit\n" | hbase_shell > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Namespace 'finance' created${NC}"
    else
        # Check again if it was created
        NAMESPACE_CHECK2=$(printf "list_namespace\nexit\n" | hbase_shell 2>/dev/null | grep "finance" || true)
        if [ ! -z "$NAMESPACE_CHECK2" ]; then
            echo -e "${GREEN}✓ Namespace 'finance' created${NC}"
        else
            echo -e "${YELLOW}⚠ Namespace creation may have failed${NC}"
        fi
    fi
fi
echo ""

# Check if table exists
echo "Checking if table 'finance:facts_daily' exists..."
TABLE_EXISTS=$(printf "list\nexit\n" | hbase_shell 2>/dev/null | grep -c "finance:facts_daily" || true)

if [ "${TABLE_EXISTS:-0}" -gt 0 ]; then
    echo -e "${YELLOW}Table 'finance:facts_daily' already exists${NC}"
    echo "Dropping existing table..."
    cat <<'EOF' | hbase_shell 2>&1
disable 'finance:facts_daily'
drop 'finance:facts_daily'
exit
EOF
    echo -e "${GREEN}✓ Existing table dropped${NC}"
    echo ""
fi

# Create table with column families
echo "Creating table 'finance:facts_daily' with column families..."
# Use heredoc to properly format HBase shell command
cat <<'EOF' | hbase_shell 2>&1
create 'finance:facts_daily', 'metrics', 'price', 'volume', 'correlation'
exit
EOF

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Table 'finance:facts_daily' created${NC}"
else
    echo -e "${RED}✗ Failed to create table${NC}"
    exit 1
fi
echo ""

# Verify table structure
echo "Verifying table structure..."
TABLE_INFO=$(printf "describe 'finance:facts_daily'\nexit\n" | hbase_shell 2>/dev/null)

if echo "$TABLE_INFO" | grep -q "metrics"; then
    echo -e "${GREEN}✓ Column family 'metrics' exists${NC}"
else
    echo -e "${RED}✗ Column family 'metrics' not found${NC}"
fi

if echo "$TABLE_INFO" | grep -q "price"; then
    echo -e "${GREEN}✓ Column family 'price' exists${NC}"
else
    echo -e "${RED}✗ Column family 'price' not found${NC}"
fi

if echo "$TABLE_INFO" | grep -q "volume"; then
    echo -e "${GREEN}✓ Column family 'volume' exists${NC}"
else
    echo -e "${RED}✗ Column family 'volume' not found${NC}"
fi

if echo "$TABLE_INFO" | grep -q "correlation"; then
    echo -e "${GREEN}✓ Column family 'correlation' exists${NC}"
else
    echo -e "${RED}✗ Column family 'correlation' not found${NC}"
fi
echo ""

echo "=========================================="
echo -e "${GREEN}HBase initialization completed successfully${NC}"
echo "=========================================="
echo ""
echo "Table structure:"
echo "  - Namespace: finance"
echo "  - Table: facts_daily"
echo "  - Column families: metrics, price, volume, correlation"
echo "  - Rowkey format: salt|symbol|yyyyMMdd"
echo ""
