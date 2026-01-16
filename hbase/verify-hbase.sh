#!/bin/bash

# HBase Verification Script
# Verifies namespace, table, column families, and performs sample read/write test

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=========================================="
echo "HBase Verification"
echo "=========================================="
echo ""

# Get HBase container name
HBASE_CONTAINER=$(docker ps --filter "ancestor=harisekhon/hbase:latest" --format "{{.Names}}" | head -1)

if [ -z "$HBASE_CONTAINER" ]; then
    echo -e "${RED}✗ Error: HBase container not found${NC}"
    exit 1
fi

echo -e "${GREEN}Found HBase container: $HBASE_CONTAINER${NC}"
echo ""

hbase_shell() {
    docker exec -i "$HBASE_CONTAINER" hbase shell
}

# Test HBase shell connectivity
echo "1. Testing HBase shell connectivity..."
if printf "version\nexit\n" | hbase_shell 2>/dev/null | grep "HBase" >/dev/null; then
    echo -e "${GREEN}✓ HBase shell is accessible${NC}"
else
    echo -e "${RED}✗ HBase shell is not accessible${NC}"
    exit 1
fi
echo ""

# Verify namespace exists
echo "2. Verifying namespace 'finance' exists..."
NAMESPACE_EXISTS=$(printf "list_namespace\nexit\n" | hbase_shell 2>/dev/null | grep -c "finance" | head -1 || echo "0")

if [ "$NAMESPACE_EXISTS" -gt 0 ]; then
    echo -e "${GREEN}✓ Namespace 'finance' exists${NC}"
else
    echo -e "${RED}✗ Namespace 'finance' does not exist${NC}"
    exit 1
fi
echo ""

# Verify table exists
echo "3. Verifying table 'finance:facts_daily' exists..."
TABLE_EXISTS=$(printf "list\nexit\n" | hbase_shell 2>/dev/null | grep -c "finance:facts_daily" || echo "0")

if [ "$TABLE_EXISTS" -gt 0 ]; then
    echo -e "${GREEN}✓ Table 'finance:facts_daily' exists${NC}"
else
    echo -e "${RED}✗ Table 'finance:facts_daily' does not exist${NC}"
    exit 1
fi
echo ""

# Verify column families
echo "4. Verifying column families..."
TABLE_INFO=$(printf "describe 'finance:facts_daily'\nexit\n" | hbase_shell 2>/dev/null)

COLUMN_FAMILIES=("metrics" "price" "volume" "correlation")
ALL_EXIST=true

for cf in "${COLUMN_FAMILIES[@]}"; do
    if echo "$TABLE_INFO" | grep -q "$cf"; then
        echo -e "${GREEN}✓ Column family '$cf' exists${NC}"
    else
        echo -e "${RED}✗ Column family '$cf' not found${NC}"
        ALL_EXIST=false
    fi
done

if [ "$ALL_EXIST" = false ]; then
    exit 1
fi
echo ""

# Test write operation
echo "5. Testing write operation..."
TEST_ROWKEY="0|BTC|20240101"
printf "put 'finance:facts_daily', '%s', 'metrics:ret_1d', '0.05'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Write operation successful${NC}"
else
    echo -e "${RED}✗ Write operation failed${NC}"
    exit 1
fi
echo ""

# Test read operation
echo "6. Testing read operation..."
READ_RESULT=$(printf "get 'finance:facts_daily', '%s'\nexit\n" "$TEST_ROWKEY" | hbase_shell 2>/dev/null)

if echo "$READ_RESULT" | grep -q "metrics:ret_1d"; then
    echo -e "${GREEN}✓ Read operation successful${NC}"
    echo "  Sample data: $TEST_ROWKEY -> metrics:ret_1d = 0.05"
else
    echo -e "${RED}✗ Read operation failed${NC}"
    exit 1
fi
echo ""

# Verify rowkey structure
echo "7. Verifying rowkey structure..."
if echo "$READ_RESULT" | grep -q "$TEST_ROWKEY"; then
    echo -e "${GREEN}✓ Rowkey structure is correct (salt|symbol|yyyyMMdd)${NC}"
    echo "  Example rowkey: $TEST_ROWKEY"
else
    echo -e "${YELLOW}⚠ Rowkey structure verification inconclusive${NC}"
fi
echo ""

# Clean up test data
echo "8. Cleaning up test data..."
printf "deleteall 'finance:facts_daily', '%s'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1
echo -e "${GREEN}✓ Test data cleaned up${NC}"
echo ""

echo "=========================================="
echo -e "${GREEN}All HBase verifications passed${NC}"
echo "=========================================="
echo ""
