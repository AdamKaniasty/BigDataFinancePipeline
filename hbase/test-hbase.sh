#!/bin/bash

# HBase Testing Script
# Comprehensive tests for HBase operations

set -euo pipefail

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASSED=0
FAILED=0

print_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ PASS: $2${NC}"
        ((PASSED++))
    else
        echo -e "${RED}✗ FAIL: $2${NC}"
        ((FAILED++))
    fi
}

echo "=========================================="
echo "HBase Comprehensive Tests"
echo "=========================================="
echo ""

# Get HBase container name
HBASE_CONTAINER=$(docker ps --filter "ancestor=harisekhon/hbase:latest" --format "{{.Names}}" | head -1)

if [ -z "$HBASE_CONTAINER" ]; then
    echo -e "${RED}✗ Error: HBase container not found${NC}"
    exit 1
fi

echo "Using HBase container: $HBASE_CONTAINER"
echo ""

hbase_shell() {
    docker exec -i "$HBASE_CONTAINER" hbase shell
}

# Test 1: Write operation
echo "Test 1: Write operation"
TEST_ROWKEY="1|BTC|20240101"
printf "put 'finance:facts_daily', '%s', 'metrics:ret_1d', '0.05'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1
print_result $? "Write data to table"

# Test 2: Read operation
echo "Test 2: Read operation"
READ_RESULT=$(printf "get 'finance:facts_daily', '%s'\nexit\n" "$TEST_ROWKEY" | hbase_shell 2>/dev/null)
if echo "$READ_RESULT" | grep -q "metrics:ret_1d"; then
    print_result 0 "Read data from table"
else
    print_result 1 "Read data from table"
fi

# Test 3: Verify rowkey structure
echo "Test 3: Verify rowkey structure"
if echo "$READ_RESULT" | grep -q "$TEST_ROWKEY"; then
    print_result 0 "Rowkey structure (salt|symbol|yyyyMMdd)"
else
    print_result 1 "Rowkey structure (salt|symbol|yyyyMMdd)"
fi

# Test 4: Test column family access - metrics
echo "Test 4: Column family access - metrics"
printf "put 'finance:facts_daily', '%s', 'metrics:vol_21d', '0.15'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1
READ_METRICS=$(printf "get 'finance:facts_daily', '%s', {COLUMN => 'metrics'}\nexit\n" "$TEST_ROWKEY" | hbase_shell 2>/dev/null)
if echo "$READ_METRICS" | grep -q "metrics:"; then
    print_result 0 "Access metrics column family"
else
    print_result 1 "Access metrics column family"
fi

# Test 5: Test column family access - price
echo "Test 5: Column family access - price"
printf "put 'finance:facts_daily', '%s', 'price:avg', '50000'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1
READ_PRICE=$(printf "get 'finance:facts_daily', '%s', {COLUMN => 'price'}\nexit\n" "$TEST_ROWKEY" | hbase_shell 2>/dev/null)
if echo "$READ_PRICE" | grep -q "price:"; then
    print_result 0 "Access price column family"
else
    print_result 1 "Access price column family"
fi

# Test 6: Test column family access - volume
echo "Test 6: Column family access - volume"
printf "put 'finance:facts_daily', '%s', 'volume:sum', '1000000'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1
READ_VOLUME=$(printf "get 'finance:facts_daily', '%s', {COLUMN => 'volume'}\nexit\n" "$TEST_ROWKEY" | hbase_shell 2>/dev/null)
if echo "$READ_VOLUME" | grep -q "volume:"; then
    print_result 0 "Access volume column family"
else
    print_result 1 "Access volume column family"
fi

# Test 7: Test column family access - correlation
echo "Test 7: Column family access - correlation"
printf "put 'finance:facts_daily', '%s', 'correlation:btc_usdpln_63d', '0.75'\nexit\n" "$TEST_ROWKEY" | hbase_shell > /dev/null 2>&1
READ_CORR=$(printf "get 'finance:facts_daily', '%s', {COLUMN => 'correlation'}\nexit\n" "$TEST_ROWKEY" | hbase_shell 2>/dev/null)
if echo "$READ_CORR" | grep -q "correlation:"; then
    print_result 0 "Access correlation column family"
else
    print_result 1 "Access correlation column family"
fi

# Test 8: Multiple rowkeys
echo "Test 8: Multiple rowkeys"
for i in {2..5}; do
    ROWKEY="$i|BTC|2024010$i"
    printf "put 'finance:facts_daily', '%s', 'metrics:ret_1d', '0.0%s'\nexit\n" "$ROWKEY" "$i" | hbase_shell > /dev/null 2>&1
done
COUNT=$(printf "scan 'finance:facts_daily', {LIMIT => 10}\nexit\n" | hbase_shell 2>/dev/null | grep -c "row(s)" || echo "0")
if [ "$COUNT" -gt 0 ]; then
    print_result 0 "Write multiple rowkeys"
else
    print_result 1 "Write multiple rowkeys"
fi

# Clean up test data
echo ""
echo "Cleaning up test data..."
printf "scan 'finance:facts_daily'\nexit\n" | hbase_shell 2>/dev/null | grep -oP 'row\(s\) in \K[0-9]+' | head -1 | while read -r count; do
    if [ ! -z "$count" ] && [ "$count" -gt 0 ]; then
        printf "deleteall 'finance:facts_daily', '1|BTC|20240101'\nexit\n" | hbase_shell > /dev/null 2>&1
        for i in {2..5}; do
            printf "deleteall 'finance:facts_daily', '%s|BTC|2024010%s'\nexit\n" "$i" "$i" | hbase_shell > /dev/null 2>&1
        done
    fi
done
echo -e "${GREEN}✓ Test data cleaned up${NC}"

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed${NC}"
    exit 1
fi
