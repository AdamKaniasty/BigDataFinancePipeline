#!/bin/bash

# Script to run all service tests
# Usage: ./tests/service/run-all-tests.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}Running All Service Tests${NC}"
echo -e "${BLUE}==========================================${NC}"
echo ""

PASSED=0
FAILED=0

run_test() {
    local test_name="$1"
    local test_script="$2"
    shift 2

    echo -e "${BLUE}Running: ${test_name}${NC}"
    echo "----------------------------------------"
    
    if bash "$test_script" "$@"; then
        echo -e "${GREEN}✓ PASSED: ${test_name}${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}✗ FAILED: ${test_name}${NC}"
        FAILED=$((FAILED + 1))
    fi
    echo ""
}

# HDFS Tests
echo -e "${YELLOW}=== HDFS Tests ===${NC}"
echo ""

# Note: Some tests require parameters, so we'll skip those or run with defaults
# run_test "Verify CSV Files" "${SCRIPT_DIR}/test-verify-csv-files.sh"
# run_test "Preview HDFS Files" "${SCRIPT_DIR}/test-preview-hdfs-files.sh"

# Kafka Tests
echo -e "${YELLOW}=== Kafka Tests ===${NC}"
echo ""
run_test "Kafka Comprehensive Tests" "${SCRIPT_DIR}/test-kafka.sh"

# Full pipeline (requires data in HDFS; date is auto-detected unless provided)
echo -e "${YELLOW}=== Full Pipeline (Spark + Hive + HBase) ===${NC}"
echo ""
if [[ -n "${FULL_PIPELINE_DATE:-}" ]]; then
    run_test "Full Pipeline (${FULL_PIPELINE_DATE})" "${SCRIPT_DIR}/test-full-pipeline.sh" "${FULL_PIPELINE_DATE}"
else
    run_test "Full Pipeline (auto date)" "${SCRIPT_DIR}/test-full-pipeline.sh"
fi

# Summary
echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}Test Summary${NC}"
echo -e "${BLUE}==========================================${NC}"
echo -e "${GREEN}Passed: ${PASSED}${NC}"
if [ $FAILED -gt 0 ]; then
    echo -e "${RED}Failed: ${FAILED}${NC}"
    exit 1
else
    echo -e "${GREEN}Failed: ${FAILED}${NC}"
    exit 0
fi
