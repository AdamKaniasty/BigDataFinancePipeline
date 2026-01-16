#!/bin/bash

# Master HBase Setup Script
# Runs all HBase initialization and verification scripts

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "HBase Complete Setup"
echo "=========================================="
echo ""

# Step 1: Initialize HBase
echo "Step 1: Initializing HBase..."
echo "----------------------------------------"
if "$SCRIPT_DIR/init-hbase.sh"; then
    echo -e "${GREEN}✓ HBase initialization completed${NC}"
else
    echo -e "${RED}✗ HBase initialization failed${NC}"
    exit 1
fi
echo ""

# Step 2: Verify HBase
echo "Step 2: Verifying HBase setup..."
echo "----------------------------------------"
if "$SCRIPT_DIR/verify-hbase.sh"; then
    echo -e "${GREEN}✓ HBase verification completed${NC}"
else
    echo -e "${RED}✗ HBase verification failed${NC}"
    exit 1
fi
echo ""

echo "=========================================="
echo -e "${GREEN}HBase setup completed successfully${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Run tests: ./test-hbase.sh"
echo "2. Configure NiFi HBase integration: ../nifi/configure-hbase.sh"
echo ""

