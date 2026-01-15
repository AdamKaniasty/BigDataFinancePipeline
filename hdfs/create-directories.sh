#!/bin/bash
# Create all required HDFS directories for the finance data pipeline

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

NAMENODE_CONTAINER="big-data-project-hdfs-namenode-1"

echo "=========================================="
echo "Create HDFS Directories"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps | grep -q "$NAMENODE_CONTAINER"; then
    echo -e "${RED}✗ Error: Container $NAMENODE_CONTAINER is not running${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Container $NAMENODE_CONTAINER is running${NC}"
echo ""

# Function to create directory with error handling
create_dir() {
    local dir_path=$1
    local description=$2
    
    echo -n "Creating $description... "
    
    if docker exec $NAMENODE_CONTAINER hdfs dfs -test -d "$dir_path" 2>/dev/null; then
        echo -e "${YELLOW}already exists${NC}"
    else
        if docker exec $NAMENODE_CONTAINER hdfs dfs -mkdir -p "$dir_path" 2>/dev/null; then
            echo -e "${GREEN}✓${NC}"
        else
            echo -e "${RED}✗ failed${NC}"
            return 1
        fi
    fi
    
    # Set permissions
    docker exec $NAMENODE_CONTAINER hdfs dfs -chmod -R 777 "$dir_path" 2>/dev/null || true
}

# Raw data directories
echo "=== Raw Data Directories ==="
create_dir "/data" "Base data directory"
create_dir "/data/finance" "Finance data directory"
create_dir "/data/finance/raw" "Raw data directory"

# Crypto data directories
echo ""
echo "=== Crypto Data Directories ==="
create_dir "/data/finance/raw/crypto-prices" "Crypto raw data directory (Binance ticker CSV)"
create_dir "/data/finance/raw/crypto-prices/_errors" "Crypto error/dead letter queue directory"

# Crypto trades (Binance aggTrade) raw directory (recommended)
create_dir "/data/finance/raw/crypto-trades" "Crypto raw trades directory (Binance aggTrade CSV)"
create_dir "/data/finance/raw/crypto-trades/_errors" "Crypto trades error/dead letter queue directory"

# Backward-compatible legacy path (kept for older drafts/tools)
create_dir "/data/finance/raw/crypto" "Legacy crypto raw data directory"
create_dir "/data/finance/raw/crypto/_errors" "Legacy crypto error/dead letter queue directory"

# NBP data directories
echo ""
echo "=== NBP Data Directories ==="
create_dir "/data/finance/raw/nbp" "NBP raw data directory"
create_dir "/data/finance/raw/nbp/_errors" "NBP error/dead letter queue directory"

# Hive warehouse directory
echo ""
echo "=== Hive Warehouse Directory ==="
create_dir "/user/hive/warehouse" "Hive warehouse directory"

# Curated/processed data directories (for future use)
echo ""
echo "=== Curated Data Directories ==="
create_dir "/data/finance/curated" "Curated data directory"
create_dir "/data/finance/curated/crypto" "Curated crypto data directory"
create_dir "/data/finance/curated/nbp" "Curated NBP data directory"

# Aggregated data directories (for Spark output)
echo ""
echo "=== Aggregated Data Directories ==="
create_dir "/data/finance/aggregated" "Aggregated data directory"
create_dir "/data/finance/aggregated/hourly" "Hourly aggregations"
create_dir "/data/finance/aggregated/daily" "Daily aggregations"

# Temporary directories
echo ""
echo "=== Temporary Directories ==="
create_dir "/tmp" "Temporary directory"
create_dir "/tmp/finance" "Finance temporary directory"

echo ""
echo "=========================================="
echo -e "${GREEN}✓ Directory creation complete!${NC}"
echo "=========================================="
echo ""

# List created directories
echo "=== Directory Structure ==="
docker exec $NAMENODE_CONTAINER hdfs dfs -ls -R /data/finance 2>/dev/null | head -20
echo ""

echo "=== Verification ==="
echo "Checking key directories:"
for dir in "/data/finance/raw/crypto-trades" "/data/finance/raw/nbp" "/user/hive/warehouse"; do
    if docker exec $NAMENODE_CONTAINER hdfs dfs -test -d "$dir" 2>/dev/null; then
        echo -e "  ${GREEN}✓${NC} $dir"
    else
        echo -e "  ${RED}✗${NC} $dir (missing)"
    fi
done

echo ""
echo "All directories are ready for use!"
