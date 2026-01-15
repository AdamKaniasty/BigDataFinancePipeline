#!/bin/bash
# Clear HDFS directories

set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

CONTAINER_NAME="big-data-project-hdfs-namenode-1"

echo "=========================================="
echo "Clear HDFS Directories"
echo "=========================================="
echo ""

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${RED}✗ Container $CONTAINER_NAME is not running${NC}"
    exit 1
fi

# Function to clear directory
clear_directory() {
    local path=$1
    local description=$2
    
    echo "Checking: $path"
    if docker exec "$CONTAINER_NAME" hdfs dfs -test -d "$path" 2>/dev/null; then
        echo -e "${YELLOW}Found: $description${NC}"
        echo "  Path: $path"
        
        # Get size before deletion
        SIZE=$(docker exec "$CONTAINER_NAME" hdfs dfs -du -s -h "$path" 2>/dev/null | awk '{print $1 " " $2}' || echo "unknown")
        echo "  Size: $SIZE"
        
        read -p "  Delete this directory? (yes/no): " confirm
        if [ "$confirm" = "yes" ]; then
            docker exec "$CONTAINER_NAME" hdfs dfs -rm -r -skipTrash "$path" 2>/dev/null
            if [ $? -eq 0 ]; then
                echo -e "${GREEN}✓ Deleted: $path${NC}"
            else
                echo -e "${RED}✗ Failed to delete: $path${NC}"
            fi
        else
            echo "  Skipped"
        fi
        echo ""
    else
        echo "  Directory does not exist"
        echo ""
    fi
}

# Menu
echo "What would you like to clear?"
echo ""
echo "1. Crypto trades data only (/data/finance/raw/crypto-trades)"
echo "2. NBP data only (/data/finance/raw/nbp)"
echo "3. All raw data (/data/finance/raw)"
echo "4. All finance data (/data/finance)"
echo "5. Custom path"
echo "6. Cancel"
echo ""
read -p "Select option (1-6): " option

case $option in
    1)
        clear_directory "/data/finance/raw/crypto-trades" "Crypto trades data"
        ;;
    2)
        clear_directory "/data/finance/raw/nbp" "NBP data"
        ;;
    3)
        clear_directory "/data/finance/raw" "All raw data"
        ;;
    4)
        clear_directory "/data/finance" "All finance data"
        ;;
    5)
        read -p "Enter HDFS path to clear: " custom_path
        if [ ! -z "$custom_path" ]; then
            clear_directory "$custom_path" "Custom path"
        else
            echo -e "${RED}✗ No path provided${NC}"
            exit 1
        fi
        ;;
    6)
        echo "Cancelled"
        exit 0
        ;;
    *)
        echo -e "${RED}✗ Invalid option${NC}"
        exit 1
        ;;
esac

echo "=========================================="
echo -e "${GREEN}✓ Done!${NC}"
echo "=========================================="
