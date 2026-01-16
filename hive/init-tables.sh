#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HIVE_CONTAINER="big-data-project-hive-1"
HIVE_CLI="/opt/hive/bin/hive"

echo "=========================================="
echo "Initializing Hive schema (finance database)"
echo "Container: ${HIVE_CONTAINER}"
echo "=========================================="
echo ""

# Basic connectivity check
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SHOW DATABASES;" >/dev/null

echo "Applying init_tables.sql ..."
docker exec -i "${HIVE_CONTAINER}" ${HIVE_CLI} -f /dev/stdin < "${SCRIPT_DIR}/init_tables.sql"
echo "Done."
echo ""

echo "Verifying tables in finance database ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SHOW TABLES IN finance;" || true

echo ""
echo "Verifying that fx_daily exists ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SHOW TABLES IN finance LIKE 'fx_daily';"

echo ""
echo "Verifying that trades_agg_hourly exists ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SHOW TABLES IN finance LIKE 'trades_agg_hourly';"

echo ""
echo "Hive initialization complete."
echo "=========================================="

