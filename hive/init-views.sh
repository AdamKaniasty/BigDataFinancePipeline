#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HIVE_CONTAINER="big-data-project-hive-1"
HIVE_CLI="/opt/hive/bin/hive"

echo "=========================================="
echo "Initializing Hive analytics tables (finance database)"
echo "Container: ${HIVE_CONTAINER}"
echo "=========================================="
echo ""

# Basic connectivity check
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SHOW DATABASES;" >/dev/null

echo "Applying init_views.sql ..."
docker exec -i "${HIVE_CONTAINER}" ${HIVE_CLI} -f /dev/stdin < "${SCRIPT_DIR}/init_views.sql"
echo "Done."
echo ""

echo "Verifying analytics tables in finance database ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SHOW TABLES IN finance;" || true

echo ""
echo "Checking sample rows from fx_daily_returns ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SELECT * FROM finance.fx_daily_returns LIMIT 5;"

echo ""
echo "Checking sample rows from crypto_daily ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SELECT * FROM finance.crypto_daily LIMIT 5;"

echo ""
echo "Checking sample rows from corr_btc_usdpln_63d ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SELECT * FROM finance.corr_btc_usdpln_63d LIMIT 5;"

echo ""
echo "Checking sample rows from crypto_monthly_facts ..."
docker exec "${HIVE_CONTAINER}" ${HIVE_CLI} -e "SELECT * FROM finance.crypto_monthly_facts LIMIT 5;"

echo ""
echo "Hive analytics initialization complete."
echo "=========================================="

