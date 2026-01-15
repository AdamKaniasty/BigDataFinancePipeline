#!/bin/bash

# End-to-end pipeline test:
# NiFi → HDFS → Spark ETL → Hive → Spark analytics → Hive views → HBase
#
# Usage:
#   ./tests/service/test-full-pipeline.sh [YYYY-MM-DD]
#
# If the date is omitted, the script auto-detects the latest date that exists
# in BOTH raw sources in HDFS:
#   - /data/finance/raw/nbp/date=YYYY-MM-DD
#   - /data/finance/raw/crypto-trades/date=YYYY-MM-DD

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}/../.."

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

NAMENODE_CONTAINER_DEFAULT="big-data-project-hdfs-namenode-1"

get_namenode_container() {
    local detected
    detected="$(docker ps --filter "label=com.docker.compose.service=hdfs-namenode" --format "{{.Names}}" | head -n 1)"
    if [[ -n "${detected}" ]]; then
        echo "${detected}"
        return 0
    fi
    echo "${NAMENODE_CONTAINER_DEFAULT}"
}

detect_latest_common_date() {
    local namenode
    namenode="$(get_namenode_container)"

    local nbp_root="/data/finance/raw/nbp"
    local crypto_root="/data/finance/raw/crypto-trades"

    if ! docker exec "${namenode}" hdfs dfs -test -d "${nbp_root}" 2>/dev/null; then
        echo "Error: HDFS path not found: ${nbp_root}" >&2
        return 1
    fi
    if ! docker exec "${namenode}" hdfs dfs -test -d "${crypto_root}" 2>/dev/null; then
        echo "Error: HDFS path not found: ${crypto_root}" >&2
        return 1
    fi

    local nbp_dates crypto_dates common_dates latest
    nbp_dates="$(
        docker exec "${namenode}" hdfs dfs -ls "${nbp_root}" 2>/dev/null \
            | awk '{print $8}' \
            | sed -n 's#.*/date=\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)$#\1#p' \
            | sort -u
    )"
    crypto_dates="$(
        docker exec "${namenode}" hdfs dfs -ls "${crypto_root}" 2>/dev/null \
            | awk '{print $8}' \
            | sed -n 's#.*/date=\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)$#\1#p' \
            | sort -u
    )"

    if [[ -z "${nbp_dates}" ]]; then
        echo "Error: No NBP dates found under ${nbp_root}" >&2
        return 1
    fi
    if [[ -z "${crypto_dates}" ]]; then
        echo "Error: No crypto-trades dates found under ${crypto_root}" >&2
        return 1
    fi

    common_dates="$(comm -12 <(printf "%s\n" "${nbp_dates}") <(printf "%s\n" "${crypto_dates}") || true)"
    latest="$(printf "%s\n" "${common_dates}" | tail -n 1)"

    if [[ -z "${latest}" ]]; then
        echo "Error: No common dates found in raw HDFS inputs." >&2
        echo "NBP dates:" >&2
        while IFS= read -r d; do
            [[ -n "${d}" ]] && printf "  %s\n" "${d}" >&2
        done <<< "${nbp_dates}"
        echo "Crypto-trades dates:" >&2
        while IFS= read -r d; do
            [[ -n "${d}" ]] && printf "  %s\n" "${d}" >&2
        done <<< "${crypto_dates}"
        return 1
    fi

    echo "${latest}"
}

RUN_DATE="${FULL_PIPELINE_DATE:-}"
if [[ $# -gt 1 ]]; then
    echo "Usage: $0 [YYYY-MM-DD]" >&2
    exit 1
fi
if [[ $# -eq 1 ]]; then
    RUN_DATE="$1"
fi
if [[ -z "${RUN_DATE}" ]]; then
    RUN_DATE="$(detect_latest_common_date)"
fi

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}End-to-End Pipeline Test (date: ${RUN_DATE})${NC}"
echo -e "${BLUE}==========================================${NC}"
echo ""

step() {
    echo -e "${YELLOW}--- $1 ---${NC}"
}

echo -e "${GREEN}Using repository root: ${REPO_ROOT}${NC}"
echo ""

cd "${REPO_ROOT}"

step "1. Spark ETL (curated layer)"
./spark/run-etl.sh --date "${RUN_DATE}"
echo ""

step "2. Spark analytics (aggregated layer)"
./spark/run-analytics.sh
echo ""

step "3. Analytics sanity checks"
./spark/run-check-analytics.sh
echo ""

step "4. Hive curated tables"
./hive/init-tables.sh
echo ""

step "5. Hive analytics tables/views"
./hive/init-views.sh
echo ""

step "6. Load facts into HBase"
./hbase/load-facts.sh --date "${RUN_DATE}"
echo ""

echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}Full pipeline completed successfully for ${RUN_DATE}${NC}"
echo -e "${GREEN}Inspect HBase with:${NC}"
echo -e "  ${GREEN}docker exec -it big-data-project-hbase-1 hbase shell${NC}"
echo -e "  ${GREEN}scan 'finance:facts_daily', {LIMIT => 10}${NC}"
echo -e "${GREEN}==========================================${NC}"
