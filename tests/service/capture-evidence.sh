#!/bin/bash
#
# Capture evidence logs for the end-to-end pipeline run.
# Produces an artifacts bundle under ./artifacts/ suitable for attaching to a report.
#
# Usage:
#   ./tests/service/capture-evidence.sh [YYYY-MM-DD]
#
# If the date is omitted, the script auto-detects the latest date that exists in BOTH:
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
HIVE_CONTAINER_DEFAULT="big-data-project-hive-1"
KAFKA_CONTAINER_DEFAULT="big-data-project-kafka-1"
HBASE_CONTAINER_DEFAULT="big-data-project-hbase-1"

get_container_by_service() {
    local service="$1"
    local fallback="$2"
    local detected
    detected="$(docker ps --filter "label=com.docker.compose.service=${service}" --format "{{.Names}}" | head -n 1)"
    if [[ -n "${detected}" ]]; then
        echo "${detected}"
        return 0
    fi
    echo "${fallback}"
}

detect_latest_common_date() {
    local namenode
    namenode="$(get_container_by_service "hdfs-namenode" "${NAMENODE_CONTAINER_DEFAULT}")"

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

cd "${REPO_ROOT}"

ARTIFACTS_ROOT="${REPO_ROOT}/artifacts"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="${ARTIFACTS_ROOT}/evidence_${RUN_DATE}_${TIMESTAMP}"

mkdir -p "${OUT_DIR}"

NAMENODE_CONTAINER="$(get_container_by_service "hdfs-namenode" "${NAMENODE_CONTAINER_DEFAULT}")"
HIVE_CONTAINER="$(get_container_by_service "hive" "${HIVE_CONTAINER_DEFAULT}")"
KAFKA_CONTAINER="$(get_container_by_service "kafka" "${KAFKA_CONTAINER_DEFAULT}")"
HBASE_CONTAINER="$(get_container_by_service "hbase" "${HBASE_CONTAINER_DEFAULT}")"

cat > "${OUT_DIR}/README.md" <<EOF
# Evidence bundle: end-to-end pipeline (${RUN_DATE})

## Objective
Demonstrate that the full Big Data stack works end-to-end:
NiFi/Kafka ingestion → HDFS raw storage → Spark ETL + analytics → Hive external tables → HBase serving layer.

## Steps
1. Run the end-to-end pipeline script: \`tests/service/test-full-pipeline.sh ${RUN_DATE}\`
2. Capture confirmations from HDFS, Hive, Kafka, NiFi, and HBase.

## Expected Result
- Spark jobs complete successfully and write Parquet outputs under \`/data/finance/curated\` and \`/data/finance/aggregated\`.
- Hive database \`finance\` contains external tables and returns sample rows.
- HBase table \`finance:facts_daily\` exists and \`scan\` returns populated rows.

## Confirmation Files
- \`pipeline.log\`: full pipeline run output
- \`hdfs_raw.log\`, \`hdfs_curated.log\`, \`hdfs_aggregated.log\`: HDFS listings
- \`hive_checks.log\`: Hive table list + sample queries
- \`hbase_checks.log\`: table describe + scan output
- \`kafka_sample.log\`: sample messages from \`crypto-trades\`
- \`nifi_controller_status.json\`: NiFi controller status snapshot
- \`nifi_root_flow.json\`: root flow (process groups list)
- \`nifi_pg_status_*.json\`: process group status snapshots
- \`nifi_processor_puthdfs_crypto.json\`: PutHDFS (Crypto) config snapshot
- \`nifi_connection_nbp_split.json\`: NBP Split Rates connection snapshot
EOF

{
    echo "date=${RUN_DATE}"
    echo "timestamp=${TIMESTAMP}"
    echo "namenode_container=${NAMENODE_CONTAINER}"
    echo "hive_container=${HIVE_CONTAINER}"
    echo "kafka_container=${KAFKA_CONTAINER}"
    echo "hbase_container=${HBASE_CONTAINER}"
    echo ""
    docker --version
    docker compose version 2>/dev/null || true
    echo ""
    docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
} > "${OUT_DIR}/environment.log"

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}Capturing evidence for date: ${RUN_DATE}${NC}"
echo -e "${BLUE}Output directory: ${OUT_DIR}${NC}"
echo -e "${BLUE}==========================================${NC}"

echo -e "${YELLOW}--- Running end-to-end pipeline ---${NC}"
./tests/service/test-full-pipeline.sh "${RUN_DATE}" 2>&1 | tee "${OUT_DIR}/pipeline.log"

echo -e "${YELLOW}--- Capturing HDFS listings ---${NC}"
{
    echo "### Raw (NBP)"
    docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls -R "/data/finance/raw/nbp/date=${RUN_DATE}" || true
    echo ""
    echo "### Raw (Crypto Trades)"
    docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls -R "/data/finance/raw/crypto-trades/date=${RUN_DATE}" || true
} > "${OUT_DIR}/hdfs_raw.log"

{
    echo "### Curated"
    docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls -R "/data/finance/curated" || true
} > "${OUT_DIR}/hdfs_curated.log"

{
    echo "### Aggregated"
    docker exec "${NAMENODE_CONTAINER}" hdfs dfs -ls -R "/data/finance/aggregated" || true
} > "${OUT_DIR}/hdfs_aggregated.log"

echo -e "${YELLOW}--- Capturing Hive checks ---${NC}"
{
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SHOW DATABASES; SHOW TABLES IN finance;" || true
    echo ""
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SELECT * FROM finance.fx_daily LIMIT 5;" || true
    echo ""
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SELECT * FROM finance.trades_agg_hourly LIMIT 5;" || true
    echo ""
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SELECT * FROM finance.fx_daily_returns LIMIT 5;" || true
    echo ""
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SELECT * FROM finance.crypto_daily LIMIT 5;" || true
    echo ""
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SELECT * FROM finance.corr_btc_usdpln_63d LIMIT 5;" || true
    echo ""
    docker exec "${HIVE_CONTAINER}" /opt/hive/bin/hive -e "SELECT * FROM finance.crypto_monthly_facts LIMIT 5;" || true
} > "${OUT_DIR}/hive_checks.log"

echo -e "${YELLOW}--- Capturing HBase checks ---${NC}"
{
    echo "list_namespace"
    echo "list"
    echo "describe 'finance:facts_daily'"
    echo "scan 'finance:facts_daily', {LIMIT => 10}"
} | docker exec -i "${HBASE_CONTAINER}" hbase shell > "${OUT_DIR}/hbase_checks.log" 2>&1

echo -e "${YELLOW}--- Capturing Kafka sample ---${NC}"
docker exec "${KAFKA_CONTAINER}" bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto-trades --from-beginning --max-messages 3" \
    > "${OUT_DIR}/kafka_sample.log" 2>&1 || true

echo -e "${YELLOW}--- Capturing NiFi status ---${NC}"
curl -s -u admin:admin123 http://localhost:8080/nifi-api/flow/status > "${OUT_DIR}/nifi_controller_status.json" || true

OUT_DIR="${OUT_DIR}" python3 - <<'PY' || true
import json
import os
import urllib.request
from base64 import b64encode

out_dir = os.environ["OUT_DIR"]
auth = "Basic " + b64encode(b"admin:admin123").decode()


def fetch(path: str) -> dict:
    req = urllib.request.Request(f"http://localhost:8080/nifi-api{path}")
    req.add_header("Authorization", auth)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def write_json(filename: str, data: dict) -> None:
    with open(os.path.join(out_dir, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


root = fetch("/flow/process-groups/root")
write_json("nifi_root_flow.json", root)

process_groups = root.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
pg_ids = {pg.get("component", {}).get("name"): pg.get("id") for pg in process_groups}

for name in ["NBP", "crypto-flow-kafka-4"]:
    pgid = pg_ids.get(name)
    if not pgid:
        continue
    status = fetch(f"/flow/process-groups/{pgid}/status")
    safe = name.replace("/", "_").replace(" ", "_")
    write_json(f"nifi_pg_status_{safe}.json", status)

crypto_id = pg_ids.get("crypto-flow-kafka-4")
if crypto_id:
    processors = fetch(f"/process-groups/{crypto_id}/processors").get("processors", [])
    for p in processors:
        if p.get("component", {}).get("name") == "PutHDFS (Crypto)":
            pid = p.get("id")
            if pid:
                write_json(
                    "nifi_processor_puthdfs_crypto.json",
                    fetch(f"/processors/{pid}"),
                )
            break

nbp_id = pg_ids.get("NBP")
if nbp_id:
    connections = fetch(f"/process-groups/{nbp_id}/connections").get("connections", [])
    for c in connections:
        comp = c.get("component", {})
        if comp.get("source", {}).get("name") == "Split Rates" and comp.get(
            "destination", {}
        ).get("name") == "Extract Currency Fields":
            cid = c.get("id")
            if cid:
                write_json(
                    "nifi_connection_nbp_split.json",
                    fetch(f"/connections/{cid}"),
                )
            break
PY

echo -e "${GREEN}Evidence captured under:${NC} ${OUT_DIR}"
