# Big Data Finance Project

A comprehensive Big Data pipeline for ingesting, processing, and storing financial data including cryptocurrency prices and Polish National Bank (NBP) exchange rates.

## Overview

This project implements a data pipeline architecture that:
- **Ingests** real-time cryptocurrency trade events from Binance WebSocket API (aggTrade)
- **Ingests** daily exchange rates from the Polish National Bank (NBP) API
- **Optionally buffers** the crypto stream through Kafka for resilience and decoupling
- **Stores** raw data in HDFS with date/hour partitioning
- **Processes** data using Apache Spark
- **Queries** data using Apache Hive
- **Serves** selected facts through Apache HBase for fast random reads

## Data Sources

### Binance WebSocket

- **Endpoint**: `wss://stream.binance.com:9443/stream`
- **Streams**: `btcusdt@aggTrade`
- **Update Frequency**: Real-time (trade events)

### NBP API

- **Endpoint**: `http://api.nbp.pl/api/exchangerates/tables/A/`
- **Update Frequency**: Daily (business days)
- **Format**: JSON

## Architecture

```
Binance WS ──▶ NiFi ──▶ (Kafka optional) ──▶ HDFS ──▶ Spark/Hive ──▶ HBase
NBP API   ─────────────────────────────────────────────▶ HDFS
```

### Components

- **Apache NiFi** (1.27.0): Data orchestration and flow management
- **Apache Kafka** (7.5.0): Message broker for data buffering
- **Apache HDFS**: Distributed file system for data storage
- **Apache Hive** (2.3.2 via `bde2020/hive`): SQL catalog over Parquet data in HDFS
- **Apache Spark** (3.5.1): Data processing engine
- **Apache HBase** (2.1.3 via `harisekhon/hbase`): Serving layer for fast random reads
- **Zookeeper**: Coordination service for Kafka and HBase

## Prerequisites

- **Docker** and **Docker Compose** installed
- **x86_64 or ARM64** (use `docker-compose-arm.yml` on Apple Silicon; some services run under amd64 emulation)
- **Python 3** (for helper scripts)
- **8GB+ RAM** recommended
- **10GB+ free disk space** for Docker volumes

## Quick Start

### 1. Clone and Navigate

```bash
cd BigDataProject
```

### 2. Start All Services

```bash
# Default (recommended on x86_64 Linux/Windows)
docker compose -f docker-compose.yml up -d

# Apple Silicon / ARM64 variant
docker compose -f docker-compose-arm.yml up -d
```

This will start:
- Zookeeper (port 2181)
- Kafka (port 9092)
- HDFS NameNode (ports 9870, 9000)
- HDFS DataNode (port 9864)
- Hive Metastore (port 9083)
- HiveServer2 (port 10000)
- Spark (interactive)
- NiFi (port 8080)

### 3. Wait for Services to Start

Wait 1-2 minutes for all services to initialize. Check status:

```bash
docker compose -f docker-compose.yml ps
# (or: docker compose -f docker-compose-arm.yml ps)
```

### 4. Create HDFS Directories

```bash
./hdfs/create-directories.sh
```

This creates the required directory structure:
- `/data/finance/raw/crypto-trades/` - Crypto trade events (aggTrade)
- `/data/finance/raw/nbp/` - NBP exchange rates
- `/user/hive/warehouse/` - Hive warehouse

### 5. Access Services

- **NiFi UI**: http://localhost:8080 (username: `admin`, password: `admin123`)
- **HDFS NameNode UI**: http://localhost:9870
- **HiveServer2**: `localhost:10000`
- **Kafka**: `localhost:9092`

## Data Flows

### Crypto Price Flow

1. **Binance WebSocket** → Connects to Binance `aggTrade` stream (BTCUSDT)
2. **ValidateRecord** → Validates incoming JSON records
3. **UpdateRecord** → Transforms and enriches data
4. **UpdateAttribute** → Adds partitioning attributes (date, hour, minute)
5. **PublishKafkaRecord** → Publishes to Kafka topic `crypto-trades` (Kafka variant)
6. **ConsumeKafkaRecord** → Consumes from Kafka (Kafka variant)
7. **MergeRecord** → Merges records by minute
8. **PutHDFS** → Writes to HDFS: `/data/finance/raw/crypto-trades/date={date}/hour={hour}/{day}-{hour}-{minute}.csv`

The direct (no-Kafka) flow skips steps 5–6 and writes to HDFS after `MergeRecord`.

**Flow Files:**
- `nifi/backups/crypto-flow.json` - Direct flow (no Kafka)
- `nifi/backups/crypto-flow-kafka-4.json` - Flow with Kafka buffer

### NBP Exchange Rates Flow

1. **InvokeHTTP** → Fetches daily rates from `http://api.nbp.pl/api/exchangerates/tables/A/`
2. **Extract Metadata** → Extracts table metadata (date, table number)
3. **SplitJson** → Splits rates array into individual records
4. **Extract Currency Fields** → Extracts currency, code, mid (rate)
5. **UpdateAttribute** → Sets date and filename
6. **PutHDFS** → Writes to HDFS: `/data/finance/raw/nbp/date={date}/nbp_rate_{CODE}_{DATE}.json`

**Flow File:**
- `nifi/backups/NBP.json`

## Loading NiFi Flows

### Option 1: Using NiFi UI

1. Open NiFi UI: http://localhost:8080
2. Right-click on canvas → **Upload Flow**
3. Select flow JSON file from `nifi/backups/`
4. Enable controller services
5. Start processors

### Option 2: Using REST API (Manual)

The flows can be loaded programmatically using the NiFi REST API. See individual flow files for details.

## Data Structure

### Crypto Trades (CSV, aggTrade)

Stored in: `/data/finance/raw/crypto-trades/date={YYYY-MM-DD}/hour={HH}/{day}-{hour}-{minute}.csv`

**Columns:**
The raw CSV preserves the Binance `aggTrade` payload (e.g., `e`, `E`, `s`, `a`, `p`, `q`, `T`) and additional fields produced by NiFi. The ETL uses the following key fields:
- `symbol` - Trading pair (`BTCUSDT`)
- `price` - Trade price
- `quantity` - Trade quantity (base asset)
- `event_time` - Trade timestamp (epoch milliseconds)

### NBP Exchange Rates (JSON)

Stored in: `/data/finance/raw/nbp/date={YYYY-MM-DD}/nbp_rate_{CODE}_{DATE}.json`

**Structure:**
```json
{
  "currency": "dolar amerykański",
  "code": "USD",
  "mid": 3.6868
}
```

## Testing

Run all service tests:

```bash
./tests/service/run-all-tests.sh
```

End-to-end pipeline (including Hive and HBase):

```bash
./tests/service/test-full-pipeline.sh [YYYY-MM-DD]
# If omitted, the script auto-detects the latest date present in both raw sources in HDFS.
```

Generate an evidence bundle (logs + confirmations) for attaching to the report:

```bash
./tests/service/capture-evidence.sh [YYYY-MM-DD]
# Outputs files under ./artifacts/
```

Individual tests:

```bash
# Kafka tests
./tests/service/test-kafka.sh

# HDFS data verification
./tests/service/test-nbp-rates.sh 2026-01-05
./tests/service/test-crypto-trades.sh 2026-01-05 12
./tests/service/test-verify-csv-files.sh
```

## Useful Commands

### HDFS Operations

```bash
# List files
docker compose -f docker-compose.yml exec hdfs-namenode hdfs dfs -ls -R /data/finance/raw
# (or: docker compose -f docker-compose-arm.yml exec hdfs-namenode hdfs dfs -ls -R /data/finance/raw)

# Read crypto prices
./hdfs/read-crypto-trades.sh 2026-01-05 12

# Read NBP rates
./hdfs/read-nbp-rates.sh 2026-01-05

# Clear data (careful!)
./hdfs/clear-hdfs-crypto.sh
./hdfs/clear-hdfs.sh
```

### Kafka Operations

```bash
# List topics
docker compose -f docker-compose.yml exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker compose -f docker-compose.yml exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-trades \
  --from-beginning
```

### NiFi Operations

```bash
# Check NiFi status
curl -u admin:admin123 http://localhost:8080/nifi-api/flow/status

# List process groups
curl -u admin:admin123 http://localhost:8080/nifi-api/flow/process-groups/root
```

### Full Pipeline Demo

To run the full batch path NiFi → HDFS → Spark ETL → Hive → Spark analytics → Hive views → HBase for a given date:

```bash
./spark/run-etl.sh --date YYYY-MM-DD
./spark/run-analytics.sh
./spark/run-check-analytics.sh
./hive/init-tables.sh
./hive/init-views.sh
./hbase/load-facts.sh --date YYYY-MM-DD
```

Or use the single end-to-end wrapper:

```bash
./tests/service/test-full-pipeline.sh YYYY-MM-DD
```

Example (2026‑01‑05):

```bash
./tests/service/test-full-pipeline.sh 2026-01-05
# ...
# Full pipeline completed successfully for 2026-01-05
```

Then inspect HBase:

```bash
docker compose -f docker-compose.yml exec hbase hbase shell
scan 'finance:facts_daily', {LIMIT => 10}
```

## Troubleshooting

### Services Not Starting

1. Check Docker logs:
   ```bash
   docker compose -f docker-compose.yml logs [service-name]
   ```

2. Check container status:
   ```bash
   docker compose -f docker-compose.yml ps
   ```

3. Restart a service:
   ```bash
   docker compose -f docker-compose.yml restart [service-name]
   ```

### HDFS Connection Issues

1. Verify NameNode is running:
   ```bash
   docker compose -f docker-compose.yml exec hdfs-namenode hdfs dfsadmin -report
   ```

2. Check NiFi core-site.xml is mounted:
   ```bash
   docker compose -f docker-compose.yml exec nifi cat /opt/nifi/nifi-current/conf/core-site.xml
   ```

### Kafka Connection Issues

1. Verify Kafka is accessible:
   ```bash
   docker compose -f docker-compose.yml exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

2. Check topic exists:
   ```bash
   docker compose -f docker-compose.yml exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

### NiFi Flow Issues

1. Check processor errors in NiFi UI
2. Verify controller services are enabled
3. Check processor relationships are connected
4. Review NiFi logs:
   ```bash
   docker compose -f docker-compose.yml logs nifi
   ```

## Project Structure

```
BigDataProject/
├── docker-compose.yml        # Default Docker Compose configuration
├── docker-compose-arm.yml    # Apple Silicon / ARM64 variant
├── nifi/
│   ├── backups/              # NiFi flow definitions
│   ├── core-site.xml         # HDFS configuration for NiFi
│   └── libs/                 # Additional libraries (java-websocket.jar)
├── spark/                    # Spark ETL + analytics jobs
├── hive/                     # Hive DDL + init scripts
├── hbase/                    # HBase setup/load scripts
├── hdfs/
│   ├── create-directories.sh # HDFS directory setup
│   ├── read-*.sh             # Data reading scripts
│   └── clear-*.sh            # Data cleanup scripts
├── kafka/
│   └── test-kafka.sh         # Kafka testing
├── tests/
│   └── service/              # Service test scripts
├── raport/                   # Project report (LaTeX)
└── README.md                 # This file
```

## Configuration Details

### NiFi Configuration

- **HDFS Connection**: Configured via `nifi/core-site.xml`
- **WebSocket Library**: `java-websocket.jar` mounted to NiFi lib directory
- **Credentials**: admin/admin123

### Kafka Configuration

- **Bootstrap Server**: `kafka:29092` (internal), `localhost:9092` (external)
- **Auto-create Topics**: Enabled
- **Replication Factor**: 1 (single broker setup)

### HDFS Configuration

- **Default FS**: `hdfs://hdfs-namenode:9000`
- **Replication Factor**: 1
- **Web UI**: http://localhost:9870

### Hive & Spark Execution

- Hive is used as the external schema/catalog over curated and analytics Parquet tables and for light `SELECT ... LIMIT` queries.
- Hive runs in local MapReduce mode in this stack; larger scans like `SELECT COUNT(*)` can be slow, so heavier analytics are handled in Spark.
- All non-trivial computation and validation (ETL, analytics, sanity checks) are done in Spark using `spark-submit`.
