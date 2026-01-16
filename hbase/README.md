# HBase Setup and Configuration

This directory contains scripts and documentation for HBase setup in the Big Data Finance project.

## Overview

HBase is used as a serving layer for fast random reads of financial metrics. The table `finance:facts_daily` stores daily aggregated financial facts with the following structure:

- **Namespace**: `finance`
- **Table**: `facts_daily`
- **Rowkey format**: `salt|symbol|yyyyMMdd` (e.g., `2|BTCUSDT|20260105`)
- **Column families**:
  - `metrics`: ret_1d
  - `price`: avg
  - `volume`: sum, count
  - `correlation`: btc_usdpln_63d (only for BTCUSDT rows when available)

## Scripts

### `init-hbase.sh`

Initializes HBase by creating the namespace and table.

**Usage:**
```bash
./hbase/init-hbase.sh
```

**What it does:**
1. Finds the HBase container
2. Waits for HBase to be ready
3. Creates namespace `finance` (if it doesn't exist)
4. Creates table `finance:facts_daily` with all column families
5. Verifies the table structure

**Requirements:**
- HBase container must be running
- Docker must have access to the HBase container

### `verify-hbase.sh`

Verifies that HBase is properly configured.

**Usage:**
```bash
./hbase/verify-hbase.sh
```

**What it does:**
1. Tests HBase shell connectivity
2. Verifies namespace `finance` exists
3. Verifies table `finance:facts_daily` exists
4. Verifies all column families exist
5. Performs sample write/read test
6. Verifies rowkey structure
7. Cleans up test data

### `test-hbase.sh`

Comprehensive testing script for HBase operations.

**Usage:**
```bash
./hbase/test-hbase.sh
```

**What it does:**
1. Tests write operations
2. Tests read operations
3. Verifies rowkey structure
4. Tests access to all column families (metrics, price, volume, correlation)
5. Tests multiple rowkeys
6. Cleans up test data

## Table Structure

### Rowkey Design

The rowkey follows the pattern: `salt|symbol|yyyyMMdd`

- **salt**: Single digit (0-9) for distribution
- **symbol**: Trading symbol (e.g., BTCUSDT)
- **yyyyMMdd**: Date in format YYYYMMDD

**Examples:**
- `2|BTCUSDT|20260105` - BTCUSDT data for January 5, 2026

### Column Families

#### metrics
- `ret_1d`: 1-day return (may be NULL for short history)

#### price
- `avg`: Average daily price (close from crypto_daily)

#### volume
- `sum`: Sum of volumes
- `count`: Count of transactions

#### correlation
- `btc_usdpln_63d`: 63-day correlation between BTC and USD/PLN (NULL for short history)

## HBase Shell Commands

### Connect to HBase Shell

```bash
docker exec -it <hbase-container> hbase shell
```

### List Namespaces

```bash
list_namespace
```

### List Tables

```bash
list
```

### Describe Table

```bash
describe 'finance:facts_daily'
```

### Write Data

```bash
put 'finance:facts_daily', '0|BTCUSDT|20240101', 'metrics:ret_1d', '0.05'
put 'finance:facts_daily', '0|BTCUSDT|20240101', 'price:avg', '50000'
put 'finance:facts_daily', '0|BTCUSDT|20240101', 'volume:sum', '1000000'
```

### Read Data

```bash
get 'finance:facts_daily', '0|BTCUSDT|20240101'
```

### Read Specific Column Family

```bash
get 'finance:facts_daily', '0|BTCUSDT|20240101', {COLUMN => 'metrics'}
```

### Scan Table

```bash
scan 'finance:facts_daily', {LIMIT => 10}
```

### Delete Data

```bash
delete 'finance:facts_daily', '0|BTCUSDT|20240101', 'metrics:ret_1d'
deleteall 'finance:facts_daily', '0|BTCUSDT|20240101'
```

## Integration with Spark

The current pipeline loads HBase through Spark by generating HBase shell
`put` commands in `spark/load_hbase_facts.py` and streaming them into the
HBase shell via `hbase/load-facts.sh`. This keeps the serving layer
simple and reproducible in the Docker stack.

## Troubleshooting

### HBase Container Not Found

**Problem**: Script cannot find HBase container

**Solution**:
1. Check if HBase is running: `docker compose ps hbase`
2. Start HBase: `docker compose up -d hbase`
3. Wait for HBase to be ready (check logs: `docker compose logs hbase`)

### Table Already Exists

**Problem**: `init-hbase.sh` fails because table exists

**Solution**: The script will automatically drop and recreate the table if it exists.

### Connection Refused

**Problem**: Cannot connect to HBase

**Solution**:
1. Verify HBase is running: `docker compose ps hbase`
2. Check HBase logs: `docker compose logs hbase`
3. Verify ZooKeeper is running (HBase depends on it)

### Permission Denied

**Problem**: Cannot execute HBase shell commands

**Solution**: Ensure scripts have execute permissions: `chmod +x hbase/*.sh`

## Next Steps

After HBase is initialized:

1. Generate HBase puts: `./hbase/load-facts.sh --date YYYY-MM-DD`
2. Test HBase operations: `./hbase/test-hbase.sh`
3. Verify setup: `./hbase/verify-hbase.sh`
