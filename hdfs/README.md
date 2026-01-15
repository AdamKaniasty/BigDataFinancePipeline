# HDFS Directory Management

## Scripts

### `create-directories.sh`
Creates all required HDFS directories for the finance data pipeline.

**Usage:**
```bash
./hdfs/create-directories.sh
```

**Creates:**
- `/data/finance/raw/crypto-trades` - Crypto raw trades (Binance aggTrade CSV)
- `/data/finance/raw/nbp` - NBP raw data
- `/data/finance/raw/crypto-trades/_errors` - Optional error/DLQ directory (not used in final flow)
- `/data/finance/raw/nbp/_errors` - Optional error/DLQ directory (not used in final flow)
- `/data/finance/raw/crypto-prices` - Legacy crypto ticker snapshots (not used; kept for compatibility)
- `/data/finance/raw/crypto` - Legacy crypto path (not used; kept for compatibility)
- `/user/hive/warehouse` - Hive warehouse
- `/data/finance/curated` - Curated data root
- `/data/finance/aggregated` - Aggregated data root

All directories are created with 777 permissions.

## Manual Commands

### List directories
```bash
docker exec big-data-project-hdfs-namenode-1 hdfs dfs -ls -R /data/finance
```

### Create a single directory
```bash
docker exec big-data-project-hdfs-namenode-1 hdfs dfs -mkdir -p /data/finance/raw/crypto-trades
```

### Set permissions
```bash
docker exec big-data-project-hdfs-namenode-1 hdfs dfs -chmod -R 777 /data/finance/raw/crypto-trades
```

### Remove directory
```bash
docker exec big-data-project-hdfs-namenode-1 hdfs dfs -rm -r /data/finance/raw/crypto-trades
```

### Check if directory exists
```bash
docker exec big-data-project-hdfs-namenode-1 hdfs dfs -test -d /data/finance/raw/crypto-trades && echo "Exists" || echo "Not found"
```

## Directory Structure

```
/data
└── finance
    ├── raw
    │   ├── crypto-trades
    │   │   └── _errors
    │   ├── nbp
    │   │   └── _errors
    │   ├── crypto-prices         (legacy, unused)
    │   └── crypto                (legacy, unused)
    ├── curated
    │   ├── crypto
    │   │   └── trades_agg_hourly
    │   └── nbp
    │       └── fx_daily
    └── aggregated
        └── daily
            ├── crypto_daily
            ├── fx_daily_returns
            └── corr_btc_usdpln_63d

/user
    └── hive
        └── warehouse
```
