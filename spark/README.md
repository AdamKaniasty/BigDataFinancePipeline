# Spark Jobs (ETL + Analytics)

This directory contains the PySpark jobs used to build the curated and
aggregated layers, plus the loader that prepares HBase `put` commands.

## Scripts
- `run-etl.sh` runs:
  - `etl_nbp.py` (NBP JSON -> curated `fx_daily`)
  - `etl_crypto.py` (raw crypto CSV -> curated `trades_agg_hourly`)
- `run-analytics.sh` runs `analytics_fx_crypto.py` (daily returns, correlations,
  monthly facts).
- `run-check-analytics.sh` runs `check_analytics_sanity.py` (basic quality checks).

## Inputs (HDFS)
- NBP raw: `/data/finance/raw/nbp/date=<YYYY-MM-DD>/nbp_rate_<CODE>_<YYYYMMDD>.json`
- Crypto raw: `/data/finance/raw/crypto-trades/date=<YYYY-MM-DD>/hour=<HH>/*.csv`

## Outputs (HDFS)
Curated:
- `/data/finance/curated/nbp/fx_daily` (partitioned by `year`, `month`)
- `/data/finance/curated/crypto/trades_agg_hourly` (partitioned by `date`, `hour`)

Aggregated:
- `/data/finance/aggregated/daily/fx_daily_returns`
- `/data/finance/aggregated/daily/crypto_daily`
- `/data/finance/aggregated/daily/corr_btc_usdpln_63d`
- `/data/finance/aggregated/monthly/crypto_monthly_facts`

HBase loader artifacts:
- `load_hbase_facts.py` writes `put` commands to
  `/data/finance/hbase/facts_daily_puts` (consumed by `hbase/load-facts.sh`).

## Examples
```
./spark/run-etl.sh --date 2026-01-05
./spark/run-analytics.sh
./spark/run-check-analytics.sh
```

