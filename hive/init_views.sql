-- Initialize Hive external tables over Spark analytics outputs.

CREATE DATABASE IF NOT EXISTS finance;

USE finance;

-- FX daily returns (output of spark/analytics_fx_crypto.py)
CREATE EXTERNAL TABLE IF NOT EXISTS fx_daily_returns (
  fx_date  DATE,
  code     STRING,
  mid      DOUBLE,
  ret_1d   DOUBLE
)
PARTITIONED BY (
  year     INT,
  month    INT
)
STORED AS PARQUET
LOCATION '/data/finance/aggregated/daily/fx_daily_returns';

-- Crypto daily closes and returns per symbol
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_daily (
  `date`   DATE,
  ts_max   TIMESTAMP,
  close    DOUBLE,
  ret_1d   DOUBLE
)
PARTITIONED BY (
  symbol   STRING
)
STORED AS PARQUET
LOCATION '/data/finance/aggregated/daily/crypto_daily';

-- 63-day rolling correlation BTCUSDT vs USD/PLN
CREATE EXTERNAL TABLE IF NOT EXISTS corr_btc_usdpln_63d (
  `date`                 DATE,
  corr_btc_usdpln_63d    DOUBLE
)
STORED AS PARQUET
LOCATION '/data/finance/aggregated/daily/corr_btc_usdpln_63d';

-- Monthly crypto facts per symbol
CREATE EXTERNAL TABLE IF NOT EXISTS crypto_monthly_facts (
  year        INT,
  month       INT,
  avg_ret_1d  DOUBLE,
  vol_ret     DOUBLE,
  days_ret    BIGINT,
  last_close  DOUBLE
)
PARTITIONED BY (
  symbol      STRING
)
STORED AS PARQUET
LOCATION '/data/finance/aggregated/monthly/crypto_monthly_facts';

-- Register partitions for partitioned tables
MSCK REPAIR TABLE fx_daily_returns;
MSCK REPAIR TABLE crypto_daily;
MSCK REPAIR TABLE crypto_monthly_facts;

