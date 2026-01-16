-- Initialize Hive database and external tables for the Big Data Finance project.

CREATE DATABASE IF NOT EXISTS finance;

USE finance;

-- Curated NBP FX rates (output of spark/etl_nbp.py)
CREATE EXTERNAL TABLE IF NOT EXISTS fx_daily (
  fx_date    DATE,
  code       STRING,
  currency   STRING,
  mid        DOUBLE,
  load_ts    TIMESTAMP
)
PARTITIONED BY (
  year       INT,
  month      INT
)
STORED AS PARQUET
LOCATION '/data/finance/curated/nbp/fx_daily';

-- Curated hourly crypto aggregates (output of spark/etl_crypto.py)
CREATE EXTERNAL TABLE IF NOT EXISTS trades_agg_hourly (
  symbol        STRING,
  hour_start_ts TIMESTAMP,
  ts_min        TIMESTAMP,
  ts_max        TIMESTAMP,
  price_open    DOUBLE,
  price_close   DOUBLE,
  price_high    DOUBLE,
  price_low     DOUBLE,
  price_avg     DOUBLE,
  price_p95     DOUBLE,
  volume_base   DOUBLE,
  volume_quote  DOUBLE,
  trade_count   BIGINT,
  load_ts       TIMESTAMP
)
PARTITIONED BY (
  `date`       STRING,
  hour         INT
)
STORED AS PARQUET
LOCATION '/data/finance/curated/crypto/trades_agg_hourly';

-- Discover and register partitions based on existing HDFS directories
MSCK REPAIR TABLE fx_daily;
MSCK REPAIR TABLE trades_agg_hourly;

