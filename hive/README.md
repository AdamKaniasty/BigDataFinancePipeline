# Hive Schema (External Tables)

Hive is used as the SQL/catalog layer over the Parquet datasets produced by
Spark. All tables live in the `finance` database and point to HDFS paths.

## Files
- `init_tables.sql` defines external tables for the curated layer:
  - `finance.fx_daily`
  - `finance.trades_agg_hourly`
- `init_views.sql` defines external tables for the aggregated layer:
  - `finance.fx_daily_returns`
  - `finance.crypto_daily`
  - `finance.corr_btc_usdpln_63d`
  - `finance.crypto_monthly_facts`
- `init-tables.sh` creates the database/tables and runs `MSCK REPAIR TABLE`.
- `init-views.sh` creates aggregated tables and runs `MSCK REPAIR TABLE`.

## Usage
```
./hive/init-tables.sh
./hive/init-views.sh
```

## Notes
- Hive is a catalog and SQL access layer; heavy analytics are done in Spark.
- `MSCK REPAIR TABLE` registers partitions created by Spark ETL.
- In this lightweight stack, large `COUNT(*)` or `GROUP BY` queries may be slow
  or unreliable; use them only for small demo samples.
