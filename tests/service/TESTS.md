# Functional Tests

This file documents objectives, steps, expected results, and confirmations for the
integration tests used in the project.

## 1) Kafka Comprehensive Tests
Script: `tests/service/test-kafka.sh`
- Objective: Verify Kafka broker availability, topic operations, produce/consume flow.
- Steps:
  1. Ensure Docker stack is running.
  2. Run `./tests/service/test-kafka.sh`.
- Expected result: All test cases PASS and the script exits 0.
- Confirmation: Console output with PASS lines and final summary.

## 2) End-to-End Pipeline
Script: `tests/service/test-full-pipeline.sh [YYYY-MM-DD]`
- Objective: Validate full flow from raw HDFS data to curated/aggregated outputs and HBase.
- Steps:
  1. Ensure HDFS contains raw NBP and crypto-trades data for the chosen date.
  2. Run `./tests/service/test-full-pipeline.sh YYYY-MM-DD` (or omit date to auto-detect).
- Expected result:
  - Spark ETL completes and writes curated Parquet.
  - Spark analytics completes and writes aggregated Parquet.
  - Hive tables load and sample queries return rows.
  - HBase table loads and `scan` returns rows.
- Confirmation: Script output ends with "Full pipeline completed successfully".

## 3) Analytics Sanity Checks
Script: `spark/run-check-analytics.sh`
- Objective: Validate that aggregated datasets are non-empty and values are reasonable.
- Steps:
  1. Run `./spark/run-analytics.sh` first.
  2. Run `./spark/run-check-analytics.sh`.
- Expected result: All checks pass and exit 0.
- Confirmation: Console output includes "All analytics sanity checks passed."

## 4) Evidence Bundle (Report Attachment)
Script: `tests/service/capture-evidence.sh [YYYY-MM-DD]`
- Objective: Capture logs and snapshots for grading evidence.
- Steps:
  1. Run `./tests/service/capture-evidence.sh YYYY-MM-DD` (the script runs the full pipeline itself).
  2. Collect the `artifacts/evidence_YYYY-MM-DD_*` directory.
- Expected result: Evidence bundle includes HDFS listings, Hive/HBase checks, and NiFi snapshots.
- Confirmation: Presence of `artifacts/evidence_*/README.md` and log files described inside.
