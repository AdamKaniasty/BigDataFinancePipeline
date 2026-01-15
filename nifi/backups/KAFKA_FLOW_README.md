# Crypto Trades Flow with Kafka (aggTrade)

This flow adds Kafka as a buffer/decoupling layer between Binance `aggTrade` ingestion and HDFS storage.

## Flow Structure

```
Binance WebSocket → ValidateRecord → UpdateRecord → UpdateAttribute
→ PublishKafkaRecord → [Kafka Topic] → ConsumeKafkaRecord
→ MergeRecord → PutHDFS
```

## Kafka Configuration

- **Topic:** `crypto-trades`
- **Bootstrap Servers (inside Docker network):** `kafka:29092`
- **Consumer Group:** `crypto-trades-consumer-group`
- **Message Key:** `/symbol` (record path, partitions by symbol)
- **Delivery Guarantee:** `acks=1` (leader acknowledgment; at-least-once behavior possible)

## Benefits

1. **Resilience:** Messages are buffered in Kafka if NiFi crashes
2. **Decoupling:** Heavy HDFS writes don't slow down ingestion
3. **Replayability:** Can replay messages from Kafka if needed
4. **Scalability:** Multiple consumers can process from the same topic

## Usage

Import `nifi/backups/crypto-flow-kafka-4.json` in the NiFi UI:

1. Open NiFi: http://localhost:8080
2. Right-click canvas → **Upload Flow**
3. Select `nifi/backups/crypto-flow-kafka-4.json`
4. Enable required controller services
5. Start processors

## Prerequisites

1. Kafka must be running (included in docker-compose.yml)
2. Topic will be auto-created by NiFi if auto-create is enabled
3. Ensure Kafka is accessible at `kafka:29092` from the NiFi container (internal listener). From the host, Kafka is exposed on `localhost:9092`.

## Notes

- This flow is configured for **BTCUSDT only** (`btcusdt@aggTrade`) to keep laptop CPU/storage usage reasonable while producing valid trade volumes and trade counts.
