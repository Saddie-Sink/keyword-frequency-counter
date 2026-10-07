# Member 2 — Apache Kafka & Streaming Infrastructure

## Overview

This module configures and maintains **Apache Kafka** as the high-throughput, fault-tolerant real-time ingestion layer between the **Python Data Producer (Member 1)** and **Apache Spark Structured Streaming (Member 3)**.

## Architecture

```text
Python Producer (Member 1)
      ↓
Kafka Producer (JSON serialized messages)
      ↓
Kafka Topic: text_stream (3 Partitions, KRaft Mode)
      ↓
Spark Structured Streaming (Member 3)
```

## Environment Variables

| Variable                  | Default Value                    | Description                                          |
| ------------------------- | -------------------------------- | ---------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` (`localhost:9092`)  | Comma-separated host:port pairs for Kafka brokers    |
| `KAFKA_TOPIC`             | `text_stream`                    | Primary topic for streaming text ingestion           |
| `KAFKA_CONSUMER_GROUP`    | `spark_keyword_analytics_group`  | Consumer group ID for streaming analytics consumers  |

## Message Schema (`text_stream`)

Messages published to `text_stream` use category keys and UTF-8 JSON payloads:

```json
{
  "id": "e8b7a51c-91f0-4a7d-8f8a-2b5c6018e991",
  "sequence_id": 42,
  "text": "Spark Structured Streaming processes large data streams with fault-tolerant checkpointing.",
  "category": "Big Data",
  "timestamp": "2026-09-30T12:58:41.123456+00:00"
}
```

## Fault Handling

1. **Kafka Broker Unavailable**: `KafkaInfrastructureManager` performs fast non-blocking TCP probes and metadata checks, preventing producer or API hangs.
2. **Topic Unavailable**: Automatically creates `text_stream` with 3 partitions via `init-topics.sh` on container startup and via `KafkaAdminClient` in `kafka_infra/healthcheck.py`.
3. **Producer / Consumer Reconnection**: Configured with controlled retries, timeouts (`request_timeout_ms=5000`), and automatic reconnection when the broker recovers.
