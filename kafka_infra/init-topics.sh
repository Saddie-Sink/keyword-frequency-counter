#!/usr/bin/env bash
# Member 2 — Kafka Topic Initialization Script
set -e

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC_NAME="${KAFKA_TOPIC:-text_stream}"
PARTITIONS="${KAFKA_PARTITIONS:-3}"
REPLICATION="${KAFKA_REPLICATION_FACTOR:-1}"

echo "[Kafka Init] Waiting for Kafka broker at ${BOOTSTRAP_SERVERS}..."
for i in {1..30}; do
  if /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --list >/dev/null 2>&1; then
    echo "[Kafka Init] Broker is ready!"
    break
  fi
  echo "[Kafka Init] Attempt ${i}/30: Kafka not ready yet, sleeping 2s..."
  sleep 2
done

echo "[Kafka Init] Ensuring topic '${TOPIC_NAME}' exists..."
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC_NAME}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION}" \
  --config retention.ms=3600000

echo "[Kafka Init] Current Kafka topics:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --describe --topic "${TOPIC_NAME}"
