"""
Member 2 — Kafka & Streaming Infrastructure Package.
Provides Kafka configuration, topic creation, broker health checks, and fault-tolerant connectivity helpers.
"""

from kafka_infra.healthcheck import (
    KafkaInfrastructureManager,
    check_kafka_health,
    ensure_topic_exists,
)

__all__ = [
    "KafkaInfrastructureManager",
    "check_kafka_health",
    "ensure_topic_exists",
]
