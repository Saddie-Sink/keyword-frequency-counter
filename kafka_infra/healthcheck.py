"""
Member 2 — Apache Kafka Infrastructure & Health Monitoring.
Manages environment variables:
- KAFKA_BOOTSTRAP_SERVERS
- KAFKA_TOPIC (default: text_stream)
- KAFKA_CONSUMER_GROUP (default: spark_keyword_analytics_group)

Handles failure scenarios:
- Kafka unavailable
- Kafka connection failure
- Topic unavailable (auto-creates `text_stream` when broker is reachable)
- Producer/Consumer connection verification
"""

import logging
import os
import socket
import time
from typing import Dict, Optional

logger = logging.getLogger("kafka_infra")

try:
    from kafka import KafkaAdminClient, KafkaConsumer
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError, TopicAlreadyExistsError
except ImportError:  # pragma: no cover
    KafkaAdminClient = None  # type: ignore
    KafkaConsumer = None  # type: ignore
    NewTopic = None  # type: ignore
    KafkaError = Exception  # type: ignore
    TopicAlreadyExistsError = Exception  # type: ignore


class KafkaInfrastructureManager:
    """
    Encapsulates Kafka connection configuration, topic initialization (`text_stream`),
    and health checking for Member 2's streaming infrastructure.
    """

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.getenv("KAFKA_TOPIC", "text_stream")
        self.consumer_group = consumer_group or os.getenv(
            "KAFKA_CONSUMER_GROUP", "spark_keyword_analytics_group"
        )
        self._last_check_time: float = 0.0
        self._cached_status: Optional[Dict[str, object]] = None

    def _check_tcp_socket(self, timeout: float = 0.5) -> bool:
        """Fast TCP socket probe before attempting heavier Kafka metadata handshake."""
        first_server = self.bootstrap_servers.split(",")[0].strip()
        host, _, port_str = first_server.partition(":")
        probe_host = "127.0.0.1" if host == "localhost" else host
        port = int(port_str) if port_str.isdigit() else 9092
        effective_timeout = 0.05 if probe_host == "127.0.0.1" else timeout
        try:
            with socket.create_connection((probe_host, port), timeout=effective_timeout):
                return True
        except OSError:
            return False

    def ensure_topic(self, num_partitions: int = 3, replication_factor: int = 1) -> bool:
        """Ensure the `text_stream` topic exists on the Kafka broker."""
        if not self._check_tcp_socket() or KafkaAdminClient is None:
            return False

        try:
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers.split(","),
                request_timeout_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
            existing_topics = admin.list_topics()
            if self.topic not in existing_topics:
                topic_spec = NewTopic(
                    name=self.topic,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
                try:
                    admin.create_topics(new_topics=[topic_spec], validate_only=False)
                    logger.info("Created Kafka topic '%s' with %d partitions.", self.topic, num_partitions)
                except TopicAlreadyExistsError:
                    pass
            admin.close()
            return True
        except Exception as exc:
            logger.warning("Unable to ensure Kafka topic '%s': %s", self.topic, exc)
            return False

    def check_health(self, force: bool = False) -> Dict[str, object]:
        """
        Perform health check on Kafka broker and `text_stream` topic.
        Caches result for 3 seconds to avoid excessive socket overhead during high-frequency polling.
        """
        now = time.monotonic()
        if not force and self._cached_status is not None and (now - self._last_check_time) < 3.0:
            return self._cached_status

        self._last_check_time = now
        reachable = self._check_tcp_socket(timeout=1.2)
        topic_available = False
        error_msg: Optional[str] = None

        if reachable and KafkaConsumer is not None:
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    group_id=None,
                    request_timeout_ms=2500,
                    api_version_auto_timeout_ms=2500,
                )
                topics = consumer.topics()
                if self.topic in topics:
                    topic_available = True
                else:
                    topic_available = self.ensure_topic()
                consumer.close()
            except Exception as exc:
                error_msg = f"Kafka metadata check failed: {exc}"
                logger.debug(error_msg)
        elif not reachable:
            error_msg = f"Kafka broker at {self.bootstrap_servers} unreachable over TCP (using local embedded stream bus fallback)"

        self._cached_status = {
            "connected": reachable,
            "mode": "broker" if reachable else "embedded_fallback",
            "bootstrap_servers": self.bootstrap_servers,
            "topic": self.topic,
            "topic_available": topic_available or not reachable,
            "consumer_group": self.consumer_group,
            "error": error_msg,
        }
        return self._cached_status


def ensure_topic_exists(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
) -> bool:
    manager = KafkaInfrastructureManager(bootstrap_servers=bootstrap_servers, topic=topic)
    return manager.ensure_topic()


def check_kafka_health(
    bootstrap_servers: Optional[str] = None,
    topic: Optional[str] = None,
    force: bool = False,
) -> Dict[str, object]:
    manager = KafkaInfrastructureManager(bootstrap_servers=bootstrap_servers, topic=topic)
    return manager.check_health(force=force)
