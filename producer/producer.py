"""
Member 1 — Streaming Data Producer & Background Task Manager.
Publishes realistic text messages to Apache Kafka topic `text_stream` at a controlled
rate, managed as a non-blocking background asyncio task with full fault handling.
"""

import asyncio
import json
import logging
import time
from typing import Awaitable, Callable, Dict, List, Optional

from producer.config import ProducerConfig
from producer.lifecycle import StreamLifecycleManager, StreamState
from producer.sample_datasets import generate_message

logger = logging.getLogger("producer")

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:  # pragma: no cover
    KafkaProducer = None  # type: ignore
    KafkaError = Exception  # type: ignore


MessageCallback = Callable[[Dict[str, object]], Awaitable[None]]


class StreamingDataProducer:
    """
    Managed Kafka streaming data producer.
    - Never blocks FastAPI request handlers with an uncontrolled `while True:`
    - Runs in a managed background `asyncio.Task`
    - Publishes JSON payloads to Kafka topic `text_stream`
    - Supports START, PAUSE, RESUME, STOP, and restarting cleanly
    """

    def __init__(self, config: Optional[ProducerConfig] = None) -> None:
        self.config = config or ProducerConfig()
        self.lifecycle = StreamLifecycleManager(
            initial_rate=self.config.messages_per_second,
            initial_category=self.config.category,
        )
        self._kafka_producer: Optional[ object ] = None
        self._kafka_connected: bool = False
        self._last_error: Optional[str] = None
        self._bg_task: Optional[asyncio.Task] = None
        self._stop_event: asyncio.Event = asyncio.Event()
        self._callbacks: List[MessageCallback] = []
        self._last_connect_attempt: float = 0.0

    def register_callback(self, callback: MessageCallback) -> None:
        """Register an async listener invoked whenever a message is produced."""
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    @property
    def kafka_connected(self) -> bool:
        return self._kafka_connected

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def _connect_kafka_sync(self) -> bool:
        """Attempt to initialize the KafkaProducer connection synchronously."""
        if KafkaProducer is None:
            self._kafka_connected = False
            self._last_error = "kafka-python library not installed"
            return False

        now = time.monotonic()
        if self._kafka_producer is not None and self._kafka_connected:
            return True

        # Throttle reconnection attempts to avoid tight retry loops when broker is down
        if now - self._last_connect_attempt < 3.0 and self._last_connect_attempt > 0:
            return self._kafka_connected

        self._last_connect_attempt = now
        try:
            import socket

            first_server = self.config.bootstrap_servers.split(",")[0].strip()
            host, _, port_str = first_server.partition(":")
            probe_host = "127.0.0.1" if host == "localhost" else host
            port = int(port_str) if port_str.isdigit() else 9092
            timeout_sec = 0.05 if probe_host in ("127.0.0.1", "::1") else 0.5
            with socket.create_connection((probe_host, port), timeout=timeout_sec):
                pass

            self._kafka_producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                request_timeout_ms=self.config.request_timeout_ms,
                max_block_ms=2000,
                retries=self.config.retries,
                acks=1,
            )
            self._kafka_connected = True
            self._last_error = None
            logger.info(
                "KafkaProducer connected to bootstrap_servers=%s, topic=%s",
                self.config.bootstrap_servers,
                self.config.topic,
            )
            return True
        except Exception as exc:
            self._kafka_connected = False
            self._last_error = str(exc)
            logger.warning(
                "Kafka broker at %s not reachable (%s); operating with resilient local fallback until broker is online.",
                self.config.bootstrap_servers,
                exc,
            )
            return False

    def publish_to_kafka(self, message: Dict[str, object]) -> bool:
        """
        Publish a single message dictionary to Kafka topic `text_stream`.
        Returns True if acknowledged by Kafka broker, False if broker was unreachable.
        """
        if not self._kafka_connected or self._kafka_producer is None:
            self._connect_kafka_sync()

        if self._kafka_producer is not None and self._kafka_connected:
            try:
                future = self._kafka_producer.send(
                    self.config.topic,
                    key=str(message.get("category", "general")),
                    value=message,
                )
                future.get(timeout=2.5)
                return True
            except Exception as exc:
                self._kafka_connected = False
                self._last_error = str(exc)
                logger.warning("Failed to publish message to Kafka topic '%s': %s", self.config.topic, exc)
                try:
                    self._kafka_producer.close(timeout=1.0)
                except Exception:
                    pass
                self._kafka_producer = None
                return False
        return False

    async def _run_loop(self) -> None:
        """
        Background async loop that produces messages while state is RUNNING,
        idles while PAUSED, and exits cleanly when STOPPED.
        """
        logger.info("Producer background task started.")
        loop = asyncio.get_running_loop()

        try:
            while not self._stop_event.is_set():
                current_state = self.lifecycle.state
                if current_state == StreamState.STOPPED:
                    break

                if current_state == StreamState.PAUSED:
                    await asyncio.sleep(0.15)
                    continue

                # State is RUNNING
                start_tick = time.perf_counter()
                rate = self.lifecycle.rate
                category = self.lifecycle.category

                seq_id = self.lifecycle.record_message_sent()
                message = generate_message(category=category, sequence_id=seq_id)

                # Publish to Kafka in executor so socket I/O never blocks event loop
                published_to_kafka = await loop.run_in_executor(
                    None, self.publish_to_kafka, message
                )
                message["kafka_published"] = published_to_kafka

                # Notify registered callbacks (e.g., WebSocket broadcaster / fallback stream processor)
                for cb in list(self._callbacks):
                    try:
                        await cb(message)
                    except Exception as cb_exc:
                        logger.error("Error in producer message callback: %s", cb_exc)

                # Rate limiting sleep
                target_interval = 1.0 / max(rate, 0.1)
                elapsed = time.perf_counter() - start_tick
                sleep_duration = max(0.005, target_interval - elapsed)
                await asyncio.sleep(sleep_duration)

        except asyncio.CancelledError:
            logger.info("Producer background task cancelled cleanly.")
        except Exception as exc:
            self._last_error = str(exc)
            logger.exception("Unexpected error in producer background loop: %s", exc)
        finally:
            logger.info("Producer background task terminated.")

    async def start_stream(
        self,
        rate: Optional[float] = None,
        category: Optional[str] = None,
    ) -> Dict[str, object]:
        """Start or update the streaming data producer background task."""
        validated_rate = (
            self.config.validate_and_clamp_rate(rate)
            if rate is not None
            else self.lifecycle.rate
        )
        validated_category = (
            self.config.validate_category(category)
            if category is not None
            else self.lifecycle.category
        )

        self.lifecycle.start(rate=validated_rate, category=validated_category)

        # Ensure background task is running
        if self._bg_task is None or self._bg_task.done():
            self._stop_event.clear()
            self._bg_task = asyncio.create_task(self._run_loop(), name="kafka_stream_producer")

        return self.get_status()

    async def pause_stream(self) -> Dict[str, object]:
        """Pause message production while keeping background task alive."""
        self.lifecycle.pause()
        return self.get_status()

    async def resume_stream(self) -> Dict[str, object]:
        """Resume message production from PAUSED state."""
        self.lifecycle.resume()
        if self._bg_task is None or self._bg_task.done():
            self._stop_event.clear()
            self._bg_task = asyncio.create_task(self._run_loop(), name="kafka_stream_producer")
        return self.get_status()

    async def stop_stream(self) -> Dict[str, object]:
        """Stop message production and terminate the background loop cleanly."""
        self.lifecycle.stop()
        self._stop_event.set()
        if self._bg_task is not None and not self._bg_task.done():
            try:
                await asyncio.wait_for(self._bg_task, timeout=2.0)
            except asyncio.TimeoutError:
                self._bg_task.cancel()
                try:
                    await self._bg_task
                except asyncio.CancelledError:
                    pass
        self._bg_task = None
        if self._kafka_producer is not None:
            try:
                self._kafka_producer.flush(timeout=1.0)
            except Exception:
                pass
        return self.get_status()

    def get_status(self) -> Dict[str, object]:
        status = self.lifecycle.get_status()
        status["kafka_connected"] = self._kafka_connected
        status["kafka_topic"] = self.config.topic
        status["bootstrap_servers"] = self.config.bootstrap_servers
        status["last_error"] = self._last_error
        return status


_producer_instance: Optional[StreamingDataProducer] = None


def get_producer_instance() -> StreamingDataProducer:
    """Singleton accessor for the application's StreamingDataProducer."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = StreamingDataProducer()
    return _producer_instance


if __name__ == "__main__":
    # Standalone CLI mode for testing Member 1's producer directly
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Real-Time Kafka Text Stream Producer")
    parser.add_argument("--rate", type=float, default=5.0, help="Messages per second")
    parser.add_argument("--category", type=str, default="Technology", help="Data category")
    parser.add_argument("--duration", type=int, default=10, help="Duration in seconds (0 for manual stop)")
    args = parser.parse_args()

    async def _cli_main() -> None:
        producer = get_producer_instance()
        await producer.start_stream(rate=args.rate, category=args.category)
        if args.duration > 0:
            await asyncio.sleep(args.duration)
            await producer.stop_stream()
        else:
            while True:
                await asyncio.sleep(1)

    asyncio.run(_cli_main())
