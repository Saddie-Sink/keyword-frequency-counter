"""
Member 1 — Data Producer & Stream Control Package.
Provides realistic multi-category text stream generation, Kafka publishing,
and strict lifecycle state management (STOPPED, RUNNING, PAUSED).
"""

from producer.config import ProducerConfig
from producer.lifecycle import StreamLifecycleManager, StreamState
from producer.producer import StreamingDataProducer, get_producer_instance
from producer.sample_datasets import CATEGORIES, generate_message

__all__ = [
    "ProducerConfig",
    "StreamLifecycleManager",
    "StreamState",
    "StreamingDataProducer",
    "get_producer_instance",
    "CATEGORIES",
    "generate_message",
]
