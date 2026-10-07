"""
Member 1 — Producer Configuration.
Manages Kafka connection settings, default streaming rates, and safety bounds.
"""

import os
from dataclasses import dataclass, field
from typing import List

VALID_CATEGORIES: List[str] = [
    "Technology",
    "Big Data",
    "Artificial Intelligence",
    "Data Science",
    "News",
    "Product Reviews",
    "All",
]


@dataclass
class ProducerConfig:
    """Configuration for the Kafka Streaming Data Producer."""

    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "text_stream")
    )
    messages_per_second: float = field(
        default_factory=lambda: float(os.getenv("DEFAULT_STREAM_RATE", "10"))
    )
    category: str = field(
        default_factory=lambda: os.getenv("DEFAULT_STREAM_CATEGORY", "Technology")
    )
    min_rate: float = 1.0
    max_rate: float = 100.0
    request_timeout_ms: int = 5000
    retries: int = 3

    def validate_and_clamp_rate(self, rate: float) -> float:
        """Ensure the streaming rate remains within safe, controlled bounds."""
        if rate < self.min_rate:
            return self.min_rate
        if rate > self.max_rate:
            return self.max_rate
        return float(rate)

    def validate_category(self, category: str) -> str:
        """Validate the requested category, defaulting to Technology if unrecognized."""
        for valid in VALID_CATEGORIES:
            if valid.lower() == category.strip().lower():
                return valid
        raise ValueError(
            f"Invalid category '{category}'. Allowed categories: {', '.join(VALID_CATEGORIES)}"
        )
