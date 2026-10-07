"""
Member 4 — Backend Configuration.
Reads PostgreSQL, Kafka, Spark, and FastAPI settings from environment variables.
"""

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    app_name: str = "Real-Time Keyword Frequency & Trend Analytics API"
    version: str = "1.0.0"

    postgres_host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    postgres_port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    postgres_db: str = field(default_factory=lambda: os.getenv("POSTGRES_DB", "keyword_analytics"))
    postgres_user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    postgres_password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"))

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "text_stream"))
    kafka_consumer_group: str = field(
        default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "spark_keyword_analytics_group")
    )

    max_recent_messages: int = 50

    @property
    def postgres_url(self) -> str:
        env_url = os.getenv("DATABASE_URL")
        if env_url:
            return env_url
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()
