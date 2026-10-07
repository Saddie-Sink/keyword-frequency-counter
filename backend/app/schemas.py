"""
Member 4 — Pydantic Request and Response Schemas (`schemas.py`).
Provides strict type validation and serialization for all REST API and WebSocket payloads.
"""

from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class StreamStartRequest(BaseModel):
    rate: Optional[float] = Field(
        default=10.0,
        ge=1.0,
        le=100.0,
        description="Messages per second (1 to 100)",
    )
    category: Optional[str] = Field(
        default="Technology",
        description="Data category: Technology, Big Data, Artificial Intelligence, Data Science, News, Product Reviews, or All",
    )


class StreamStatusResponse(BaseModel):
    state: str
    rate: float
    category: str
    messages_sent_session: int
    messages_sent_total: int
    started_at: Optional[str] = None
    updated_at: str
    uptime_seconds: float
    kafka_connected: bool
    kafka_topic: str
    bootstrap_servers: str
    last_error: Optional[str] = None


class ComponentStatus(BaseModel):
    name: str
    status: str  # e.g., "Connected", "Running", "Ready", "Degraded"
    healthy: bool
    details: Optional[Dict[str, object]] = None


class SystemStatusResponse(BaseModel):
    kafka: ComponentStatus
    spark: ComponentStatus
    postgresql: ComponentStatus
    backend: ComponentStatus
    websocket: ComponentStatus
    stream_state: str
    timestamp: str


class KeywordItem(BaseModel):
    rank: int
    keyword: str
    frequency: int
    percentage: float
    window_label: str = "1m"
    window_start: Optional[str] = None
    window_end: Optional[str] = None


class KeywordDetailResponse(BaseModel):
    keyword: str
    rank: Optional[int] = None
    current_frequency: int
    percentage: float
    change_percentage: float
    previous_frequency: int
    windows: Dict[str, int]
    history: List[Dict[str, object]]


class TrendingKeywordItem(BaseModel):
    keyword: str
    current_frequency: int
    previous_frequency: int
    change_percentage: float
    window_start: Optional[str] = None
    direction: str  # "up", "down", "stable", "new"


class StreamStatsSummary(BaseModel):
    total_messages: int
    total_words: int
    unique_keywords: int
    messages_per_sec: float
    processing_rate: float
    active_window: str
    timestamp: str
    frequency_over_time: List[Dict[str, object]] = Field(default_factory=list)


class LiveMessageItem(BaseModel):
    id: str
    sequence_id: int
    text: str
    category: str
    timestamp: str
    keywords: List[str] = Field(default_factory=list)
