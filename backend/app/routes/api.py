"""
Member 4 — REST API Endpoints (`routes/api.py`).
Implements Section 6 Part B:
- GET /api/health
- GET /api/status
- GET /api/stats
- GET /api/keywords
- GET /api/keywords/{keyword}
- GET /api/trending
- GET /api/messages
- GET /api/stream/status
"""

from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from backend.app.analytics_engine import analytics_engine
from backend.app.database import get_db
from backend.app.health_service import get_system_status
from backend.app.schemas import (
    KeywordDetailResponse,
    KeywordItem,
    LiveMessageItem,
    StreamStatsSummary,
    StreamStatusResponse,
    SystemStatusResponse,
    TrendingKeywordItem,
)
from producer.lifecycle import StreamState
from producer.producer import get_producer_instance
from producer.sample_datasets import CATEGORIES

router = APIRouter(prefix="/api", tags=["Analytics & System API"])


@router.get("/health")
async def health_check():
    """Return service health check summary."""
    status = get_system_status()
    return {
        "status": "healthy",
        "service": "Real-Time Keyword Analytics Backend",
        "stream_state": status["stream_state"],
        "components": {
            "kafka": status["kafka"]["status"],
            "spark": status["spark"]["status"],
            "postgresql": status["postgresql"]["status"],
            "backend": status["backend"]["status"],
            "websocket": status["websocket"]["status"],
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.get("/status", response_model=SystemStatusResponse)
async def system_status():
    """Return detailed status for Kafka, Spark, PostgreSQL, Backend, and WebSocket."""
    return get_system_status()


@router.get("/stats", response_model=StreamStatsSummary)
async def stream_stats(
    window: str = Query(default="1m", description="Time window: 1m, 5m, 15m, or 1h"),
):
    """Return real-time summary statistics and frequency-over-time series."""
    producer = get_producer_instance()
    is_running = producer.lifecycle.state == StreamState.RUNNING
    return analytics_engine.get_stream_stats(
        configured_rate=producer.lifecycle.rate,
        stream_running=is_running,
        active_window=window,
    )


@router.get("/keywords", response_model=List[KeywordItem])
async def get_keywords(
    limit: int = Query(default=10, ge=1, le=100, description="Top N keywords (5, 10, 20, 50)"),
    window: str = Query(default="1m", description="Spark time window: 1m, 5m, 15m, 1h"),
    db: Session = Depends(get_db),
):
    """Return top ranked keywords with frequency and percentage for the selected time window."""
    return analytics_engine.get_top_keywords(window_label=window, limit=limit, db=db)


@router.get("/keywords/{keyword}", response_model=KeywordDetailResponse)
async def get_keyword_detail(keyword: str):
    """Return detailed statistics across all time windows (1m, 5m, 15m, 1h) for a specific keyword."""
    return analytics_engine.get_keyword_detail(keyword)


@router.get("/trending", response_model=List[TrendingKeywordItem])
async def get_trending(
    limit: int = Query(default=10, ge=1, le=50, description="Number of trending keywords to return"),
):
    """Return trending keywords sorted by Change % ((Current - Previous) / Previous * 100)."""
    return analytics_engine.get_trending_keywords(limit=limit)


@router.get("/messages", response_model=List[LiveMessageItem])
async def get_recent_messages(
    limit: int = Query(default=20, ge=1, le=50, description="Bounded number of recent messages"),
):
    """Return a bounded list of recent streamed messages."""
    return analytics_engine.get_recent_messages(limit=limit)


@router.get("/stream/status", response_model=StreamStatusResponse)
async def get_stream_status():
    """Return current producer lifecycle state, rate, and category."""
    producer = get_producer_instance()
    return producer.get_status()


@router.get("/categories")
async def get_categories():
    """Return all supported data categories for the stream producer."""
    return {"categories": CATEGORIES + ["All"]}
