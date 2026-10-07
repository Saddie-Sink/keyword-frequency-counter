"""
Member 4 — FastAPI Application Entrypoint (`main.py`).
Integrates:
- PostgreSQL database initialization (`init_db`)
- REST API routes (`/api/...`)
- Stream Control routes (`/api/stream/...`)
- WebSocket server (`WS /ws`) emitting:
    * `stats_update`
    * `keyword_update`
    * `trend_update`
    * `stream_status`
    * `system_status`
    * `new_message`
    * `error`
- Producer callback pipeline so every streamed message is ingested and pushed live
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backend.app.analytics_engine import analytics_engine
from backend.app.config import settings
from backend.app.database import init_db
from backend.app.health_service import get_system_status
from backend.app.routes.api import router as api_router
from backend.app.routes.stream import router as stream_router
from backend.app.websocket_manager import ws_manager
from producer.lifecycle import StreamState
from producer.producer import get_producer_instance

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backend.main")

_last_ws_aggregate_push: float = 0.0


async def _on_producer_message(message: Dict[str, object]) -> None:
    """
    Callback invoked whenever Member 1's producer emits a message.
    1. Ingests into RealTimeAnalyticsEngine
    2. Immediately broadcasts `new_message` to connected WebSocket clients
    3. Throttles aggregated `stats_update`, `keyword_update`, and `trend_update`
       broadcasts to ~3 times per second so high streaming rates never flood the browser.
    """
    global _last_ws_aggregate_push
    enriched = analytics_engine.ingest_message(message)

    if ws_manager.connection_count == 0:
        return

    await ws_manager.broadcast("new_message", enriched)

    now = time.monotonic()
    if now - _last_ws_aggregate_push >= 0.35:
        _last_ws_aggregate_push = now
        producer = get_producer_instance()
        is_running = producer.lifecycle.state == StreamState.RUNNING
        await ws_manager.broadcast(
            "stats_update",
            analytics_engine.get_stream_stats(
                configured_rate=producer.lifecycle.rate,
                stream_running=is_running,
            ),
        )
        await ws_manager.broadcast(
            "keyword_update",
            {
                "1m": analytics_engine.get_top_keywords("1m", limit=50),
                "5m": analytics_engine.get_top_keywords("5m", limit=50),
                "15m": analytics_engine.get_top_keywords("15m", limit=50),
                "1h": analytics_engine.get_top_keywords("1h", limit=50),
            },
        )
        await ws_manager.broadcast(
            "trend_update",
            analytics_engine.get_trending_keywords(limit=25),
        )


async def _periodic_status_broadcaster() -> None:
    """Background task that broadcasts `system_status` and `stats_update` periodically."""
    try:
        while True:
            await asyncio.sleep(2.0)
            if ws_manager.connection_count > 0:
                producer = get_producer_instance()
                is_running = producer.lifecycle.state == StreamState.RUNNING
                await ws_manager.broadcast("system_status", get_system_status())
                await ws_manager.broadcast("stream_status", producer.get_status())
                await ws_manager.broadcast(
                    "stats_update",
                    analytics_engine.get_stream_stats(
                        configured_rate=producer.lifecycle.rate,
                        stream_running=is_running,
                    ),
                )
    except asyncio.CancelledError:
        pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s v%s...", settings.app_name, settings.version)
    init_db()
    producer = get_producer_instance()
    producer.register_callback(_on_producer_message)
    broadcaster_task = asyncio.create_task(_periodic_status_broadcaster())
    yield
    logger.info("Shutting down application and stopping stream producer...")
    broadcaster_task.cancel()
    await producer.stop_stream()


app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description=(
        "Real-Time Big Data Analytics API powered by Apache Kafka, "
        "Apache Spark Structured Streaming, PostgreSQL, and FastAPI WebSockets."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
app.include_router(stream_router)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Real-time WebSocket endpoint (`WS /ws`).
    On initial connection, sends a complete snapshot of:
    - `system_status`
    - `stream_status`
    - `stats_update`
    - `keyword_update`
    - `trend_update`
    """
    await ws_manager.connect(websocket)
    producer = get_producer_instance()
    is_running = producer.lifecycle.state == StreamState.RUNNING

    try:
        # Send initial state snapshot to newly connected client
        await ws_manager.send_personal(websocket, "system_status", get_system_status())
        await ws_manager.send_personal(websocket, "stream_status", producer.get_status())
        await ws_manager.send_personal(
            websocket,
            "stats_update",
            analytics_engine.get_stream_stats(
                configured_rate=producer.lifecycle.rate,
                stream_running=is_running,
            ),
        )
        await ws_manager.send_personal(
            websocket,
            "keyword_update",
            {
                "1m": analytics_engine.get_top_keywords("1m", limit=50),
                "5m": analytics_engine.get_top_keywords("5m", limit=50),
                "15m": analytics_engine.get_top_keywords("15m", limit=50),
                "1h": analytics_engine.get_top_keywords("1h", limit=50),
            },
        )
        await ws_manager.send_personal(
            websocket,
            "trend_update",
            analytics_engine.get_trending_keywords(limit=25),
        )

        while True:
            raw_msg = await websocket.receive_text()
            try:
                data = json.loads(raw_msg)
                action = data.get("action")
                if action == "ping":
                    await ws_manager.send_personal(websocket, "system_status", get_system_status())
                elif action == "select_window":
                    w = str(data.get("window", "1m"))
                    await ws_manager.send_personal(
                        websocket,
                        "keyword_update",
                        {
                            w: analytics_engine.get_top_keywords(w, limit=50),
                        },
                    )
            except Exception as parse_err:
                await ws_manager.send_personal(
                    websocket,
                    "error",
                    {"message": f"Invalid WebSocket message format: {parse_err}"},
                )
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
    except Exception as exc:
        logger.debug("WebSocket session ended: %s", exc)
        await ws_manager.disconnect(websocket)
