"""
Member 4 — WebSocket Connection Manager (`websocket_manager.py`).
Implements `WS /ws` real-time push notifications for:
- `stats_update`
- `keyword_update`
- `trend_update`
- `stream_status`
- `system_status`
- `new_message`
- `error`
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set
from fastapi import WebSocket

logger = logging.getLogger("backend.websocket")

SUPPORTED_WS_EVENTS = {
    "stats_update",
    "keyword_update",
    "trend_update",
    "stream_status",
    "system_status",
    "new_message",
    "error",
}


class WebSocketManager:
    """Manages active WebSocket client connections and broadcasts aggregated updates."""

    def __init__(self) -> None:
        self._active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()

    @property
    def connection_count(self) -> int:
        return len(self._active_connections)

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._active_connections.add(websocket)
        logger.info("WebSocket client connected (active_clients=%d)", len(self._active_connections))

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._active_connections.discard(websocket)
        logger.info("WebSocket client disconnected (active_clients=%d)", len(self._active_connections))

    async def send_personal(self, websocket: WebSocket, event_type: str, data: object) -> None:
        payload = {
            "type": event_type,
            "data": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await websocket.send_text(json.dumps(payload, default=str))
        except Exception as exc:
            logger.debug("Failed to send personal WebSocket message: %s", exc)
            await self.disconnect(websocket)

    async def broadcast(self, event_type: str, data: object) -> None:
        """Broadcast an event payload to all connected WebSocket clients."""
        if not self._active_connections:
            return

        payload = json.dumps(
            {
                "type": event_type,
                "data": data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            default=str,
        )

        dead_connections: List[WebSocket] = []
        async with self._lock:
            clients = list(self._active_connections)

        for ws in clients:
            try:
                await ws.send_text(payload)
            except Exception:
                dead_connections.append(ws)

        if dead_connections:
            async with self._lock:
                for dead in dead_connections:
                    self._active_connections.discard(dead)


ws_manager = WebSocketManager()
