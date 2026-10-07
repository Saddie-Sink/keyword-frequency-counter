"""
Member 4 & Member 1 — Stream Control API Endpoints (`routes/stream.py`).
Implements Section 6 Part C:
- POST /api/stream/start
- POST /api/stream/pause
- POST /api/stream/resume
- POST /api/stream/stop

Communicates directly with Member 1's `StreamingDataProducer` lifecycle manager
and broadcasts `stream_status` and `system_status` events over WebSocket.
"""

from typing import Optional

from fastapi import APIRouter, Body, HTTPException

from backend.app.health_service import get_system_status
from backend.app.schemas import StreamStartRequest, StreamStatusResponse
from backend.app.websocket_manager import ws_manager
from producer.lifecycle import InvalidStateTransitionError
from producer.producer import get_producer_instance

router = APIRouter(prefix="/api/stream", tags=["Stream Control API"])


@router.post("/start", response_model=StreamStatusResponse)
async def start_stream(
    payload: Optional[StreamStartRequest] = Body(default=None),
):
    """
    Start the real-time data producer stream (`STOPPED -> RUNNING`).
    Accepts optional `rate` (messages/sec) and `category`.
    Can also be called again after `STOP` without restarting Docker.
    """
    producer = get_producer_instance()
    rate = payload.rate if payload and payload.rate is not None else None
    category = payload.category if payload and payload.category is not None else None

    try:
        status = await producer.start_stream(rate=rate, category=category)
    except ValueError as exc:
        await ws_manager.broadcast("error", {"message": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await ws_manager.broadcast("stream_status", status)
    await ws_manager.broadcast("system_status", get_system_status())
    return status


@router.post("/pause", response_model=StreamStatusResponse)
async def pause_stream():
    """Pause the running stream (`RUNNING -> PAUSED`)."""
    producer = get_producer_instance()
    try:
        status = await producer.pause_stream()
    except InvalidStateTransitionError as exc:
        await ws_manager.broadcast("error", {"message": str(exc)})
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    await ws_manager.broadcast("stream_status", status)
    await ws_manager.broadcast("system_status", get_system_status())
    return status


@router.post("/resume", response_model=StreamStatusResponse)
async def resume_stream():
    """Resume a paused stream (`PAUSED -> RUNNING`)."""
    producer = get_producer_instance()
    try:
        status = await producer.resume_stream()
    except InvalidStateTransitionError as exc:
        await ws_manager.broadcast("error", {"message": str(exc)})
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    await ws_manager.broadcast("stream_status", status)
    await ws_manager.broadcast("system_status", get_system_status())
    return status


@router.post("/stop", response_model=StreamStatusResponse)
async def stop_stream():
    """Stop the stream (`RUNNING` or `PAUSED` -> `STOPPED`)."""
    producer = get_producer_instance()
    status = await producer.stop_stream()
    await ws_manager.broadcast("stream_status", status)
    await ws_manager.broadcast("system_status", get_system_status())
    return status
