"""
Member 1 — Stream Lifecycle State Machine.
Enforces the required state transitions:
STOPPED -> START -> RUNNING -> PAUSE -> PAUSED -> RESUME -> RUNNING -> STOP -> STOPPED
and supports restarting (STOPPED -> START -> RUNNING) without restarting Docker.
"""

import logging
import threading
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Optional

logger = logging.getLogger("producer.lifecycle")


class StreamState(str, Enum):
    STOPPED = "STOPPED"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"


class InvalidStateTransitionError(ValueError):
    """Raised when an invalid stream lifecycle transition is requested."""


class StreamLifecycleManager:
    """
    Thread-safe lifecycle manager for the streaming data producer.
    Tracks state, rate, category, uptime, and total messages published.
    """

    def __init__(
        self,
        initial_rate: float = 10.0,
        initial_category: str = "Technology",
    ) -> None:
        self._lock = threading.RLock()
        self._state: StreamState = StreamState.STOPPED
        self._rate: float = initial_rate
        self._category: str = initial_category
        self._started_at: Optional[datetime] = None
        self._updated_at: datetime = datetime.now(timezone.utc)
        self._messages_sent_session: int = 0
        self._messages_sent_total: int = 0

    @property
    def state(self) -> StreamState:
        with self._lock:
            return self._state

    @property
    def rate(self) -> float:
        with self._lock:
            return self._rate

    @property
    def category(self) -> str:
        with self._lock:
            return self._category

    def update_config(self, rate: Optional[float] = None, category: Optional[str] = None) -> None:
        with self._lock:
            if rate is not None:
                self._rate = rate
            if category is not None:
                self._category = category
            self._updated_at = datetime.now(timezone.utc)

    def start(self, rate: Optional[float] = None, category: Optional[str] = None) -> StreamState:
        """
        Transition: STOPPED -> RUNNING (or update config if already RUNNING).
        If PAUSED, starting also transitions to RUNNING.
        """
        with self._lock:
            if rate is not None:
                self._rate = rate
            if category is not None:
                self._category = category

            if self._state == StreamState.STOPPED:
                self._state = StreamState.RUNNING
                self._started_at = datetime.now(timezone.utc)
                self._messages_sent_session = 0
                self._updated_at = datetime.now(timezone.utc)
                logger.info(
                    "Stream lifecycle transition: STOPPED -> RUNNING (rate=%s msg/s, category=%s)",
                    self._rate,
                    self._category,
                )
            elif self._state == StreamState.PAUSED:
                self._state = StreamState.RUNNING
                self._updated_at = datetime.now(timezone.utc)
                logger.info(
                    "Stream lifecycle transition: PAUSED -> RUNNING via START (rate=%s msg/s, category=%s)",
                    self._rate,
                    self._category,
                )
            else:
                # Already RUNNING: allow dynamic rate/category update
                self._updated_at = datetime.now(timezone.utc)
                logger.info(
                    "Stream already RUNNING; updated config (rate=%s msg/s, category=%s)",
                    self._rate,
                    self._category,
                )
            return self._state

    def pause(self) -> StreamState:
        """Transition: RUNNING -> PAUSED."""
        with self._lock:
            if self._state != StreamState.RUNNING:
                raise InvalidStateTransitionError(
                    f"Cannot PAUSE stream when current state is {self._state.value}. Stream must be RUNNING."
                )
            self._state = StreamState.PAUSED
            self._updated_at = datetime.now(timezone.utc)
            logger.info("Stream lifecycle transition: RUNNING -> PAUSED")
            return self._state

    def resume(self) -> StreamState:
        """Transition: PAUSED -> RUNNING."""
        with self._lock:
            if self._state != StreamState.PAUSED:
                raise InvalidStateTransitionError(
                    f"Cannot RESUME stream when current state is {self._state.value}. Stream must be PAUSED."
                )
            self._state = StreamState.RUNNING
            self._updated_at = datetime.now(timezone.utc)
            logger.info("Stream lifecycle transition: PAUSED -> RUNNING")
            return self._state

    def stop(self) -> StreamState:
        """Transition: RUNNING or PAUSED -> STOPPED."""
        with self._lock:
            prev_state = self._state
            self._state = StreamState.STOPPED
            self._started_at = None
            self._updated_at = datetime.now(timezone.utc)
            logger.info("Stream lifecycle transition: %s -> STOPPED", prev_state.value)
            return self._state

    def record_message_sent(self) -> int:
        """Increment message counters and return total messages sent."""
        with self._lock:
            self._messages_sent_session += 1
            self._messages_sent_total += 1
            return self._messages_sent_total

    def get_status(self) -> Dict[str, object]:
        """Return current stream status snapshot."""
        with self._lock:
            uptime_seconds = 0.0
            if self._started_at and self._state != StreamState.STOPPED:
                uptime_seconds = round(
                    (datetime.now(timezone.utc) - self._started_at).total_seconds(), 1
                )
            return {
                "state": self._state.value,
                "rate": self._rate,
                "category": self._category,
                "messages_sent_session": self._messages_sent_session,
                "messages_sent_total": self._messages_sent_total,
                "started_at": self._started_at.isoformat() if self._started_at else None,
                "updated_at": self._updated_at.isoformat(),
                "uptime_seconds": uptime_seconds,
            }
