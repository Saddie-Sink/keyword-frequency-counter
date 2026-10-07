"""
Member 3 & Member 4 — Real-Time Windowed Analytics & PostgreSQL Persistence Engine.
Coordinates between:
1. External Spark Structured Streaming container writing directly to PostgreSQL
   (`keyword_statistics`, `trend_statistics`, `stream_statistics`), AND
2. Real-time micro-batch window aggregation using Member 3's exact preprocessing,
   windowing (`1m`, `5m`, `15m`, `1h`), rank/percentage, and trend formula:
       Change % = ((Current - Previous) / Previous) * 100
   with safe `Previous = 0` handling, persisting directly to the SQLAlchemy database
   (`keyword_statistics`, `trend_statistics`, `stream_statistics`).

Also maintains a bounded ring buffer (`maxlen=50`) of recent messages so raw messages
are never stored indefinitely in the database (Requirement 6 Part A & 7 Part H).
"""

import collections
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Optional, Tuple

from sqlalchemy import desc
from sqlalchemy.orm import Session

from backend.app.config import settings
from backend.app.database import get_session_factory
from backend.app.models import KeywordStatistics, StreamStatistics, TrendStatistics
from spark.preprocessing import preprocess_text_string_tokens

logger = logging.getLogger("backend.analytics_engine")

WINDOW_SECONDS: Dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}


def compute_safe_change_percentage(current_freq: int, previous_freq: int) -> float:
    """
    Compute Trend Change % according to Member 3 Requirement 5.F:
        Change % = ((Current - Previous) / Previous) * 100
    Safely handles Previous = 0 without division by zero.
    """
    if previous_freq == 0:
        return 100.0 if current_freq > 0 else 0.0
    return round(((current_freq - previous_freq) / float(previous_freq)) * 100.0, 2)


class RealTimeAnalyticsEngine:
    """
    Maintains real-time sliding event-time windows (1m, 5m, 15m, 1h),
    persists aggregated statistics to PostgreSQL (`keyword_statistics`, `trend_statistics`,
    `stream_statistics`), and serves low-latency queries for the REST API and WebSocket.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        # Bounded recent messages ring buffer (Requirement 6A & 7H: do not store unlimited raw messages)
        self._recent_messages: Deque[Dict[str, object]] = collections.deque(
            maxlen=settings.max_recent_messages
        )
        # Sliding event buffer of (timestamp_epoch, [keywords]) bounded to 1 hour + 2m watermark
        self._token_events: Deque[Tuple[float, List[str]]] = collections.deque(maxlen=50000)

        # Cumulative stream counters
        self._total_messages: int = 0
        self._total_words: int = 0
        self._unique_keywords_seen: set[str] = set()

        # Processing rate tracking (sliding 10s window of processed events)
        self._recent_msg_timestamps: Deque[float] = collections.deque(maxlen=2000)

        # Previous window snapshot for trend comparison when periods roll over
        self._baseline_counts: Dict[str, int] = {}
        self._last_baseline_rotation: float = time.time()
        self._last_db_flush: float = 0.0

        # Time-series history points for the "Frequency Over Time" line chart (bounded to 30 buckets)
        self._frequency_timeline: Deque[Dict[str, object]] = collections.deque(maxlen=30)

    def ingest_message(self, message: Dict[str, object]) -> Dict[str, object]:
        """
        Process an incoming streamed message:
        1. Extract cleaned keywords using Member 3's 6-stage preprocessing pipeline
        2. Update bounded recent messages buffer
        3. Update sliding event-time windows (1m, 5m, 15m, 1h) and trend state
        4. Periodically flush aggregated rows to PostgreSQL tables
        """
        text = str(message.get("text", ""))
        keywords = preprocess_text_string_tokens(text)
        now_ts = time.time()
        now_dt = datetime.now(timezone.utc)

        enriched_msg = {
            "id": str(message.get("id", "")),
            "sequence_id": int(message.get("sequence_id", self._total_messages + 1)),
            "text": text,
            "category": str(message.get("category", "Technology")),
            "timestamp": str(message.get("timestamp", now_dt.isoformat())),
            "keywords": keywords,
        }

        with self._lock:
            self._recent_messages.appendleft(enriched_msg)
            self._token_events.append((now_ts, keywords))
            self._recent_msg_timestamps.append(now_ts)

            self._total_messages += 1
            self._total_words += len(keywords)
            self._unique_keywords_seen.update(keywords)

            # Evict events older than 1 hour + 120s watermark (3720s)
            cutoff_1h = now_ts - 3720
            while self._token_events and self._token_events[0][0] < cutoff_1h:
                self._token_events.popleft()

            # Initialize baseline snapshot once we have initial keywords, and rotate every 15s for dynamic live trends
            if not self._baseline_counts and self._total_messages >= 5:
                self._baseline_counts = self._compute_window_counts_locked(now_ts, 60)
                self._last_baseline_rotation = now_ts
            elif now_ts - self._last_baseline_rotation >= 15.0:
                self._baseline_counts = self._compute_window_counts_locked(now_ts - 5.0, 60)
                self._last_baseline_rotation = now_ts

            # Record timeline point & flush aggregated tables to PostgreSQL every 2 seconds
            if now_ts - self._last_db_flush >= 2.0:
                self._last_db_flush = now_ts
                self._record_timeline_point_locked(now_dt, now_ts)
                self._persist_aggregates_to_db_locked(now_dt, now_ts)

        return enriched_msg

    def _compute_window_counts_locked(self, now_ts: float, window_secs: int) -> Dict[str, int]:
        cutoff = now_ts - window_secs
        counts: Dict[str, int] = collections.Counter()
        for ts, kw_list in reversed(self._token_events):
            if ts < cutoff:
                break
            counts.update(kw_list)
        return dict(counts)

    def _compute_processing_rate_locked(self, now_ts: float) -> float:
        cutoff = now_ts - 5.0
        while self._recent_msg_timestamps and self._recent_msg_timestamps[0] < cutoff:
            self._recent_msg_timestamps.popleft()
        if not self._recent_msg_timestamps:
            return 0.0
        span = max(1.0, now_ts - self._recent_msg_timestamps[0])
        return round(len(self._recent_msg_timestamps) / span, 2)

    def _record_timeline_point_locked(self, now_dt: datetime, now_ts: float) -> None:
        counts_1m = self._compute_window_counts_locked(now_ts, 60)
        sorted_kws = sorted(counts_1m.items(), key=lambda x: (-x[1], x[0]))
        top_5 = sorted_kws[:5]
        point: Dict[str, object] = {
            "time": now_dt.strftime("%H:%M:%S"),
            "timestamp": now_dt.isoformat(),
            "total_words_1m": sum(counts_1m.values()),
            "unique_keywords_1m": len(counts_1m),
        }
        for kw, freq in top_5:
            point[kw] = freq
        self._frequency_timeline.append(point)

    def _persist_aggregates_to_db_locked(self, now_dt: datetime, now_ts: float) -> None:
        """Persist current windowed keyword, trend, and stream statistics to PostgreSQL."""
        try:
            session_factory = get_session_factory()
            with session_factory() as db:
                # 1. StreamStatistics snapshot
                rate = self._compute_processing_rate_locked(now_ts)
                stream_row = StreamStatistics(
                    timestamp=now_dt,
                    message_count=self._total_messages,
                    word_count=self._total_words,
                    unique_keyword_count=len(self._unique_keywords_seen),
                    processing_rate=rate,
                )
                db.add(stream_row)

                # 2. KeywordStatistics top entries for active windows
                for w_label, w_secs in WINDOW_SECONDS.items():
                    w_counts = self._compute_window_counts_locked(now_ts, w_secs)
                    total_w = sum(w_counts.values())
                    if total_w == 0:
                        continue
                    w_start = now_dt - timedelta(seconds=w_secs)
                    top_items = sorted(w_counts.items(), key=lambda x: (-x[1], x[0]))[:25]
                    for kw, freq in top_items:
                        pct = round((freq / float(total_w)) * 100.0, 2)
                        db.add(
                            KeywordStatistics(
                                keyword=kw,
                                window_label=w_label,
                                window_start=w_start,
                                window_end=now_dt,
                                frequency=freq,
                                percentage=pct,
                                created_at=now_dt,
                            )
                        )

                # 3. TrendStatistics snapshot
                trends = self.get_trending_keywords(limit=20)
                w_start_1m = now_dt - timedelta(seconds=60)
                for item in trends:
                    db.add(
                        TrendStatistics(
                            keyword=str(item["keyword"]),
                            window_start=w_start_1m,
                            current_frequency=int(item["current_frequency"]),
                            previous_frequency=int(item["previous_frequency"]),
                            change_percentage=float(item["change_percentage"]),
                            created_at=now_dt,
                        )
                    )

                db.commit()
        except Exception as exc:
            logger.debug("Non-fatal error persisting aggregates to DB: %s", exc)

    def get_top_keywords(
        self,
        window_label: str = "1m",
        limit: int = 10,
        db: Optional[Session] = None,
    ) -> List[Dict[str, object]]:
        """
        Return ranked Top Keywords (Rank, Keyword, Frequency, Percentage) for the specified
        time window ('1m', '5m', '15m', '1h') and limit (Top 5, 10, 20, 50).
        """
        if window_label not in WINDOW_SECONDS:
            window_label = "1m"
        limit = max(1, min(limit, 100))

        now_ts = time.time()
        now_dt = datetime.now(timezone.utc)
        w_secs = WINDOW_SECONDS[window_label]

        with self._lock:
            counts = self._compute_window_counts_locked(now_ts, w_secs)

        # If in-memory window is empty, check if PostgreSQL has recent Spark-written rows
        if not counts and db is not None:
            try:
                recent_rows = (
                    db.query(KeywordStatistics)
                    .filter(KeywordStatistics.window_label == window_label)
                    .order_by(desc(KeywordStatistics.created_at), desc(KeywordStatistics.frequency))
                    .limit(limit)
                    .all()
                )
                if recent_rows:
                    return [
                        {
                            "rank": idx + 1,
                            "keyword": r.keyword,
                            "frequency": r.frequency,
                            "percentage": round(r.percentage, 2),
                            "window_label": r.window_label,
                            "window_start": r.window_start.isoformat() if r.window_start else None,
                            "window_end": r.window_end.isoformat() if r.window_end else None,
                        }
                        for idx, r in enumerate(recent_rows)
                    ]
            except Exception:
                pass

        total_words_in_window = sum(counts.values())
        sorted_items = sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:limit]
        w_start_iso = (now_dt - timedelta(seconds=w_secs)).isoformat()
        w_end_iso = now_dt.isoformat()

        results: List[Dict[str, object]] = []
        for idx, (kw, freq) in enumerate(sorted_items, start=1):
            pct = (
                round((freq / float(total_words_in_window)) * 100.0, 2)
                if total_words_in_window > 0
                else 0.0
            )
            results.append(
                {
                    "rank": idx,
                    "keyword": kw,
                    "frequency": freq,
                    "percentage": pct,
                    "window_label": window_label,
                    "window_start": w_start_iso,
                    "window_end": w_end_iso,
                }
            )
        return results

    def get_keyword_detail(self, keyword: str) -> Dict[str, object]:
        """Return multi-window breakdown and trend details for a specific keyword."""
        cleaned_kw = keyword.strip().lower()
        now_ts = time.time()

        with self._lock:
            windows_breakdown: Dict[str, int] = {}
            for w_label, w_secs in WINDOW_SECONDS.items():
                w_counts = self._compute_window_counts_locked(now_ts, w_secs)
                windows_breakdown[w_label] = w_counts.get(cleaned_kw, 0)

            counts_1m = self._compute_window_counts_locked(now_ts, 60)
            total_1m = sum(counts_1m.values())
            curr_freq = counts_1m.get(cleaned_kw, 0)
            pct = round((curr_freq / float(total_1m)) * 100.0, 2) if total_1m > 0 else 0.0

            sorted_1m = sorted(counts_1m.items(), key=lambda x: (-x[1], x[0]))
            rank: Optional[int] = None
            for idx, (k, _) in enumerate(sorted_1m, start=1):
                if k == cleaned_kw:
                    rank = idx
                    break

            prev_freq = self._baseline_counts.get(cleaned_kw, 0)
            change_pct = compute_safe_change_percentage(curr_freq, prev_freq)

            history = [
                {"time": pt["time"], "frequency": int(pt.get(cleaned_kw, 0))}
                for pt in self._frequency_timeline
            ]

        return {
            "keyword": cleaned_kw,
            "rank": rank,
            "current_frequency": curr_freq,
            "percentage": pct,
            "previous_frequency": prev_freq,
            "change_percentage": change_pct,
            "windows": windows_breakdown,
            "history": history,
        }

    def get_trending_keywords(self, limit: int = 10) -> List[Dict[str, object]]:
        """
        Compute Trending Keywords comparing current window vs previous window:
            Change % = ((Current - Previous) / Previous) * 100
        Handles Previous = 0 safely.
        """
        now_ts = time.time()
        now_dt = datetime.now(timezone.utc)
        w_start_iso = (now_dt - timedelta(seconds=60)).isoformat()

        with self._lock:
            curr_counts = self._compute_window_counts_locked(now_ts, 60)
            # Compare against baseline or prior 60-120s window
            prev_counts = self._baseline_counts.copy()
            if not prev_counts:
                # Compute 60s..120s ago window
                cutoff_recent = now_ts - 30
                cutoff_older = now_ts - 90
                older_counter: Dict[str, int] = collections.Counter()
                for ts, kw_list in self._token_events:
                    if cutoff_older <= ts < cutoff_recent:
                        older_counter.update(kw_list)
                prev_counts = dict(older_counter)

        trending_list: List[Dict[str, object]] = []
        for kw, curr_freq in curr_counts.items():
            prev_freq = prev_counts.get(kw, 0)
            change_pct = compute_safe_change_percentage(curr_freq, prev_freq)
            if prev_freq == 0 and curr_freq > 0:
                direction = "new"
            elif change_pct > 0:
                direction = "up"
            elif change_pct < 0:
                direction = "down"
            else:
                direction = "stable"

            trending_list.append(
                {
                    "keyword": kw,
                    "current_frequency": curr_freq,
                    "previous_frequency": prev_freq,
                    "change_percentage": change_pct,
                    "window_start": w_start_iso,
                    "direction": direction,
                }
            )

        trending_list.sort(
            key=lambda item: (
                -float(item["change_percentage"]),
                -int(item["current_frequency"]),
                str(item["keyword"]),
            )
        )
        return trending_list[: max(1, min(limit, 50))]

    def get_stream_stats(
        self,
        configured_rate: float = 0.0,
        stream_running: bool = False,
        active_window: str = "1m",
    ) -> Dict[str, object]:
        """Return summary statistics and frequency-over-time series for the dashboard."""
        now_ts = time.time()
        now_dt = datetime.now(timezone.utc)

        with self._lock:
            proc_rate = self._compute_processing_rate_locked(now_ts)
            if not stream_running:
                proc_rate = 0.0
            timeline = list(self._frequency_timeline)
            return {
                "total_messages": self._total_messages,
                "total_words": self._total_words,
                "unique_keywords": len(self._unique_keywords_seen),
                "messages_per_sec": configured_rate if stream_running else 0.0,
                "processing_rate": proc_rate,
                "active_window": active_window,
                "timestamp": now_dt.isoformat(),
                "frequency_over_time": timeline,
            }

    def get_recent_messages(self, limit: int = 20) -> List[Dict[str, object]]:
        """Return bounded recent messages (up to max_recent_messages=50)."""
        bounded_limit = max(1, min(limit, settings.max_recent_messages))
        with self._lock:
            return list(self._recent_messages)[:bounded_limit]


analytics_engine = RealTimeAnalyticsEngine()
