"""
Member 4 — PostgreSQL SQLAlchemy ORM Models (`models.py`).
Defines the exact three analytical tables specified in Section 6 Part A:
1. `keyword_statistics`
2. `trend_statistics`
3. `stream_statistics`
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.database import Base


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class KeywordStatistics(Base):
    """
    Aggregated time-window keyword frequency & percentage records.
    Columns:
    - id
    - keyword
    - window_label ('1m', '5m', '15m', '1h')
    - window_start
    - window_end
    - frequency
    - percentage
    - created_at
    """

    __tablename__ = "keyword_statistics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    keyword: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    window_label: Mapped[str] = mapped_column(String(16), nullable=False, default="1m", index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    frequency: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, index=True
    )

    __table_args__ = (
        Index("idx_kw_window_created", "window_label", "created_at"),
    )


class TrendStatistics(Base):
    """
    Trending keyword velocity records comparing current vs previous window frequencies.
    Columns:
    - id
    - keyword
    - window_start
    - current_frequency
    - previous_frequency
    - change_percentage
    - created_at
    """

    __tablename__ = "trend_statistics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    keyword: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    current_frequency: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    previous_frequency: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    change_percentage: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, index=True
    )


class StreamStatistics(Base):
    """
    Stream-level throughput and volume snapshots.
    Columns:
    - id
    - timestamp
    - message_count
    - word_count
    - unique_keyword_count
    - processing_rate
    """

    __tablename__ = "stream_statistics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_utc_now, index=True
    )
    message_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    word_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unique_keyword_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    processing_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
