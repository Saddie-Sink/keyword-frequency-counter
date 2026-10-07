"""
Member 4 — Database Initialization & Connection Management.
Connects to PostgreSQL when available, and gracefully falls back to a local SQLite database
using the exact same SQLAlchemy ORM schema (`keyword_statistics`, `trend_statistics`,
`stream_statistics`) when running locally without a PostgreSQL server.
"""

import logging
import os
import socket
from pathlib import Path
from typing import Dict, Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from backend.app.config import settings

logger = logging.getLogger("backend.database")


class Base(DeclarativeBase):
    pass


def _can_reach_postgres(host: str, port: int, timeout: float = 0.5) -> bool:
    probe_host = "127.0.0.1" if host == "localhost" else host
    effective_timeout = 0.05 if probe_host == "127.0.0.1" else timeout
    try:
        with socket.create_connection((probe_host, port), timeout=effective_timeout):
            return True
    except OSError:
        return False


_active_db_mode: str = "postgresql"
_engine = None
_SessionLocal = None


def get_engine():
    global _engine, _active_db_mode
    if _engine is not None:
        return _engine

    explicit_url = os.getenv("DATABASE_URL")
    if explicit_url and explicit_url.startswith("sqlite"):
        _active_db_mode = "sqlite_fallback"
        _engine = create_engine(
            explicit_url,
            connect_args={"check_same_thread": False},
        )
        return _engine

    if _can_reach_postgres(settings.postgres_host, settings.postgres_port):
        try:
            candidate = create_engine(
                settings.postgres_url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                connect_args={"connect_timeout": 3},
            )
            with candidate.connect() as conn:
                conn.execute(text("SELECT 1"))
            _active_db_mode = "postgresql"
            _engine = candidate
            logger.info("Connected to PostgreSQL at %s:%s/%s", settings.postgres_host, settings.postgres_port, settings.postgres_db)
            return _engine
        except Exception as exc:
            logger.warning("PostgreSQL connection failed (%s); falling back to local SQLite persistence.", exc)

    # Resilient local fallback using identical SQLAlchemy tables
    data_dir = Path(__file__).resolve().parents[2] / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    sqlite_path = data_dir / "keyword_analytics.db"
    _active_db_mode = "sqlite_fallback"
    _engine = create_engine(
        f"sqlite:///{sqlite_path.as_posix()}",
        connect_args={"check_same_thread": False},
    )
    logger.info("Initialized local SQLAlchemy database at %s", sqlite_path)
    return _engine


def get_session_factory() -> sessionmaker:
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SessionLocal


def init_db() -> None:
    """Create all database tables if they do not already exist."""
    from backend.app import models  # noqa: F401

    engine = get_engine()
    Base.metadata.create_all(bind=engine)


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency yielding a database session."""
    session_factory = get_session_factory()
    db = session_factory()
    try:
        yield db
    finally:
        db.close()


def check_database_health() -> Dict[str, object]:
    """Check database connectivity and return status details for Member 4 health service."""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {
            "connected": True,
            "mode": _active_db_mode,
            "host": settings.postgres_host if _active_db_mode == "postgresql" else "local_storage",
            "database": settings.postgres_db,
            "error": None,
        }
    except Exception as exc:
        return {
            "connected": False,
            "mode": _active_db_mode,
            "host": settings.postgres_host,
            "database": settings.postgres_db,
            "error": str(exc),
        }
