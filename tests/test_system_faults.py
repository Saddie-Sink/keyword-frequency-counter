"""
Member 2 & Member 6 — System Fault Tolerance & Failure Handling Tests.
Verifies graceful behavior under:
- Kafka broker unreachable
- Database connection health check
- Invalid stream state transitions (409 Conflict)
- Invalid stream category (400 Bad Request)
- WebSocket disconnect cleanup
"""

from fastapi.testclient import TestClient

from backend.app.database import check_database_health
from backend.app.main import app
from backend.app.websocket_manager import ws_manager
from kafka_infra.healthcheck import KafkaInfrastructureManager


def test_kafka_unreachable_graceful_fallback():
    """Verify KafkaInfrastructureManager handles an unreachable broker without crashing or hanging."""
    manager = KafkaInfrastructureManager(
        bootstrap_servers="127.0.0.1:59999",
        topic="text_stream",
    )
    health = manager.check_health(force=True)
    assert health["connected"] is False
    assert health["mode"] == "embedded_fallback"
    assert health["topic"] == "text_stream"
    assert health["error"] is not None


def test_database_health_check():
    db_health = check_database_health()
    assert db_health["connected"] is True
    assert db_health["mode"] in ("postgresql", "sqlite_fallback")


def test_invalid_lifecycle_and_bad_category_fault_handling():
    with TestClient(app) as client:
        # Ensure stream is stopped first
        client.post("/api/stream/stop")

        # Pausing a STOPPED stream must return HTTP 409 Conflict
        r_pause_err = client.post("/api/stream/pause")
        assert r_pause_err.status_code == 409

        # Resuming a STOPPED stream must return HTTP 409 Conflict
        r_resume_err = client.post("/api/stream/resume")
        assert r_resume_err.status_code == 409

        # Starting with an invalid category must return HTTP 400 Bad Request
        r_bad_cat = client.post(
            "/api/stream/start",
            json={"rate": 10, "category": "InvalidCategoryName"},
        )
        assert r_bad_cat.status_code == 400


def test_websocket_disconnect_cleanup():
    initial_count = ws_manager.connection_count
    with TestClient(app) as client:
        with client.websocket_connect("/ws"):
            assert ws_manager.connection_count == initial_count + 1
    assert ws_manager.connection_count == initial_count
