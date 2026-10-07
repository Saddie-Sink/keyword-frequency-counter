"""
Member 4 & Member 6 — FastAPI REST API, Stream Control & WebSocket Tests.
Verifies all required endpoints:
- GET  /api/health
- GET  /api/status
- GET  /api/stats
- GET  /api/keywords
- GET  /api/keywords/{keyword}
- GET  /api/trending
- GET  /api/messages
- GET  /api/stream/status
- POST /api/stream/start
- POST /api/stream/pause
- POST /api/stream/resume
- POST /api/stream/stop
- WS   /ws
"""

import time
from fastapi.testclient import TestClient

from backend.app.main import app


def test_all_rest_and_stream_control_endpoints():
    with TestClient(app) as client:
        # 1. Health & System Status
        r_health = client.get("/api/health")
        assert r_health.status_code == 200
        assert r_health.json()["status"] == "healthy"

        r_status = client.get("/api/status")
        assert r_status.status_code == 200
        status_data = r_status.json()
        for comp in ["kafka", "spark", "postgresql", "backend", "websocket"]:
            assert comp in status_data

        # 2. Initial Stream Status must be STOPPED (Requirement 8 Part A)
        r_stream = client.get("/api/stream/status")
        assert r_stream.status_code == 200
        assert r_stream.json()["state"] == "STOPPED"

        # 3. Stream Control Lifecycle: START -> PAUSE -> RESUME -> STOP -> START again
        r_start = client.post(
            "/api/stream/start",
            json={"rate": 20, "category": "Technology"},
        )
        assert r_start.status_code == 200
        assert r_start.json()["state"] == "RUNNING"
        assert r_start.json()["rate"] == 20.0
        assert r_start.json()["category"] == "Technology"

        # Let producer generate a few messages
        time.sleep(0.3)

        r_pause = client.post("/api/stream/pause")
        assert r_pause.status_code == 200
        assert r_pause.json()["state"] == "PAUSED"

        r_resume = client.post("/api/stream/resume")
        assert r_resume.status_code == 200
        assert r_resume.json()["state"] == "RUNNING"

        time.sleep(0.2)

        r_stop = client.post("/api/stream/stop")
        assert r_stop.status_code == 200
        assert r_stop.json()["state"] == "STOPPED"

        # Verify START again without restarting infrastructure
        r_restart = client.post(
            "/api/stream/start",
            json={"rate": 10, "category": "Big Data"},
        )
        assert r_restart.status_code == 200
        assert r_restart.json()["state"] == "RUNNING"
        client.post("/api/stream/stop")

        # 4. Analytics Endpoints (/api/stats, /api/keywords, /api/trending, /api/messages)
        r_stats = client.get("/api/stats?window=1m")
        assert r_stats.status_code == 200
        stats_payload = r_stats.json()
        assert stats_payload["total_messages"] >= 1
        assert stats_payload["total_words"] >= 1

        r_kw = client.get("/api/keywords?window=1m&limit=10")
        assert r_kw.status_code == 200
        kw_list = r_kw.json()
        assert len(kw_list) >= 1
        first_kw = kw_list[0]["keyword"]

        r_kw_detail = client.get(f"/api/keywords/{first_kw}")
        assert r_kw_detail.status_code == 200
        assert r_kw_detail.json()["keyword"] == first_kw

        r_trending = client.get("/api/trending?limit=10")
        assert r_trending.status_code == 200
        assert isinstance(r_trending.json(), list)

        r_msgs = client.get("/api/messages?limit=20")
        assert r_msgs.status_code == 200
        assert len(r_msgs.json()) >= 1


def test_websocket_connection_and_events():
    with TestClient(app) as client:
        client.post("/api/stream/stop")
        with client.websocket_connect("/ws") as ws:
            received_types = set()
            while len(received_types) < 5:
                msg = ws.receive_json()
                received_types.add(msg["type"])

            assert "system_status" in received_types
            assert "stream_status" in received_types
            assert "stats_update" in received_types
            assert "keyword_update" in received_types
            assert "trend_update" in received_types

            # Test ping action over WebSocket
            ws.send_json({"action": "ping"})
            found_pong = False
            for _ in range(5):
                pong = ws.receive_json()
                if pong["type"] == "system_status":
                    found_pong = True
                    break
            assert found_pong
