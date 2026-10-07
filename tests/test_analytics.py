"""
Member 3 & Member 6 — Analytics Engine Tests (Frequency, Windows, Trends, Zero-Division).
Verifies:
- Keyword frequency, percentage, and rank calculation
- Time-window aggregation (1m, 5m, 15m, 1h)
- Trend calculation: Change % = ((Current - Previous) / Previous) * 100
- Safe zero-division handling when Previous = 0
"""

from backend.app.analytics_engine import (
    RealTimeAnalyticsEngine,
    compute_safe_change_percentage,
)


def test_trend_change_percentage_formula_and_zero_division():
    """
    Verify Requirement 5.F & 8.D:
    Change % = ((Current - Previous) / Previous) * 100
    Handle Previous = 0 safely without ZeroDivisionError.
    """
    # Standard growth: ((150 - 100) / 100) * 100 = +50.0%
    assert compute_safe_change_percentage(150, 100) == 50.0

    # Standard decline: ((50 - 100) / 100) * 100 = -50.0%
    assert compute_safe_change_percentage(50, 100) == -50.0

    # No change: ((80 - 80) / 80) * 100 = 0.0%
    assert compute_safe_change_percentage(80, 80) == 0.0

    # Zero-division safe handling when Previous = 0
    assert compute_safe_change_percentage(25, 0) == 100.0
    assert compute_safe_change_percentage(0, 0) == 0.0


def test_analytics_engine_frequency_rank_percentage_and_windows():
    engine = RealTimeAnalyticsEngine()

    sample_messages = [
        {
            "id": "m1",
            "sequence_id": 1,
            "text": "Python and Spark power Big Data Analytics with Python.",
            "category": "Big Data",
            "timestamp": "2026-09-30T12:00:00Z",
        },
        {
            "id": "m2",
            "sequence_id": 2,
            "text": "Python streams Kafka messages into Spark Structured Streaming.",
            "category": "Big Data",
            "timestamp": "2026-09-30T12:00:01Z",
        },
    ]

    for msg in sample_messages:
        engine.ingest_message(msg)

    # Check 1m, 5m, 15m, 1h windows
    for window_label in ["1m", "5m", "15m", "1h"]:
        top_kws = engine.get_top_keywords(window_label=window_label, limit=20)
        assert len(top_kws) == 11
        # "python" appears 3 times, "spark" appears 2 times
        assert top_kws[0]["rank"] == 1
        assert top_kws[0]["keyword"] == "python"
        assert top_kws[0]["frequency"] == 3
        assert top_kws[1]["rank"] == 2
        assert top_kws[1]["keyword"] == "spark"
        assert top_kws[1]["frequency"] == 2

        total_pct = sum(item["percentage"] for item in top_kws)
        assert 99.5 <= total_pct <= 100.5

    # Check keyword detail drilldown
    detail = engine.get_keyword_detail("python")
    assert detail["keyword"] == "python"
    assert detail["rank"] == 1
    assert detail["current_frequency"] == 3
    assert detail["windows"]["1m"] == 3
    assert detail["windows"]["1h"] == 3

    # Check trending keywords
    trending = engine.get_trending_keywords(limit=10)
    assert len(trending) > 0
    assert all("change_percentage" in item for item in trending)
