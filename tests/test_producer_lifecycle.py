"""
Member 1 & Member 6 — Producer Lifecycle & Data Generation Tests.
Verifies:
- Data generation across all 6 categories (Technology, Big Data, Artificial Intelligence,
  Data Science, News, Product Reviews)
- Configurable rate validation and clamping
- Full lifecycle sequence:
  STOPPED -> START -> RUNNING -> PAUSE -> PAUSED -> RESUME -> RUNNING -> STOP -> STOPPED -> START again
"""

import asyncio
import pytest

from producer.config import ProducerConfig
from producer.lifecycle import (
    InvalidStateTransitionError,
    StreamLifecycleManager,
    StreamState,
)
from producer.producer import StreamingDataProducer
from producer.sample_datasets import CATEGORIES, generate_message


def test_sample_datasets_all_categories():
    for category in CATEGORIES:
        msg = generate_message(category=category, sequence_id=1)
        assert msg["category"] == category
        assert isinstance(msg["text"], str) and len(msg["text"]) > 10
        assert "id" in msg
        assert "timestamp" in msg


def test_config_rate_clamping_and_category_validation():
    cfg = ProducerConfig()
    assert cfg.validate_and_clamp_rate(0.1) == cfg.min_rate
    assert cfg.validate_and_clamp_rate(500.0) == cfg.max_rate
    assert cfg.validate_and_clamp_rate(15.0) == 15.0

    assert cfg.validate_category("big data") == "Big Data"
    assert cfg.validate_category("Technology") == "Technology"
    with pytest.raises(ValueError):
        cfg.validate_category("UnknownCategory")


def test_lifecycle_state_machine_full_sequence():
    """
    Test Requirement 3.D & 8.C:
    STOPPED -> START -> RUNNING -> PAUSE -> PAUSED -> RESUME -> RUNNING -> STOP -> STOPPED
    Then START again without restarting.
    """
    manager = StreamLifecycleManager(initial_rate=10.0, initial_category="Technology")
    assert manager.state == StreamState.STOPPED

    # Cannot pause or resume from STOPPED
    with pytest.raises(InvalidStateTransitionError):
        manager.pause()
    with pytest.raises(InvalidStateTransitionError):
        manager.resume()

    # START -> RUNNING
    assert manager.start(rate=12.0, category="Big Data") == StreamState.RUNNING
    assert manager.state == StreamState.RUNNING
    assert manager.rate == 12.0
    assert manager.category == "Big Data"

    # PAUSE -> PAUSED
    assert manager.pause() == StreamState.PAUSED
    assert manager.state == StreamState.PAUSED

    # RESUME -> RUNNING
    assert manager.resume() == StreamState.RUNNING
    assert manager.state == StreamState.RUNNING

    # STOP -> STOPPED
    assert manager.stop() == StreamState.STOPPED
    assert manager.state == StreamState.STOPPED

    # START AGAIN without restarting infrastructure
    assert manager.start(rate=20.0, category="Artificial Intelligence") == StreamState.RUNNING
    assert manager.state == StreamState.RUNNING
    assert manager.rate == 20.0
    assert manager.category == "Artificial Intelligence"


@pytest.mark.asyncio
async def test_streaming_producer_background_task_lifecycle():
    producer = StreamingDataProducer()
    received_messages = []

    async def on_msg(msg):
        received_messages.append(msg)

    producer.register_callback(on_msg)

    # Start stream at 20 msg/sec
    status = await producer.start_stream(rate=20.0, category="Technology")
    assert status["state"] == "RUNNING"
    await asyncio.sleep(0.25)
    assert len(received_messages) >= 2

    # Pause stream
    status = await producer.pause_stream()
    assert status["state"] == "PAUSED"
    count_at_pause = len(received_messages)
    await asyncio.sleep(0.2)
    # Should not continue producing after pause settles
    assert len(received_messages) <= count_at_pause + 1

    # Resume stream
    status = await producer.resume_stream()
    assert status["state"] == "RUNNING"
    await asyncio.sleep(0.2)
    assert len(received_messages) > count_at_pause

    # Stop stream
    status = await producer.stop_stream()
    assert status["state"] == "STOPPED"

    # Start again cleanly
    status = await producer.start_stream(rate=15.0, category="Data Science")
    assert status["state"] == "RUNNING"
    await asyncio.sleep(0.15)
    status = await producer.stop_stream()
    assert status["state"] == "STOPPED"
