"""
Tests for producer.py

These are integration tests — they need a real Redis instance running.
Start Redis first: docker compose up -d

Run with: pytest tests/test_producer.py -v
"""

import time
import pytest
import pytest_asyncio
import asyncio

from src.producer import Producer
from src.models import JobStatus


# ---------------------------------------------------------------------------
# Test setup
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def producer():
    """Fresh producer connected to Redis for each test."""
    async with Producer() as p:
        # clean up any leftover test jobs before each test
        await p._redis.flushdb()
        yield p


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_immediate_job(producer):
    """Adding a job should store it in Redis and return it with correct status."""
    job = await producer.add("send_email", {"to": "test@example.com"})

    assert job.id is not None
    assert job.name == "send_email"
    assert job.status == JobStatus.WAITING
    assert job.data == {"to": "test@example.com"}


@pytest.mark.asyncio
async def test_job_stored_in_redis(producer):
    """Job added by producer should be retrievable from Redis."""
    job = await producer.add("send_email", {"to": "test@example.com"})
    fetched = await producer.get_job(job.id)

    assert fetched is not None
    assert fetched.id == job.id
    assert fetched.name == job.name
    assert fetched.data == job.data


@pytest.mark.asyncio
async def test_job_in_waiting_queue(producer):
    """Added job ID should appear in the waiting sorted set."""
    job = await producer.add("resize_image", {"url": "https://example.com/img.jpg"})

    from src.models import RedisKeys
    score = await producer._redis.zscore(RedisKeys.waiting("default"), job.id)
    assert score is not None  # job is in the waiting ZSET
    assert score == job.priority  # scored by priority


@pytest.mark.asyncio
async def test_priority_ordering(producer):
    """Higher priority jobs (lower score) should appear first in the queue."""
    low  = await producer.add("low_priority_job",  priority=100)
    high = await producer.add("high_priority_job", priority=0)
    normal = await producer.add("normal_job",      priority=10)

    from src.models import RedisKeys
    # ZRANGE returns IDs sorted by score ascending (lowest score first)
    order = await producer._redis.zrange(RedisKeys.waiting("default"), 0, -1)

    assert order[0] == high.id    # priority 0 — first
    assert order[1] == normal.id  # priority 10 — second
    assert order[2] == low.id     # priority 100 — last


@pytest.mark.asyncio
async def test_delayed_job(producer):
    """Delayed job should go to delayed ZSET, not waiting."""
    job = await producer.add("send_reminder", {"msg": "hello"}, delay=60)

    assert job.status == JobStatus.DELAYED
    assert job.run_at is not None
    assert job.run_at > time.time()  # scheduled in the future

    from src.models import RedisKeys
    # should NOT be in waiting
    in_waiting = await producer._redis.zscore(RedisKeys.waiting("default"), job.id)
    assert in_waiting is None

    # should be in delayed
    in_delayed = await producer._redis.zscore(RedisKeys.delayed("default"), job.id)
    assert in_delayed is not None


@pytest.mark.asyncio
async def test_queue_stats(producer):
    """Stats should reflect correct counts after adding jobs."""
    await producer.add("job1")
    await producer.add("job2")
    await producer.add("job3", delay=30)

    stats = await producer.queue_stats()
    assert stats["waiting"] == 2
    assert stats["delayed"] == 1
    assert stats["active"] == 0
    assert stats["completed"] == 0
    assert stats["failed"] == 0


@pytest.mark.asyncio
async def test_multiple_queues(producer):
    """Jobs in different queues should be independent."""
    await producer.add("send_email", queue="emails")
    await producer.add("send_sms",   queue="notifications")

    email_stats = await producer.queue_stats("emails")
    notif_stats  = await producer.queue_stats("notifications")

    assert email_stats["waiting"] == 1
    assert notif_stats["waiting"] == 1