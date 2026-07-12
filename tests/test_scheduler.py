"""
Tests for scheduler.py

Needs Redis running: docker compose up -d
Run with: pytest tests/test_scheduler.py -v
"""

import asyncio
import time
import pytest
import pytest_asyncio

from src.scheduler import Scheduler
from src.producer import Producer
from src.models import JobStatus, RedisKeys


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def setup():
    """Clean Redis and return connected producer + scheduler."""
    producer = Producer()
    await producer.connect()
    await producer._redis.flushdb()

    scheduler = Scheduler()
    await scheduler.connect()

    yield producer, scheduler

    await producer.disconnect()
    await scheduler.disconnect()


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_delayed_job_not_in_waiting_immediately(setup):
    """Delayed job should go to delayed ZSET, not waiting."""
    producer, _ = setup

    job = await producer.add("send_reminder", {"msg": "hello"}, delay=60)

    stats = await producer.queue_stats()
    assert stats["delayed"] == 1
    assert stats["waiting"] == 0
    assert job.status == JobStatus.DELAYED


@pytest.mark.asyncio
async def test_scheduler_promotes_due_job(setup):
    """Scheduler should move a job from delayed to waiting when time comes."""
    producer, scheduler = setup
    await producer._redis.flushdb()

    # add job with delay=0 — it's immediately due
    job = await producer.add("due_now", {}, delay=0)

    # manually set run_at to past so it's overdue
    await producer._redis.hset(
        RedisKeys.job("default", job.id),
        mapping={"run_at": str(time.time() - 10)}
    )
    # add to delayed with past timestamp score
    await producer._redis.zadd(
        RedisKeys.delayed("default"),
        {job.id: time.time() - 10}
    )
    # remove from waiting (producer put it there since delay=0)
    await producer._redis.zrem(RedisKeys.waiting("default"), job.id)

    # run one scheduler tick
    promoted = await scheduler._tick()

    assert promoted == 1

    stats = await producer.queue_stats()
    assert stats["waiting"] == 1
    assert stats["delayed"] == 0


@pytest.mark.asyncio
async def test_scheduler_ignores_future_jobs(setup):
    """Scheduler should NOT promote jobs scheduled for the future."""
    producer, scheduler = setup
    await producer._redis.flushdb()

    # job delayed 60 seconds in the future
    await producer.add("future_job", {}, delay=60)

    promoted = await scheduler._tick()

    assert promoted == 0

    stats = await producer.queue_stats()
    assert stats["delayed"] == 1
    assert stats["waiting"] == 0


@pytest.mark.asyncio
async def test_scheduler_promotes_multiple_jobs(setup):
    """Scheduler should promote all due jobs in one tick."""
    producer, scheduler = setup
    await producer._redis.flushdb()

    # add 3 overdue jobs manually
    for i in range(3):
        job = await producer.add(f"job_{i}", {}, delay=0)
        await producer._redis.zadd(
            RedisKeys.delayed("default"),
            {job.id: time.time() - 10}
        )
        await producer._redis.zrem(RedisKeys.waiting("default"), job.id)
        await producer._redis.hset(
            RedisKeys.job("default", job.id),
            "run_at", str(time.time() - 10)
        )

    promoted = await scheduler._tick()
    assert promoted == 3

    stats = await producer.queue_stats()
    assert stats["waiting"] == 3
    assert stats["delayed"] == 0


@pytest.mark.asyncio
async def test_promoted_job_status_is_waiting(setup):
    """Promoted job should have status=waiting in Redis."""
    producer, scheduler = setup
    await producer._redis.flushdb()

    job = await producer.add("check_status", {}, delay=0)
    await producer._redis.zadd(
        RedisKeys.delayed("default"),
        {job.id: time.time() - 5}
    )
    await producer._redis.zrem(RedisKeys.waiting("default"), job.id)
    await producer._redis.hset(
        RedisKeys.job("default", job.id),
        "status", JobStatus.DELAYED.value
    )

    await scheduler._tick()

    fetched = await producer.get_job(job.id)
    assert fetched.status == JobStatus.WAITING