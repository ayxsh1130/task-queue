"""
Tests for reaper.py

Needs Redis running: docker compose up -d
Run with: pytest tests/test_reaper.py -v
"""

import asyncio
import pytest
import pytest_asyncio

from src.reaper import Reaper
from src.producer import Producer
from src.models import JobStatus, RedisKeys


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def setup():
    """Clean Redis and return connected producer + reaper."""
    producer = Producer()
    await producer.connect()
    await producer._redis.flushdb()

    reaper = Reaper()
    await reaper.connect()

    yield producer, reaper

    await producer.disconnect()
    await reaper.disconnect()


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_reaper_ignores_jobs_with_active_lock(setup):
    """Reaper should NOT touch jobs whose worker lock is still alive."""
    producer, reaper = setup
    await producer._redis.flushdb()

    job = await producer.add("healthy_job", {})

    # simulate worker picking up job — move to active + set lock
    await producer._redis.zrem(RedisKeys.waiting("default"), job.id)
    await producer._redis.lpush(RedisKeys.active("default"), job.id)
    await producer._redis.hset(
        RedisKeys.job("default", job.id),
        "status", JobStatus.ACTIVE.value
    )
    # set lock with long TTL — worker is alive
    await producer._redis.set(
        RedisKeys.lock("default", job.id),
        "locked",
        ex=30
    )

    recovered = await reaper._scan()

    assert recovered == 0  # should not touch this job

    # job should still be active
    fetched = await producer.get_job(job.id)
    assert fetched.status == JobStatus.ACTIVE


@pytest.mark.asyncio
async def test_reaper_recovers_stale_job(setup):
    """Reaper should requeue a job whose worker lock has expired."""
    producer, reaper = setup
    await producer._redis.flushdb()

    job = await producer.add("stale_job", {})

    # simulate worker picking up job — move to active, NO lock set
    await producer._redis.zrem(RedisKeys.waiting("default"), job.id)
    await producer._redis.lpush(RedisKeys.active("default"), job.id)
    await producer._redis.hset(
        RedisKeys.job("default", job.id),
        "status", JobStatus.ACTIVE.value
    )
    # deliberately NOT setting a lock — simulates expired/crashed worker

    recovered = await reaper._scan()

    assert recovered == 1

    # job should be back in waiting
    fetched = await producer.get_job(job.id)
    assert fetched.status == JobStatus.WAITING

    stats = await producer.queue_stats()
    assert stats["waiting"] == 1
    assert stats["active"] == 0


@pytest.mark.asyncio
async def test_reaper_recovers_multiple_stale_jobs(setup):
    """Reaper should recover all stale jobs in one scan."""
    producer, reaper = setup
    await producer._redis.flushdb()

    # create 3 stale jobs
    for i in range(3):
        job = await producer.add(f"stale_{i}", {})
        await producer._redis.zrem(RedisKeys.waiting("default"), job.id)
        await producer._redis.lpush(RedisKeys.active("default"), job.id)
        await producer._redis.hset(
            RedisKeys.job("default", job.id),
            "status", JobStatus.ACTIVE.value
        )
        # no lock set — simulates crashed worker

    recovered = await reaper._scan()

    assert recovered == 3

    stats = await producer.queue_stats()
    assert stats["waiting"] == 3
    assert stats["active"] == 0


@pytest.mark.asyncio
async def test_reaper_mixed_healthy_and_stale(setup):
    """Reaper should only recover stale jobs, leave healthy ones alone."""
    producer, reaper = setup
    await producer._redis.flushdb()

    # healthy job — has lock
    healthy = await producer.add("healthy", {})
    await producer._redis.zrem(RedisKeys.waiting("default"), healthy.id)
    await producer._redis.lpush(RedisKeys.active("default"), healthy.id)
    await producer._redis.hset(
        RedisKeys.job("default", healthy.id),
        "status", JobStatus.ACTIVE.value
    )
    await producer._redis.set(
        RedisKeys.lock("default", healthy.id), "locked", ex=30
    )

    # stale job — no lock
    stale = await producer.add("stale", {})
    await producer._redis.zrem(RedisKeys.waiting("default"), stale.id)
    await producer._redis.lpush(RedisKeys.active("default"), stale.id)
    await producer._redis.hset(
        RedisKeys.job("default", stale.id),
        "status", JobStatus.ACTIVE.value
    )
    # no lock

    recovered = await reaper._scan()

    assert recovered == 1  # only the stale one

    healthy_job = await producer.get_job(healthy.id)
    stale_job = await producer.get_job(stale.id)

    assert healthy_job.status == JobStatus.ACTIVE   # untouched
    assert stale_job.status == JobStatus.WAITING     # recovered


@pytest.mark.asyncio
async def test_reaper_is_stale_check(setup):
    """_is_stale should correctly identify stale vs healthy jobs."""
    producer, reaper = setup
    await producer._redis.flushdb()

    job = await producer.add("test_job", {})

    # no lock — should be stale
    assert await reaper._is_stale("default", job.id) is True

    # set lock — should not be stale
    await producer._redis.set(
        RedisKeys.lock("default", job.id), "locked", ex=30
    )
    assert await reaper._is_stale("default", job.id) is False