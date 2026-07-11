"""
Tests for worker.py

Needs Redis running: docker compose up -d
Run with: pytest tests/test_worker.py -v
"""

import asyncio
import pytest
import pytest_asyncio

from src.worker import Worker
from src.producer import Producer
from src.models import JobStatus, RedisKeys


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def redis_clean():
    """Clean Redis before each test."""
    producer = Producer()
    await producer.connect()
    await producer._redis.flushdb()
    yield producer
    await producer.disconnect()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_processes_job(redis_clean):
    """Worker should pick up a job and mark it completed."""
    producer = redis_clean
    job = await producer.add("greet", {"name": "World"})

    async with Worker() as worker:
        results = []

        @worker.job("greet")
        async def greet(j):
            results.append(j.data["name"])
            return {"message": f"Hello {j.data['name']}"}

        # process one job then stop
        await worker._poll()
        await asyncio.sleep(0.1)

    # check job is completed in Redis
    fetched = await producer.get_job(job.id)
    assert fetched.status == JobStatus.COMPLETED
    assert fetched.result == {"message": "Hello World"}
    assert "World" in results


@pytest.mark.asyncio
async def test_worker_retries_on_failure(redis_clean):
    """Failed job should be retried up to max_attempts."""
    producer = redis_clean
    job = await producer.add("flaky", {}, max_attempts=3)

    call_count = 0

    async with Worker() as worker:
        @worker.job("flaky")
        async def flaky(j):
            nonlocal call_count
            call_count += 1
            raise ValueError("something went wrong")

        # process the job (first attempt)
        await worker._poll()
        await asyncio.sleep(0.1)

    # after 1 failure it should be back in waiting for retry
    fetched = await producer.get_job(job.id)
    assert fetched.attempts == 1
    assert fetched.status == JobStatus.WAITING
    assert fetched.error == "something went wrong"


@pytest.mark.asyncio
async def test_job_moves_to_failed_after_max_attempts(redis_clean):
    """Job should move to dead letter queue after exhausting all attempts."""
    producer = redis_clean
    job = await producer.add("always_fails", {}, max_attempts=1)

    async with Worker() as worker:
        @worker.job("always_fails")
        async def always_fails(j):
            raise RuntimeError("permanent error")

        await worker._poll()
        await asyncio.sleep(0.1)

    fetched = await producer.get_job(job.id)
    assert fetched.status == JobStatus.FAILED
    assert fetched.attempts == 1

    # should be in failed sorted set
    in_failed = await producer._redis.zscore(
        RedisKeys.failed("default"), job.id
    )
    assert in_failed is not None


@pytest.mark.asyncio
async def test_worker_lock_prevents_double_processing(redis_clean):
    """Two workers should not process the same job."""
    producer = redis_clean
    await producer.add("count", {})

    process_count = 0

    async def count_handler(j):
        nonlocal process_count
        process_count += 1
        await asyncio.sleep(0.1)
        return {"count": process_count}

    # run two workers simultaneously
    async with Worker(concurrency=1) as w1, Worker(concurrency=1) as w2:
        w1.register("count", count_handler)
        w2.register("count", count_handler)

        await asyncio.gather(
            w1._poll(),
            w2._poll(),
        )
        await asyncio.sleep(0.2)

    # job should only be processed once
    assert process_count == 1


@pytest.mark.asyncio
async def test_handler_registration(redis_clean):
    """Worker should raise error for unregistered job types."""
    producer = redis_clean
    await producer.add("unknown_job_type", {})

    errors = []

    async with Worker() as worker:
        # no handler registered for "unknown_job_type"
        original_fail = worker._fail

        async def capture_fail(job, error):
            errors.append(error)
            await original_fail(job, error)

        worker._fail = capture_fail
        await worker._poll()
        await asyncio.sleep(0.1)

    assert any("No handler registered" in e for e in errors)


@pytest.mark.asyncio
async def test_completed_job_has_result(redis_clean):
    """Completed job should store handler return value as result."""
    producer = redis_clean
    await producer.add("compute", {"x": 10, "y": 20})

    async with Worker() as worker:
        @worker.job("compute")
        async def compute(j):
            return {"sum": j.data["x"] + j.data["y"]}

        await worker._poll()
        await asyncio.sleep(0.1)

    stats = await producer.queue_stats()
    assert stats["completed"] == 1
    assert stats["waiting"] == 0