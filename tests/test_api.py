"""
Tests for api.py

Needs Redis running: docker compose up -d
Run with: pytest tests/test_api.py -v
"""

import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from src.api import app
from src.producer import Producer
from src.models import JobStatus, RedisKeys


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest_asyncio.fixture
async def client():
    """HTTP test client + clean Redis."""
    producer = Producer()
    await producer.connect()
    await producer._redis.flushdb()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test"
    ) as c:
        yield c, producer

    await producer.disconnect()


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_empty_queue(client):
    """Stats should return empty list when no queues exist."""
    c, _ = client
    res = await c.get("/stats")
    assert res.status_code == 200
    assert res.json() == []


@pytest.mark.asyncio
async def test_stats_with_jobs(client):
    """Stats should reflect correct counts after adding jobs."""
    c, producer = client
    await producer.add("send_email", queue="emails")
    await producer.add("send_sms",   queue="emails")
    await producer.add("send_push",  delay=60, queue="emails")

    res = await c.get("/stats/emails")
    assert res.status_code == 200
    data = res.json()
    assert data["waiting"] == 2
    assert data["delayed"] == 1
    assert data["active"] == 0
    assert data["queue"] == "emails"


@pytest.mark.asyncio
async def test_get_job(client):
    """Should return full job detail by ID."""
    c, producer = client
    job = await producer.add("resize_image", {"url": "https://example.com/img.jpg"})

    res = await c.get(f"/jobs/default/{job.id}")
    assert res.status_code == 200
    data = res.json()
    assert data["id"] == job.id
    assert data["name"] == "resize_image"
    assert data["data"] == {"url": "https://example.com/img.jpg"}
    assert data["status"] == "waiting"


@pytest.mark.asyncio
async def test_get_job_not_found(client):
    """Should return 404 for non-existent job."""
    c, _ = client
    res = await c.get("/jobs/default/nonexistent-id")
    assert res.status_code == 404


@pytest.mark.asyncio
async def test_list_jobs(client):
    """Should list jobs filtered by status."""
    c, producer = client
    await producer.add("job_a")
    await producer.add("job_b")

    res = await c.get("/jobs/default?status=waiting")
    assert res.status_code == 200
    jobs = res.json()
    assert len(jobs) >= 2
    assert all(j["status"] == "waiting" for j in jobs)


@pytest.mark.asyncio
async def test_retry_failed_job(client):
    """Should re-queue a failed job back to waiting."""
    c, producer = client
    job = await producer.add("failing_job")

    # manually mark as failed
    redis = producer._redis
    await redis.zrem(RedisKeys.waiting("default"), job.id)
    await redis.zadd(RedisKeys.failed("default"), {job.id: 1.0})
    await redis.hset(RedisKeys.job("default", job.id), mapping={
        "status": JobStatus.FAILED.value,
        "error": "timeout",
    })

    res = await c.post(f"/jobs/default/{job.id}/retry")
    assert res.status_code == 200
    assert "re-queued" in res.json()["message"]

    # verify it's back in waiting
    check = await c.get(f"/jobs/default/{job.id}")
    assert check.json()["status"] == "waiting"


@pytest.mark.asyncio
async def test_retry_non_failed_job(client):
    """Should reject retry on a non-failed job."""
    c, producer = client
    job = await producer.add("active_job")

    res = await c.post(f"/jobs/default/{job.id}/retry")
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_dashboard_loads(client):
    """Dashboard should return HTML."""
    c, _ = client
    res = await c.get("/")
    assert res.status_code == 200
    assert "text/html" in res.headers["content-type"]
    assert "Task Queue Dashboard" in res.text


@pytest.mark.asyncio
async def test_flush_queue(client):
    """Flushing a queue should remove all jobs."""
    c, producer = client
    await producer.add("job1")
    await producer.add("job2")

    res = await c.delete("/queues/default")
    assert res.status_code == 200

    stats = await c.get("/stats/default")
    data = stats.json()
    assert data["waiting"] == 0
    assert data["total"] == 0