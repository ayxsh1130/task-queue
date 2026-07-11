"""
producer.py — Add jobs to the Redis queue

This is the public API for creating jobs. Other parts of your app
(API handlers, cron jobs, webhooks) call this to enqueue work.

Two types of jobs:
  - Immediate: added to the waiting ZSET, workers pick them up right away
  - Delayed:   added to the delayed ZSET with a future timestamp score,
               the scheduler promotes them to waiting when their time comes

All Redis operations use Lua scripts to ensure atomicity — meaning
the HSET (store job data) + ZADD (add to queue) happen together or not at all.
No other process can see a half-created job.
"""

from __future__ import annotations

import time
import os
from typing import Any

import redis.asyncio as aioredis
from dotenv import load_dotenv

from src.models import Job, JobStatus, RedisKeys

load_dotenv()

# ---------------------------------------------------------------------------
# Lua scripts — atomic multi-step Redis operations
# ---------------------------------------------------------------------------

# Adds an immediate job:
#   1. HSET  — store job data as a hash
#   2. ZADD  — add job ID to waiting sorted set with priority as score
_ADD_JOB_LUA = """
local job_key     = KEYS[1]  -- queue:{name}:jobs:{id}
local waiting_key = KEYS[2]  -- queue:{name}:waiting

local job_data    = ARGV[1]  -- flattened job fields (alternating key/value)
local priority    = tonumber(ARGV[2])
local job_id      = ARGV[3]

-- store job fields (ARGV[1] is a flat list: field1, val1, field2, val2 ...)
redis.call('HSET', job_key, unpack(cjson.decode(job_data)))

-- add to waiting queue scored by priority
redis.call('ZADD', waiting_key, priority, job_id)

return job_id
"""

# Adds a delayed job:
#   1. HSET  — store job data
#   2. ZADD  — add to delayed set scored by run_at timestamp
_ADD_DELAYED_LUA = """
local job_key     = KEYS[1]  -- queue:{name}:jobs:{id}
local delayed_key = KEYS[2]  -- queue:{name}:delayed

local job_data    = ARGV[1]
local run_at      = tonumber(ARGV[2])
local job_id      = ARGV[3]

redis.call('HSET', job_key, unpack(cjson.decode(job_data)))
redis.call('ZADD', delayed_key, run_at, job_id)

return job_id
"""


# ---------------------------------------------------------------------------
# Producer class
# ---------------------------------------------------------------------------

class Producer:
    """Pushes jobs into the queue.

    Usage:
        async with Producer() as p:
            job = await p.add("send_email", {"to": "x@example.com"})
            print(job.id)
    """

    def __init__(self, redis_url: str | None = None):
        self._url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._redis: aioredis.Redis | None = None
        self._add_script = None
        self._add_delayed_script = None

    async def connect(self):
        """Open Redis connection and register Lua scripts."""
        self._redis = await aioredis.from_url(self._url, decode_responses=True)
        # registering scripts compiles them on the Redis server — faster repeated calls
        self._add_script = self._redis.register_script(_ADD_JOB_LUA)
        self._add_delayed_script = self._redis.register_script(_ADD_DELAYED_LUA)

    async def disconnect(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.aclose()

    # context manager support — lets you use `async with Producer() as p:`
    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *_):
        await self.disconnect()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def add(
        self,
        name: str,
        data: dict[str, Any] | None = None,
        *,
        queue: str = "default",
        priority: int = 10,
        max_attempts: int = 3,
        delay: float = 0,
    ) -> Job:
        """Add a job to the queue.

        Args:
            name:         Job type identifier e.g. "send_email", "resize_image"
            data:         Payload passed to the worker
            queue:        Which queue to add to (default: "default")
            priority:     Lower = picked first. 0=critical, 10=normal, 100=low
            max_attempts: How many times to retry before giving up
            delay:        Seconds from now before the job becomes runnable

        Returns:
            The created Job object (with id, status, timestamps filled in)
        """
        self._ensure_connected()

        job = Job(
            name=name,
            queue=queue,
            data=data or {},
            priority=priority,
            max_attempts=max_attempts,
            delay=delay,
        )

        if delay > 0:
            return await self._add_delayed(job)
        return await self._add_immediate(job)

    async def get_job(self, job_id: str, queue: str = "default") -> Job | None:
        """Fetch a job by ID. Returns None if not found."""
        self._ensure_connected()
        data = await self._redis.hgetall(RedisKeys.job(queue, job_id))
        if not data:
            return None
        return Job.from_redis_dict(data)

    async def queue_stats(self, queue: str = "default") -> dict[str, int]:
        """Return counts for each queue state — useful for monitoring."""
        self._ensure_connected()
        pipe = self._redis.pipeline()
        pipe.zcard(RedisKeys.waiting(queue))
        pipe.llen(RedisKeys.active(queue))
        pipe.zcard(RedisKeys.completed(queue))
        pipe.zcard(RedisKeys.failed(queue))
        pipe.zcard(RedisKeys.delayed(queue))
        waiting, active, completed, failed, delayed = await pipe.execute()
        return {
            "waiting": waiting,
            "active": active,
            "completed": completed,
            "failed": failed,
            "delayed": delayed,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _add_immediate(self, job: Job) -> Job:
        """Push job to the waiting sorted set."""
        import json
        job.status = JobStatus.WAITING
        redis_dict = job.to_redis_dict()

        # Lua expects a JSON array of alternating key/value pairs
        flat = []
        for k, v in redis_dict.items():
            flat.extend([k, v])

        await self._add_script(
            keys=[
                RedisKeys.job(job.queue, job.id),
                RedisKeys.waiting(job.queue),
            ],
            args=[json.dumps(flat), job.priority, job.id],
        )
        return job

    async def _add_delayed(self, job: Job) -> Job:
        """Push job to the delayed sorted set scored by scheduled run time."""
        import json
        job.status = JobStatus.DELAYED
        job.run_at = time.time() + job.delay
        redis_dict = job.to_redis_dict()

        flat = []
        for k, v in redis_dict.items():
            flat.extend([k, v])

        await self._add_delayed_script(
            keys=[
                RedisKeys.job(job.queue, job.id),
                RedisKeys.delayed(job.queue),
            ],
            args=[json.dumps(flat), job.run_at, job.id],
        )
        return job

    def _ensure_connected(self):
        if not self._redis:
            raise RuntimeError("Producer not connected. Use 'async with Producer()' or call connect() first.")