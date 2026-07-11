"""
worker.py — Job processor

The worker is the heart of the system. It:
1. Listens for jobs using BZPOPMIN (blocking pop) — no CPU wasted polling
2. Acquires a lock using SETNX — prevents two workers grabbing same job
3. Moves job to active list and updates status
4. Executes the registered handler function
5. Marks job completed or failed
6. Retries failed jobs with exponential backoff
7. Moves jobs that exceed max_attempts to dead letter queue

Usage:
    worker = Worker("default")

    @worker.job("send_email")
    async def send_email(job: Job) -> dict:
        return {"sent": True}

    await worker.start()
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Callable, Coroutine

import redis.asyncio as aioredis
from dotenv import load_dotenv

from src.models import Job, JobStatus, RedisKeys

load_dotenv()

logger = logging.getLogger(__name__)

JobHandler = Callable[[Job], Coroutine[Any, Any, dict]]


class Worker:
    """Processes jobs from a Redis queue."""

    def __init__(
        self,
        queue: str = "default",
        concurrency: int | None = None,
        redis_url: str | None = None,
        lock_ttl: int | None = None,
        base_retry_delay: float | None = None,
        max_completed: int | None = None,
    ):
        self.queue = queue
        self.concurrency = concurrency or int(os.getenv("WORKER_CONCURRENCY", 5))
        self._url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._lock_ttl = lock_ttl or int(os.getenv("WORKER_LOCK_TTL", 30))
        self._base_retry_delay = base_retry_delay or float(os.getenv("BASE_RETRY_DELAY", 1))
        self._max_completed = max_completed or int(os.getenv("MAX_COMPLETED_JOBS", 1000))

        self._redis: aioredis.Redis | None = None
        self._handlers: dict[str, JobHandler] = {}
        self._running = False
        self._semaphore: asyncio.Semaphore | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self):
        self._redis = await aioredis.from_url(self._url, decode_responses=True)
        self._semaphore = asyncio.Semaphore(self.concurrency)
        logger.info(f"Worker connected — queue={self.queue} concurrency={self.concurrency}")

    async def disconnect(self):
        self._running = False
        if self._redis:
            await self._redis.aclose()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *_):
        await self.disconnect()

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def register(self, job_name: str, handler: JobHandler):
        """Register a handler function for a job type."""
        self._handlers[job_name] = handler
        logger.info(f"Registered handler for '{job_name}'")

    def job(self, name: str):
        """Decorator version of register().

        Example:
            @worker.job("send_email")
            async def send_email(job: Job) -> dict:
                return {"sent": True}
        """
        def decorator(fn: JobHandler):
            self.register(name, fn)
            return fn
        return decorator

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def start(self):
        """Start the worker loop. Runs until stop() is called."""
        self._running = True
        logger.info(f"Worker started — listening on queue '{self.queue}'")
        while self._running:
            try:
                await self._poll()
            except Exception as e:
                logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(1)

    async def stop(self):
        self._running = False

    async def _poll(self):
        """Block until a job appears, then process it.

        BZPOPMIN atomically:
          1. Blocks until an item exists in the ZSET
          2. Removes and returns the item with the lowest score (highest priority)

        This means by the time we get the job_id, it's already removed
        from the waiting queue — no other worker can get it.
        """
        result = await self._redis.bzpopmin(
            RedisKeys.waiting(self.queue),
            timeout=2
        )

        if result is None:
            return  # timeout — loop and try again

        _, job_id, _ = result  # (key, member, score)

        async with self._semaphore:
            await self._process(job_id)

    # ------------------------------------------------------------------
    # Job processing
    # ------------------------------------------------------------------

    async def _process(self, job_id: str):
        """Full lifecycle of processing one job."""

        # 1. Try to acquire lock — extra safety net for edge cases
        lock_acquired = await self._acquire_lock(job_id)
        if not lock_acquired:
            # Another worker got it in a race condition — put it back
            logger.warning(f"Could not acquire lock for {job_id} — re-queuing")
            job_data = await self._redis.hgetall(RedisKeys.job(self.queue, job_id))
            if job_data:
                job = Job.from_redis_dict(job_data)
                await self._redis.zadd(RedisKeys.waiting(self.queue), {job_id: job.priority})
            return

        try:
            # 2. Move to active + update status
            now = str(time.time())
            pipe = self._redis.pipeline()
            pipe.lpush(RedisKeys.active(self.queue), job_id)
            pipe.hset(RedisKeys.job(self.queue, job_id), mapping={
                "status": JobStatus.ACTIVE.value,
                "started_at": now,
            })
            await pipe.execute()

            # 3. Fetch full job data
            job_data = await self._redis.hgetall(RedisKeys.job(self.queue, job_id))
            if not job_data:
                logger.error(f"Job {job_id} data missing from Redis")
                return

            job = Job.from_redis_dict(job_data)
            job.attempts += 1

            logger.info(f"Processing job {job_id} name={job.name} attempt={job.attempts}/{job.max_attempts}")

            # 4. Execute handler
            try:
                handler = self._handlers.get(job.name)
                if handler is None:
                    raise ValueError(f"No handler registered for job type '{job.name}'")

                result = await handler(job)
                await self._complete(job, result)

            except Exception as e:
                logger.error(f"Job {job_id} failed: {e}")
                await self._fail(job, str(e))

        finally:
            await self._release_lock(job_id)

    async def _complete(self, job: Job, result: dict):
        """Mark job as completed and store result."""
        finished_at = time.time()

        pipe = self._redis.pipeline()
        # remove from active
        pipe.lrem(RedisKeys.active(self.queue), 1, job.id)
        # add to completed sorted set scored by finish time
        pipe.zadd(RedisKeys.completed(self.queue), {job.id: finished_at})
        # update job fields
        pipe.hset(RedisKeys.job(self.queue, job.id), mapping={
            "status": JobStatus.COMPLETED.value,
            "finished_at": str(finished_at),
            "result": json.dumps(result or {}),
            "attempts": str(job.attempts),
        })
        await pipe.execute()

        # trim oldest completed jobs beyond the limit
        count = await self._redis.zcard(RedisKeys.completed(self.queue))
        if count > self._max_completed:
            overflow = await self._redis.zrange(
                RedisKeys.completed(self.queue), 0, count - self._max_completed - 1
            )
            if overflow:
                pipe = self._redis.pipeline()
                for old_id in overflow:
                    pipe.delete(RedisKeys.job(self.queue, old_id))
                pipe.zremrangebyrank(RedisKeys.completed(self.queue), 0, count - self._max_completed - 1)
                await pipe.execute()

        logger.info(f"Job {job.id} completed successfully")

    async def _fail(self, job: Job, error: str):
        """Handle failure — retry with backoff or move to dead letter queue."""
        finished_at = time.time()

        # exponential backoff: 1s → 2s → 4s → 8s...
        backoff = self._base_retry_delay * (2 ** (job.attempts - 1))
        retry_priority = job.priority + backoff

        pipe = self._redis.pipeline()
        pipe.lrem(RedisKeys.active(self.queue), 1, job.id)
        pipe.hset(RedisKeys.job(self.queue, job.id), mapping={
            "attempts": str(job.attempts),
            "error": error,
            "finished_at": str(finished_at),
        })

        if job.attempts < job.max_attempts:
            # requeue for retry
            pipe.hset(RedisKeys.job(self.queue, job.id), mapping={"status": JobStatus.WAITING.value})
            pipe.zadd(RedisKeys.waiting(self.queue), {job.id: retry_priority})
            await pipe.execute()
            logger.info(f"Job {job.id} retry {job.attempts}/{job.max_attempts} backoff={backoff}s")
        else:
            # dead letter queue
            pipe.hset(RedisKeys.job(self.queue, job.id), mapping={"status": JobStatus.FAILED.value})
            pipe.zadd(RedisKeys.failed(self.queue), {job.id: finished_at})
            await pipe.execute()
            logger.warning(f"Job {job.id} exhausted all {job.max_attempts} attempts → dead letter queue")

    # ------------------------------------------------------------------
    # Lock helpers
    # ------------------------------------------------------------------

    async def _acquire_lock(self, job_id: str) -> bool:
        """Acquire an exclusive lock using SET NX EX (atomic)."""
        acquired = await self._redis.set(
            RedisKeys.lock(self.queue, job_id),
            "locked",
            nx=True,
            ex=self._lock_ttl,
        )
        return acquired is not None

    async def _release_lock(self, job_id: str):
        """Release the job lock."""
        await self._redis.delete(RedisKeys.lock(self.queue, job_id))