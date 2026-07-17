
"""
api.py — Complete REST + WebSocket API for tq-engine

Endpoints:
    GET  /                          → health check
    GET  /queues                    → list all queues
    GET  /queues/{name}/stats       → stats for one queue
    GET  /queues/{name}/jobs        → list jobs by status
    POST /queues/{name}/pause       → pause a queue
    POST /queues/{name}/resume      → resume a queue
    DELETE /queues/{name}           → flush a queue

    GET  /jobs/{queue}/{id}         → full job detail
    POST /jobs/{queue}/{id}/retry   → re-queue a failed job
    GET  /jobs/search               → search jobs by name

    GET  /workers                   → list active workers
    GET  /metrics                   → prometheus-style metrics

    WS   /ws                        → real-time data stream

Run with:
    uvicorn src.api:app --reload --port 8000
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Optional

import redis.asyncio as aioredis
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.models import Job, JobStatus, RedisKeys

load_dotenv()

app = FastAPI(
    title="tq-engine API",
    description="Distributed task queue REST + WebSocket API",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# CORS — allows React dev server to talk to FastAPI
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------

async def get_redis() -> aioredis.Redis:
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return await aioredis.from_url(url, decode_responses=True)

# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        disconnected = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)

manager = ConnectionManager()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class QueueStats(BaseModel):
    name: str
    waiting: int
    active: int
    completed: int
    failed: int
    delayed: int
    total: int
    paused: bool


class JobResponse(BaseModel):
    id: str
    name: str
    queue: str
    status: str
    data: dict
    result: dict | None
    error: str | None
    attempts: int
    max_attempts: int
    priority: int
    created_at: float
    started_at: float | None
    finished_at: float | None
    run_at: float | None


class RetryResponse(BaseModel):
    job_id: str
    message: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def job_to_response(job: Job) -> JobResponse:
    return JobResponse(
        id=job.id,
        name=job.name,
        queue=job.queue,
        status=job.status.value,
        data=job.data,
        result=job.result,
        error=job.error,
        attempts=job.attempts,
        max_attempts=job.max_attempts,
        priority=job.priority,
        created_at=job.created_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        run_at=job.run_at,
    )


async def get_queue_stats(redis: aioredis.Redis, name: str) -> QueueStats:
    pipe = redis.pipeline()
    pipe.zcard(RedisKeys.waiting(name))
    pipe.llen(RedisKeys.active(name))
    pipe.zcard(RedisKeys.completed(name))
    pipe.zcard(RedisKeys.failed(name))
    pipe.zcard(RedisKeys.delayed(name))
    pipe.exists(f"queue:{name}:paused")
    waiting, active, completed, failed, delayed, paused = await pipe.execute()
    return QueueStats(
        name=name,
        waiting=waiting,
        active=active,
        completed=completed,
        failed=failed,
        delayed=delayed,
        total=waiting + active + completed + failed + delayed,
        paused=bool(paused),
    )


async def get_all_queue_names(redis: aioredis.Redis) -> list[str]:
    queues = set()
    for pattern in ["queue:*:waiting", "queue:*:active", "queue:*:completed", "queue:*:failed"]:
        keys = await redis.keys(pattern)
        for key in keys:
            parts = key.split(":")
            if len(parts) == 3:
                queues.add(parts[1])
    return sorted(queues)


async def get_worker_status(redis: aioredis.Redis) -> list[dict]:
    """Find active workers by scanning lock keys."""
    lock_keys = await redis.keys("queue:*:lock:*")
    workers = []
    for key in lock_keys:
        parts = key.split(":")
        if len(parts) == 4:
            queue   = parts[1]
            job_id  = parts[3]
            ttl     = await redis.ttl(key)
            job_data = await redis.hgetall(RedisKeys.job(queue, job_id))
            job_name = job_data.get("name", "unknown") if job_data else "unknown"
            workers.append({
                "queue":    queue,
                "job_id":   job_id,
                "job_name": job_name,
                "lock_ttl": ttl,
            })
    return workers


# ---------------------------------------------------------------------------
# Background task — broadcasts stats via WebSocket every second
# ---------------------------------------------------------------------------

async def broadcast_loop():
    """Push queue stats to all connected WebSocket clients every second."""
    while True:
        try:
            if manager.active:
                redis = await get_redis()
                try:
                    queue_names = await get_all_queue_names(redis)
                    stats = []
                    for name in queue_names:
                        s = await get_queue_stats(redis, name)
                        stats.append(s.model_dump())

                    workers = await get_worker_status(redis)

                    # throughput — jobs completed in last second
                    throughput_key = "tq:throughput"
                    prev = await redis.get(throughput_key)
                    total_completed = sum(s["completed"] for s in stats)
                    throughput = total_completed - int(prev) if prev else 0
                    await redis.set(throughput_key, total_completed, ex=10)

                    await manager.broadcast({
                        "type":       "stats",
                        "queues":     stats,
                        "workers":    workers,
                        "throughput": max(0, throughput),
                        "timestamp":  time.time(),
                    })
                finally:
                    await redis.aclose()
        except Exception as e:
            pass
        await asyncio.sleep(1)


@app.on_event("startup")
async def startup():
    asyncio.create_task(broadcast_loop())


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
async def health():
    return {
        "status":  "ok",
        "service": "tq-engine",
        "version": "1.0.0",
        "time":    time.time(),
    }


@app.get("/queues", response_model=list[QueueStats])
async def list_queues():
    """List all queues with stats."""
    redis = await get_redis()
    try:
        names = await get_all_queue_names(redis)
        return [await get_queue_stats(redis, n) for n in names]
    finally:
        await redis.aclose()


@app.get("/queues/{name}/stats", response_model=QueueStats)
async def queue_stats(name: str):
    """Get stats for a specific queue."""
    redis = await get_redis()
    try:
        return await get_queue_stats(redis, name)
    finally:
        await redis.aclose()


@app.get("/queues/{name}/jobs", response_model=list[JobResponse])
async def list_jobs(
    name: str,
    status: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List jobs in a queue filtered by status."""
    redis = await get_redis()
    try:
        if status == "waiting" or status is None:
            ids = await redis.zrange(RedisKeys.waiting(name), offset, offset + limit - 1)
        elif status == "active":
            ids = await redis.lrange(RedisKeys.active(name), offset, offset + limit - 1)
        elif status == "completed":
            ids = await redis.zrange(RedisKeys.completed(name), offset, offset + limit - 1, rev=True)
        elif status == "failed":
            ids = await redis.zrange(RedisKeys.failed(name), offset, offset + limit - 1, rev=True)
        elif status == "delayed":
            ids = await redis.zrange(RedisKeys.delayed(name), offset, offset + limit - 1)
        else:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

        jobs = []
        for job_id in ids:
            data = await redis.hgetall(RedisKeys.job(name, job_id))
            if data:
                jobs.append(job_to_response(Job.from_redis_dict(data)))
        return jobs
    finally:
        await redis.aclose()


@app.post("/queues/{name}/pause")
async def pause_queue(name: str):
    """Pause a queue — workers will stop picking up new jobs."""
    redis = await get_redis()
    try:
        await redis.set(f"queue:{name}:paused", "1")
        return {"message": f"Queue '{name}' paused"}
    finally:
        await redis.aclose()


@app.post("/queues/{name}/resume")
async def resume_queue(name: str):
    """Resume a paused queue."""
    redis = await get_redis()
    try:
        await redis.delete(f"queue:{name}:paused")
        return {"message": f"Queue '{name}' resumed"}
    finally:
        await redis.aclose()


@app.delete("/queues/{name}")
async def flush_queue(name: str):
    """Flush all jobs in a queue. Irreversible."""
    redis = await get_redis()
    try:
        pipe = redis.pipeline()
        pipe.delete(RedisKeys.waiting(name))
        pipe.delete(RedisKeys.active(name))
        pipe.delete(RedisKeys.completed(name))
        pipe.delete(RedisKeys.failed(name))
        pipe.delete(RedisKeys.delayed(name))
        await pipe.execute()
        return {"message": f"Queue '{name}' flushed"}
    finally:
        await redis.aclose()


@app.get("/jobs/{queue}/{job_id}", response_model=JobResponse)
async def get_job(queue: str, job_id: str):
    """Get full details of a specific job."""
    redis = await get_redis()
    try:
        data = await redis.hgetall(RedisKeys.job(queue, job_id))
        if not data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        return job_to_response(Job.from_redis_dict(data))
    finally:
        await redis.aclose()


@app.post("/jobs/{queue}/{job_id}/retry", response_model=RetryResponse)
async def retry_job(queue: str, job_id: str):
    """Re-queue a failed job."""
    redis = await get_redis()
    try:
        data = await redis.hgetall(RedisKeys.job(queue, job_id))
        if not data:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        job = Job.from_redis_dict(data)
        if job.status != JobStatus.FAILED:
            raise HTTPException(
                status_code=400,
                detail=f"Job is {job.status.value}, not failed"
            )

        pipe = redis.pipeline()
        pipe.zrem(RedisKeys.failed(queue), job_id)
        pipe.zadd(RedisKeys.waiting(queue), {job_id: job.priority})
        pipe.hset(RedisKeys.job(queue, job_id), mapping={
            "status":      JobStatus.WAITING.value,
            "attempts":    "0",
            "error":       "",
            "result":      "",
            "started_at":  "",
            "finished_at": "",
        })
        await pipe.execute()
        return RetryResponse(job_id=job_id, message=f"Job {job_id} re-queued")
    finally:
        await redis.aclose()


@app.get("/workers")
async def list_workers():
    """List all active workers."""
    redis = await get_redis()
    try:
        return await get_worker_status(redis)
    finally:
        await redis.aclose()


@app.get("/metrics")
async def metrics():
    """Prometheus-style metrics endpoint."""
    redis = await get_redis()
    try:
        names  = await get_all_queue_names(redis)
        lines  = ["# tq-engine metrics"]
        for name in names:
            s = await get_queue_stats(redis, name)
            lines.append(f'tq_jobs_waiting{{queue="{name}"}} {s.waiting}')
            lines.append(f'tq_jobs_active{{queue="{name}"}} {s.active}')
            lines.append(f'tq_jobs_completed{{queue="{name}"}} {s.completed}')
            lines.append(f'tq_jobs_failed{{queue="{name}"}} {s.failed}')
            lines.append(f'tq_jobs_delayed{{queue="{name}"}} {s.delayed}')
        return "\n".join(lines)
    finally:
        await redis.aclose()


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint — streams real-time queue stats to connected clients."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)