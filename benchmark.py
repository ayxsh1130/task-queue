"""
benchmark.py — Comprehensive benchmark suite for tq-engine

Tests:
  1.  Enqueue throughput
  2.  Processing throughput
  3.  Concurrent load (zero duplicates, zero loss)
  4.  Crash recovery (reaper test)
  5.  Retry accuracy
  6.  Latency distribution (P50, P95, P99)
  7.  Worker scaling (1 -> 2 -> 5 -> 10 workers)
  8.  Sustained load
  9.  Memory usage
  10. Breaking point

Workload:
  - Compute jobs : CPU work (fibonacci + sorting)
  - DB jobs      : real PostgreSQL writes
  No external HTTP calls — measures queue performance, not network latency.

Run:
  python benchmark.py
  python benchmark.py --jobs 50000 --workers 5
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import time
from typing import Any

import asyncpg
import redis.asyncio as aioredis
from dotenv import load_dotenv

from src.database import get_pool, setup_schema, cleanup_benchmark_data
from src.models import Job, RedisKeys
from src.producer import Producer
from src.reaper import Reaper
from src.worker import Worker

load_dotenv()

REDIS_URL    = os.getenv("REDIS_URL",    "redis://localhost:6379/0")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://taskqueue:taskqueue@localhost:5432/taskqueue")

# ---------------------------------------------------------------------------
# Terminal formatting
# ---------------------------------------------------------------------------

RESET   = "\033[0m"
BOLD    = "\033[1m"
GREEN   = "\033[32m"
BLUE    = "\033[34m"
YELLOW  = "\033[33m"
RED     = "\033[31m"
CYAN    = "\033[36m"
DIM     = "\033[2m"


def header(title: str):
    w = 56
    print(f"\n{BOLD}{CYAN}╔{'═' * w}╗")
    print(f"║  {title:<{w - 2}}║")
    print(f"╚{'═' * w}╝{RESET}")


def section(title: str):
    print(f"\n{BOLD}{BLUE}  {title}{RESET}")
    print(f"  {DIM}{'─' * 52}{RESET}")


def result(label: str, value: str, unit: str = "", color: str = BOLD):
    print(f"  {DIM}{label:<30}{RESET} {color}{value}{RESET} {DIM}{unit}{RESET}")


def success(msg: str):
    print(f"  {GREEN}  PASS  {msg}{RESET}")


def warning(msg: str):
    print(f"  {YELLOW}  WARN  {msg}{RESET}")


def fail(msg: str):
    print(f"  {RED}  FAIL  {msg}{RESET}")


def progress(current: int, total: int, rate: float = 0):
    pct   = (current / total) * 100
    filled = int(pct / 5)
    bar   = "#" * filled + "-" * (20 - filled)
    rate_str = f"  {rate:,.0f}/s" if rate > 0 else ""
    print(f"  [{bar}] {pct:5.1f}%  {current:,}/{total:,}{rate_str}", end="\r")


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

_db_pool: asyncpg.Pool | None = None
_processed_ids: set[str]      = set()
_duplicate_count: int         = 0
_latencies: list[float]       = []
_lock = asyncio.Lock()


def reset_state():
    global _processed_ids, _duplicate_count, _latencies
    _processed_ids   = set()
    _duplicate_count = 0
    _latencies       = []


# ---------------------------------------------------------------------------
# Real workload handlers
# ---------------------------------------------------------------------------

async def compute_handler(job: Job) -> dict:
    """CPU-bound: fibonacci + list sort."""
    enqueued_at = job.data.get("enqueued_at", time.time())

    # fibonacci
    n = job.data.get("n", 30)
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b

    # sort
    data     = [random.randint(0, 10000) for _ in range(500)]
    checksum = sum(sorted(data)[:10])

    async with _lock:
        if job.id in _processed_ids:
            global _duplicate_count
            _duplicate_count += 1
        _processed_ids.add(job.id)
        _latencies.append((time.time() - enqueued_at) * 1000)

    return {"fib": a, "checksum": checksum}


async def db_handler(job: Job) -> dict:
    """I/O-bound: PostgreSQL write."""
    enqueued_at = job.data.get("enqueued_at", time.time())

    async with _lock:
        if job.id in _processed_ids:
            global _duplicate_count
            _duplicate_count += 1
        _processed_ids.add(job.id)
        _latencies.append((time.time() - enqueued_at) * 1000)

    if _db_pool:
        async with _db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO benchmark_results (job_id, job_name, payload, result, latency_ms) "
                "VALUES ($1, $2, $3, $4, $5)",
                job.id,
                job.name,
                json.dumps(job.data),
                json.dumps({"written": True}),
                (time.time() - enqueued_at) * 1000,
            )

    return {"written": True}


async def flaky_handler(job: Job) -> dict:
    """Fails on first attempt, succeeds on retry."""
    async with _lock:
        _latencies.append((time.time() - job.data.get("enqueued_at", time.time())) * 1000)

    if job.attempts < 2:
        raise ValueError(f"Simulated failure on attempt {job.attempts}")
    return {"recovered": True}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def clean(redis: aioredis.Redis, queue: str):
    await redis.delete(
        RedisKeys.waiting(queue),
        RedisKeys.active(queue),
        RedisKeys.completed(queue),
        RedisKeys.failed(queue),
        RedisKeys.delayed(queue),
    )


async def enqueue(producer: Producer, queue: str, num: int, job_name: str = "compute", n: int = 25):
    sem = asyncio.Semaphore(20)

    async def add(i):
        async with sem:
            return await producer.add(
                f"{job_name}_job",
                {"n": n, "index": i, "enqueued_at": time.time()},
                queue=queue,
            )

    for b in range(0, num, 200):
        end = min(b + 200, num)
        await asyncio.gather(*[add(i) for i in range(b, end)])
        progress(end, num)
    print()


def make_workers(queue: str, num: int, concurrency: int = 1) -> list[Worker]:
    return [
        Worker(queue=queue, concurrency=concurrency, redis_url=REDIS_URL)
        for _ in range(num)
    ]


async def connect_workers(workers: list[Worker]):
    for w in workers:
        await w.connect()
        w.register("compute_job", compute_handler)
        w.register("db_job",      db_handler)
        w.register("flaky_job",   flaky_handler)


async def disconnect_workers(workers: list[Worker]):
    for w in workers:
        await w.disconnect()


async def drain(
    redis: aioredis.Redis,
    queue: str,
    total: int,
    workers: list[Worker],
    timeout: float = 300,
) -> tuple[int, int, float]:
    """Run workers until all jobs complete or timeout."""

    async def loop(w: Worker):
        while True:
            done = (
                await redis.zcard(RedisKeys.completed(queue)) +
                await redis.zcard(RedisKeys.failed(queue))
            )
            if done >= total:
                break
            try:
                await asyncio.wait_for(w._poll(), timeout=3.0)
            except (asyncio.TimeoutError, Exception):
                continue

    tasks   = [asyncio.create_task(loop(w)) for w in workers]
    start   = time.perf_counter()
    stalled = 0
    last    = 0

    while True:
        completed = await redis.zcard(RedisKeys.completed(queue))
        failed    = await redis.zcard(RedisKeys.failed(queue))
        done      = completed + failed
        elapsed   = time.perf_counter() - start
        rate      = done / elapsed if elapsed > 0 else 0

        progress(done, total, rate)

        if done >= total:
            break

        if done == last:
            stalled += 1
            if stalled > 60:
                warning(f"Stalled at {done:,} after {elapsed:.0f}s")
                break
        else:
            stalled = 0
        last = done

        if elapsed > timeout:
            warning(f"Timeout after {timeout}s")
            break

        await asyncio.sleep(0.5)

    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed   = time.perf_counter() - start
    completed = await redis.zcard(RedisKeys.completed(queue))
    failed    = await redis.zcard(RedisKeys.failed(queue))
    return completed, failed, elapsed


def latency_stats() -> dict:
    if not _latencies:
        return {}
    s = sorted(_latencies)
    return {
        "p50": statistics.median(s),
        "p95": s[int(len(s) * 0.95)],
        "p99": s[int(len(s) * 0.99)],
        "avg": statistics.mean(s),
        "max": max(s),
        "min": min(s),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

async def t1_enqueue_throughput(producer, redis, num_jobs) -> float:
    section(f"Test 1  Enqueue Throughput  ({num_jobs:,} jobs)")
    q = "bench_t1"
    await clean(redis, q)

    start = time.perf_counter()
    await enqueue(producer, q, num_jobs)
    elapsed = time.perf_counter() - start
    rate    = num_jobs / elapsed

    result("Jobs enqueued",  f"{num_jobs:,}")
    result("Time",           f"{elapsed:.2f}",  "s")
    result("Throughput",     f"{rate:,.0f}",    "jobs/sec", GREEN)

    await clean(redis, q)
    return rate


async def t2_process_throughput(producer, redis, num_jobs, num_workers) -> float:
    section(f"Test 2  Processing Throughput  ({num_jobs:,} jobs, {num_workers} workers)")
    q = "bench_t2"
    await clean(redis, q)
    reset_state()

    print(f"  Enqueuing {num_jobs:,} jobs...")
    await enqueue(producer, q, num_jobs, "compute")

    workers = make_workers(q, num_workers)
    await connect_workers(workers)

    print(f"  Processing...")
    completed, failed, elapsed = await drain(redis, q, num_jobs, workers)
    print()
    await disconnect_workers(workers)

    rate = num_jobs / elapsed
    result("Completed",  f"{completed:,}")
    result("Failed",     f"{failed:,}")
    result("Time",       f"{elapsed:.2f}",   "s")
    result("Throughput", f"{rate:,.0f}",     "jobs/sec", GREEN)

    await clean(redis, q)
    return rate


async def t3_concurrent_load(producer, redis, num_jobs, num_workers) -> bool:
    section(f"Test 3  Concurrent Load  ({num_jobs:,} jobs, {num_workers} workers)")
    q = "bench_t3"
    await clean(redis, q)
    reset_state()

    workers = make_workers(q, num_workers, concurrency=2)
    await connect_workers(workers)

    sem = asyncio.Semaphore(20)

    async def add(i):
        async with sem:
            return await producer.add(
                "compute_job",
                {"n": 20, "enqueued_at": time.time()},
                queue=q,
            )

    async def enqueue_all():
        for b in range(0, num_jobs, 100):
            end = min(b + 100, num_jobs)
            await asyncio.gather(*[add(i) for i in range(b, end)])

    (completed, failed, elapsed), _ = await asyncio.gather(
        drain(redis, q, num_jobs, workers, timeout=120),
        enqueue_all(),
    )
    print()
    await disconnect_workers(workers)

    lost = num_jobs - completed - failed
    result("Total jobs",  f"{num_jobs:,}")
    result("Completed",   f"{completed:,}")
    result("Lost",        f"{lost:,}")
    result("Duplicates",  f"{_duplicate_count:,}")

    passed = lost == 0 and _duplicate_count == 0
    if lost == 0:
        success("Zero job loss")
    else:
        fail(f"{lost:,} jobs lost")

    if _duplicate_count == 0:
        success("Zero duplicates")
    else:
        fail(f"{_duplicate_count:,} duplicate processings")

    await clean(redis, q)
    return passed


async def t4_crash_recovery(producer, redis) -> bool:
    section("Test 4  Crash Recovery")
    q = "bench_t4"
    await clean(redis, q)

    jobs = [await producer.add("compute_job", {"n": 10}, queue=q) for _ in range(10)]

    # simulate 5 crashed workers — move to active with no lock
    for j in jobs[:5]:
        await redis.zrem(RedisKeys.waiting(q), j.id)
        await redis.lpush(RedisKeys.active(q), j.id)
        await redis.hset(RedisKeys.job(q, j.id), "status", "active")

    result("Jobs stuck in active", "5")

    reaper = Reaper(redis_url=REDIS_URL, poll_interval=1)
    await reaper.connect()
    recovered = await reaper._scan()
    await reaper.disconnect()

    result("Recovered by reaper", f"{recovered}", "", GREEN)

    passed = recovered == 5
    if passed:
        success("Reaper correctly recovered all stale jobs")
    else:
        fail(f"Expected 5 recovered, got {recovered}")

    await clean(redis, q)
    return passed


async def t5_retry_accuracy(producer, redis) -> bool:
    section("Test 5  Retry Accuracy")
    q     = "bench_t5"
    num   = 50
    await clean(redis, q)
    reset_state()

    for _ in range(num):
        await producer.add(
            "flaky_job",
            {"enqueued_at": time.time()},
            queue=q,
            max_attempts=3,
        )

    workers = make_workers(q, 3)
    await connect_workers(workers)

    # lower backoff for test speed
    for w in workers:
        w._base_retry_delay = 0.1

    completed, failed, elapsed = await drain(redis, q, num, workers, timeout=60)
    print()
    await disconnect_workers(workers)

    result("Flaky jobs",          f"{num}")
    result("Eventually completed", f"{completed}")
    result("Permanently failed",  f"{failed}")

    passed = completed == num
    if passed:
        success("All flaky jobs recovered via retry")
    else:
        warning(f"{num - completed} jobs did not recover")

    await clean(redis, q)
    return passed


async def t6_latency(producer, redis, num_jobs) -> dict:
    section(f"Test 6  Latency Distribution  ({num_jobs:,} jobs)")
    q = "bench_t6"
    await clean(redis, q)
    reset_state()

    await enqueue(producer, q, num_jobs, "compute", n=15)

    workers = make_workers(q, 5, concurrency=2)
    await connect_workers(workers)

    completed, _, elapsed = await drain(redis, q, num_jobs, workers)
    print()
    await disconnect_workers(workers)

    stats = latency_stats()
    result("P50",     f"{stats.get('p50', 0):.1f}", "ms")
    result("P95",     f"{stats.get('p95', 0):.1f}", "ms")
    result("P99",     f"{stats.get('p99', 0):.1f}", "ms", YELLOW)
    result("Average", f"{stats.get('avg', 0):.1f}", "ms")
    result("Max",     f"{stats.get('max', 0):.1f}", "ms", RED)
    result("Min",     f"{stats.get('min', 0):.1f}", "ms", GREEN)

    await clean(redis, q)
    return stats


async def t7_worker_scaling(producer, redis) -> dict:
    section("Test 7  Worker Scaling")
    jobs_per   = 1000
    scaling    = {}

    for n in [1, 2, 5, 10]:
        q = f"bench_t7_{n}"
        await clean(redis, q)
        reset_state()

        await enqueue(producer, q, jobs_per, "compute", n=20)

        workers = make_workers(q, n)
        await connect_workers(workers)

        completed, _, elapsed = await drain(redis, q, jobs_per, workers, timeout=60)
        print()
        await disconnect_workers(workers)

        rate = jobs_per / elapsed
        scaling[n] = rate
        result(f"{n} worker(s)", f"{rate:,.0f}", "jobs/sec",
               GREEN if n > 1 else BOLD)

        await clean(redis, q)

    base = scaling.get(1, 1)
    print(f"\n  Scaling efficiency:")
    for n, r in scaling.items():
        eff = (r / base / n) * 100
        result(f"  {n}x workers", f"{eff:.0f}%", "efficiency")

    return scaling


async def t8_sustained_load(producer, redis, duration: int) -> dict:
    section(f"Test 8  Sustained Load  ({duration}s)")
    q       = "bench_t8"
    running = True
    enqueued = 0

    await clean(redis, q)

    workers = make_workers(q, 5, concurrency=2)
    await connect_workers(workers)

    async def enqueue_loop():
        nonlocal enqueued, running
        sem = asyncio.Semaphore(10)

        async def add():
            nonlocal enqueued
            async with sem:
                jtype = random.choice(["compute", "db"])
                await producer.add(
                    f"{jtype}_job",
                    {"n": 15, "enqueued_at": time.time()},
                    queue=q,
                )
                enqueued += 1

        while running:
            await asyncio.gather(*[add() for _ in range(20)])
            await asyncio.sleep(0.1)

    async def process_loop():
        while running:
            for w in workers:
                try:
                    await asyncio.wait_for(w._poll(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

    start      = time.perf_counter()
    eq_task    = asyncio.create_task(enqueue_loop())
    proc_task  = asyncio.create_task(process_loop())

    while time.perf_counter() - start < duration:
        elapsed   = time.perf_counter() - start
        completed = await redis.zcard(RedisKeys.completed(q))
        rate      = completed / elapsed if elapsed > 0 else 0
        print(
            f"  {elapsed:5.0f}s | "
            f"enqueued: {enqueued:,} | "
            f"completed: {completed:,} | "
            f"rate: {rate:,.0f}/s",
            end="\r"
        )
        await asyncio.sleep(1)

    running = False
    eq_task.cancel()
    proc_task.cancel()
    await asyncio.gather(eq_task, proc_task, return_exceptions=True)
    await disconnect_workers(workers)

    elapsed   = time.perf_counter() - start
    completed = await redis.zcard(RedisKeys.completed(q))
    waiting   = await redis.zcard(RedisKeys.waiting(q))

    print()
    result("Duration",       f"{elapsed:.1f}",              "s")
    result("Total enqueued", f"{enqueued:,}")
    result("Total completed",f"{completed:,}")
    result("Still waiting",  f"{waiting:,}")
    result("Avg throughput", f"{completed / elapsed:,.0f}", "jobs/sec", GREEN)

    if waiting < enqueued * 0.1:
        success("Queue kept up with sustained load")
    else:
        warning("Queue fell behind under sustained load")

    await clean(redis, q)
    return {"enqueued": enqueued, "completed": completed, "rate": completed / elapsed}


async def t9_memory(producer, redis) -> dict:
    section("Test 9  Memory Usage")
    q   = "bench_t9"
    num = 10_000
    await clean(redis, q)

    info     = await redis.info("memory")
    baseline = info["used_memory"] / 1024 / 1024

    await enqueue(producer, q, num, "compute", n=20)

    info     = await redis.info("memory")
    after    = info["used_memory"] / 1024 / 1024
    overhead = after - baseline
    per_job  = (overhead * 1024) / num

    result("Baseline",           f"{baseline:.1f}",           "MB")
    result("After 10K jobs",     f"{after:.1f}",              "MB")
    result("Queue overhead",     f"{overhead:.1f}",           "MB")
    result("Per job",            f"{per_job:.2f}",            "KB",  GREEN)
    result("Projected 100K",     f"{per_job * 100 / 1024:.1f}", "MB")
    result("Projected 1M",       f"{per_job * 1000 / 1024:.1f}", "MB")

    await clean(redis, q)
    return {"per_job_kb": per_job, "overhead_mb": overhead}


async def t10_breaking_point(producer, redis) -> int:
    section("Test 10  Breaking Point")
    last_good = 0

    workers = make_workers("bench_t10", 10, concurrency=3)

    for batch in [500, 1000, 2000, 5000, 10000]:
        q = f"bench_t10_{batch}"
        await clean(redis, q)
        reset_state()

        for w in workers:
            w.queue = q
            if not w._redis:
                await w.connect()
                w.register("compute_job", compute_handler)
                w.register("db_job",      db_handler)

        await enqueue(producer, q, batch, "compute", n=10)

        completed, failed, elapsed = await drain(redis, q, batch, workers, timeout=30)
        print()

        rate     = batch / elapsed
        loss_pct = ((batch - completed) / batch) * 100
        color    = GREEN if loss_pct < 1 else RED

        result(
            f"Batch {batch:,}",
            f"{rate:,.0f} jobs/sec | loss: {loss_pct:.1f}%",
            "", color
        )

        if loss_pct < 1:
            last_good = int(rate)
        else:
            fail(f"Breaking point hit at batch size {batch:,}")
            break

        await clean(redis, q)

    await disconnect_workers(workers)

    result("Max sustainable rate", f"{last_good:,}", "jobs/sec", GREEN)
    return last_good


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run_benchmark(num_jobs: int, num_workers: int):
    global _db_pool

    header(f"tq-engine Benchmark Suite")
    print(f"\n  Jobs        : {num_jobs:,}")
    print(f"  Workers     : {num_workers}")
    print(f"  Redis       : {REDIS_URL}")
    print(f"  PostgreSQL  : {DATABASE_URL}")

    redis    = await aioredis.from_url(REDIS_URL, decode_responses=True)
    _db_pool = await get_pool()

    async with _db_pool.acquire() as conn:
        await setup_schema(conn)
        await cleanup_benchmark_data(conn)

    async with Producer(redis_url=REDIS_URL) as producer:
        r = {}

        r["enqueue"]  = await t1_enqueue_throughput(producer, redis, num_jobs)
        r["process"]  = await t2_process_throughput(producer, redis, num_jobs // 5, num_workers)
        r["conc"]     = await t3_concurrent_load(producer, redis, num_jobs // 10, num_workers)
        r["crash"]    = await t4_crash_recovery(producer, redis)
        r["retry"]    = await t5_retry_accuracy(producer, redis)
        r["latency"]  = await t6_latency(producer, redis, num_jobs // 10)
        r["scaling"]  = await t7_worker_scaling(producer, redis)
        r["sustained"]= await t8_sustained_load(producer, redis, 30)
        r["memory"]   = await t9_memory(producer, redis)
        r["breaking"] = await t10_breaking_point(producer, redis)

    lat = r["latency"]

    header("BENCHMARK RESULTS")
    print(f"""
  Performance:
    Enqueue throughput    : {r['enqueue']:>10,.0f}  jobs/sec
    Process throughput    : {r['process']:>10,.0f}  jobs/sec
    Breaking point        : {r['breaking']:>10,}  jobs/sec
    Sustained rate        : {r['sustained'].get('rate', 0):>10,.0f}  jobs/sec

  Reliability:
    Zero job loss         : {'PASS' if r['conc'] else 'FAIL'}
    Crash recovery        : {'PASS' if r['crash'] else 'FAIL'}
    Retry accuracy        : {'PASS' if r['retry'] else 'FAIL'}

  Latency:
    P50                   : {lat.get('p50', 0):>10.1f}  ms
    P95                   : {lat.get('p95', 0):>10.1f}  ms
    P99                   : {lat.get('p99', 0):>10.1f}  ms
    Average               : {lat.get('avg', 0):>10.1f}  ms

  Efficiency:
    Memory per job        : {r['memory'].get('per_job_kb', 0):>10.2f}  KB
    Jobs enqueued (8s)    : {r['sustained'].get('enqueued', 0):>10,}
    Jobs completed (8s)   : {r['sustained'].get('completed', 0):>10,}

  Worker Scaling:
""")
    for n, rate in r["scaling"].items():
        print(f"    {n} worker(s){'':>14}: {rate:>10,.0f}  jobs/sec")

    await redis.aclose()
    await _db_pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="tq-engine Benchmark Suite")
    parser.add_argument("--jobs",    type=int, default=50_000)
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()
    asyncio.run(run_benchmark(args.jobs, args.workers))