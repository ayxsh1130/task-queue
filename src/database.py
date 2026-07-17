"""
database.py — PostgreSQL connection and schema setup

Used by benchmark workers to write real data to the database.
Also used later by AutoApply for persistent job storage.
"""

from __future__ import annotations
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://taskqueue:taskqueue@localhost:5432/taskqueue")


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)


async def get_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


async def setup_schema(conn: asyncpg.Connection):
    """Create tables needed for benchmark and future use."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS benchmark_results (
            id          SERIAL PRIMARY KEY,
            job_id      TEXT NOT NULL,
            job_name    TEXT NOT NULL,
            payload     JSONB,
            result      JSONB,
            processed_at TIMESTAMPTZ DEFAULT NOW(),
            latency_ms  FLOAT
        );

        CREATE TABLE IF NOT EXISTS http_results (
            id          SERIAL PRIMARY KEY,
            job_id      TEXT NOT NULL,
            url         TEXT NOT NULL,
            status_code INTEGER,
            response_ms FLOAT,
            fetched_at  TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS compute_results (
            id          SERIAL PRIMARY KEY,
            job_id      TEXT NOT NULL,
            input_n     INTEGER,
            result      BIGINT,
            compute_ms  FLOAT,
            computed_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


async def cleanup_benchmark_data(conn: asyncpg.Connection):
    """Clean up benchmark data between runs."""
    await conn.execute("TRUNCATE benchmark_results, http_results, compute_results;")