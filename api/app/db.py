from typing import Any, Dict, Optional
import psycopg
from psycopg.rows import dict_row
from api.app.config import settings


def get_db_connection():
    return psycopg.connect(settings.database_url, row_factory=dict_row)


def get_queue_depth(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) as count FROM jobs WHERE status = 'queued'")
        res = cur.fetchone()
        return res["count"] if res else 0


def create_job(
    conn: psycopg.Connection,
    idempotency_key: str,
    payload: Dict[str, Any],
    priority: int = 0,
    max_attempts: int = 3,
) -> Dict[str, Any]:
    with conn.cursor() as cur:
        # Check idempotency first
        cur.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
        existing = cur.fetchone()
        if existing:
            return dict(existing)

        cur.execute(
            """
            INSERT INTO jobs (idempotency_key, payload, status, priority, max_attempts)
            VALUES (%s, %s, 'queued', %s, %s)
            RETURNING *
            """,
            (idempotency_key, psycopg.types.json.Jsonb(payload), priority, max_attempts),
        )
        new_job = cur.fetchone()
        conn.commit()
        return dict(new_job)


def get_job_by_id(conn: psycopg.Connection, job_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        job = cur.fetchone()
        return dict(job) if job else None
