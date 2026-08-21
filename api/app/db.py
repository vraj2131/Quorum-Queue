from typing import Any, Dict, Optional
import psycopg
from psycopg.rows import dict_row
from app.config import get_shards_map, settings
from app.router import ConsistentHashRing

_hash_ring: Optional[ConsistentHashRing] = None
_shards_map: Dict[str, str] = {}


def get_hash_ring() -> ConsistentHashRing:
    global _hash_ring, _shards_map
    if _hash_ring is None:
        _shards_map = get_shards_map()
        _hash_ring = ConsistentHashRing(vnodes_per_node=256)
        for shard_id in _shards_map.keys():
            _hash_ring.add_shard(shard_id)
    return _hash_ring


def get_db_connection_for_tenant(tenant_id: str) -> tuple[psycopg.Connection, str]:
    shards_config = get_shards_map()
    ring = get_hash_ring()
    shard_id = ring.get_shard(tenant_id)
    db_url = shards_config.get(shard_id, settings.database_url)
    conn = psycopg.connect(db_url, row_factory=dict_row)
    return conn, shard_id


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
    tenant_id: str,
    payload: Dict[str, Any],
    priority: int = 0,
    max_attempts: int = 3,
) -> Dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE idempotency_key = %s", (idempotency_key,))
        existing = cur.fetchone()
        if existing:
            return dict(existing)

        cur.execute(
            """
            INSERT INTO jobs (idempotency_key, tenant_id, payload, status, priority, max_attempts)
            VALUES (%s, %s, %s, 'queued', %s, %s)
            RETURNING *
            """,
            (
                idempotency_key,
                tenant_id,
                psycopg.types.json.Jsonb(payload),
                priority,
                max_attempts,
            ),
        )
        new_job = cur.fetchone()
        conn.commit()
        return dict(new_job)


def get_job_by_id(conn: psycopg.Connection, job_id: str) -> Optional[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        job = cur.fetchone()
        return dict(job) if job else None
