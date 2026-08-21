from typing import Any, Dict
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row
from app.config import get_shards_map, settings
from app.db import (
    create_job,
    get_db_connection_for_tenant,
    get_job_by_id,
    get_queue_depth,
)

router = APIRouter(prefix="/v1/jobs", tags=["Jobs"])


class JobCreateRequest(BaseModel):
    idempotency_key: str = Field(..., description="Unique key for idempotent submission")
    tenant_id: str = Field(default="default", description="Tenant identifier for database shard routing")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Job execution parameters")
    priority: int = Field(default=0, ge=-10, le=10, description="Job priority")
    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum execution attempts")


@router.post("", status_code=status.HTTP_201_CREATED)
def submit_job(req: JobCreateRequest):
    try:
        conn, shard_id = get_db_connection_for_tenant(req.tenant_id)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database routing error for tenant '{req.tenant_id}': {str(e)}",
        )

    with conn:
        depth = get_queue_depth(conn)
        if depth >= settings.max_queue_depth:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Queue depth ({depth}) on shard '{shard_id}' exceeds limit ({settings.max_queue_depth}).",
                headers={"Retry-After": "5"},
            )

        job = create_job(
            conn,
            idempotency_key=req.idempotency_key,
            tenant_id=req.tenant_id,
            payload=req.payload,
            priority=req.priority,
            max_attempts=req.max_attempts,
        )
        job["shard_id"] = shard_id
        return job


@router.get("/{job_id}")
def get_job(job_id: str):
    shards_config = get_shards_map()
    for shard_id, db_url in shards_config.items():
        try:
            with psycopg.connect(db_url, row_factory=dict_row) as conn:
                job = get_job_by_id(conn, job_id)
                if job:
                    job["shard_id"] = shard_id
                    return job
        except Exception:
            continue

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Job with ID '{job_id}' not found across any database shard",
    )


@router.get("/queue/depth")
def get_depth():
    shards_config = get_shards_map()
    total_depth = 0
    per_shard = {}

    for shard_id, db_url in shards_config.items():
        try:
            with psycopg.connect(db_url, row_factory=dict_row) as conn:
                depth = get_queue_depth(conn)
                per_shard[shard_id] = depth
                total_depth += depth
        except Exception as e:
            per_shard[shard_id] = f"error: {str(e)}"

    return {
        "queue_depth": total_depth,
        "max_capacity_per_shard": settings.max_queue_depth,
        "per_shard": per_shard,
    }
