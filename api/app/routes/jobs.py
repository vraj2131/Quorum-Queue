from typing import Any, Dict
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field
from app.config import settings
from app.db import create_job, get_db_connection, get_job_by_id, get_queue_depth

router = APIRouter(prefix="/v1/jobs", tags=["Jobs"])


class JobCreateRequest(BaseModel):
    idempotency_key: str = Field(..., description="Unique key for idempotent submission")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Job execution parameters")
    priority: int = Field(default=0, ge=-10, le=10, description="Job priority")
    max_attempts: int = Field(default=3, ge=1, le=10, description="Maximum execution attempts")


@router.post("", status_code=status.HTTP_201_CREATED)
def submit_job(req: JobCreateRequest):
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}",
        )

    with conn:
        # Check backpressure / queue depth
        depth = get_queue_depth(conn)
        if depth >= settings.max_queue_depth:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"Queue depth ({depth}) exceeds capacity limit ({settings.max_queue_depth}). Try again later.",
                headers={"Retry-After": "5"},
            )

        job = create_job(
            conn,
            idempotency_key=req.idempotency_key,
            payload=req.payload,
            priority=req.priority,
            max_attempts=req.max_attempts,
        )
        return job


@router.get("/{job_id}")
def get_job(job_id: str):
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}",
        )

    with conn:
        job = get_job_by_id(conn, job_id)
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job with ID '{job_id}' not found",
            )
        return job


@router.get("/queue/depth")
def get_depth():
    try:
        conn = get_db_connection()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection error: {str(e)}",
        )

    with conn:
        depth = get_queue_depth(conn)
        return {"queue_depth": depth, "max_capacity": settings.max_queue_depth}
