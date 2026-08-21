from typing import Any, Dict, Optional
from pydantic import BaseModel


class Job(BaseModel):
    id: str
    idempotency_key: str
    tenant_id: str = "default"
    payload: Dict[str, Any]
    status: str
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 3
    worker_id: Optional[str] = None
    shard_id: Optional[str] = None
    created_at: str
    updated_at: str


class QueueDepthResponse(BaseModel):
    queue_depth: int
    max_capacity_per_shard: int = 1000
    per_shard: Optional[Dict[str, Any]] = None
