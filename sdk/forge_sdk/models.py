from typing import Any, Dict, Optional
from pydantic import BaseModel


class Job(BaseModel):
    id: str
    idempotency_key: str
    payload: Dict[str, Any]
    status: str
    priority: int = 0
    attempts: int = 0
    max_attempts: int = 3
    worker_id: Optional[str] = None
    created_at: str
    updated_at: str


class QueueDepthResponse(BaseModel):
    queue_depth: int
    max_capacity: int
