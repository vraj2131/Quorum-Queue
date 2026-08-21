import time
from typing import Any, Dict, Optional
import httpx
from forge_sdk.models import Job, QueueDepthResponse


class ForgeClientError(Exception):
    """Base exception for Forge SDK operations."""
    pass


class QueueFullError(ForgeClientError):
    """Raised when the job queue exceeds capacity (429)."""
    pass


class JobNotFoundError(ForgeClientError):
    """Raised when the requested job is not found (404)."""
    pass


class ForgeClient:
    def __init__(self, base_url: str = "http://localhost:8000", timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(base_url=self.base_url, timeout=timeout)

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def submit_job(
        self,
        idempotency_key: str,
        tenant_id: str = "default",
        payload: Optional[Dict[str, Any]] = None,
        priority: int = 0,
        max_attempts: int = 3,
    ) -> Job:
        payload_data = payload or {}
        req_data = {
            "idempotency_key": idempotency_key,
            "tenant_id": tenant_id,
            "payload": payload_data,
            "priority": priority,
            "max_attempts": max_attempts,
        }
        res = self.client.post("/v1/jobs", json=req_data)
        if res.status_code == 429:
            raise QueueFullError(f"Job submission rejected due to queue backpressure: {res.text}")
        if res.status_code not in (200, 201):
            raise ForgeClientError(f"Failed to submit job ({res.status_code}): {res.text}")
        return Job.model_validate(res.json())

    def get_job(self, job_id: str) -> Job:
        res = self.client.get(f"/v1/jobs/{job_id}")
        if res.status_code == 404:
            raise JobNotFoundError(f"Job '{job_id}' not found")
        if res.status_code != 200:
            raise ForgeClientError(f"Failed to fetch job ({res.status_code}): {res.text}")
        return Job.model_validate(res.json())

    def get_queue_depth(self) -> QueueDepthResponse:
        res = self.client.get("/v1/jobs/queue/depth")
        if res.status_code != 200:
            raise ForgeClientError(f"Failed to fetch queue depth ({res.status_code}): {res.text}")
        return QueueDepthResponse.model_validate(res.json())

    def wait_for_job(
        self,
        job_id: str,
        timeout: float = 30.0,
        poll_interval: float = 0.5,
    ) -> Job:
        start_time = time.time()
        while time.time() - start_time < timeout:
            job = self.get_job(job_id)
            if job.status in ("succeeded", "failed"):
                return job
            time.sleep(poll_interval)
        raise TimeoutError(f"Timed out waiting for job '{job_id}' after {timeout} seconds")
