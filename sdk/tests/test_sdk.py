import pytest
import httpx
from forge_sdk.client import ForgeClient, JobNotFoundError, QueueFullError


def test_submit_job_success():
    def handler(request: httpx.Request):
        assert request.url.path == "/v1/jobs"
        return httpx.Response(
            201,
            json={
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "idempotency_key": "test-key-1",
                "payload": {"type": "sleep", "duration": "10ms"},
                "status": "queued",
                "priority": 5,
                "attempts": 0,
                "max_attempts": 3,
                "worker_id": None,
                "created_at": "2026-08-20T00:00:00Z",
                "updated_at": "2026-08-20T00:00:00Z",
            },
        )

    transport = httpx.MockTransport(handler)
    client = ForgeClient(base_url="http://test")
    client.client = httpx.Client(transport=transport, base_url="http://test")

    job = client.submit_job(idempotency_key="test-key-1", payload={"type": "sleep", "duration": "10ms"}, priority=5)
    assert job.id == "123e4567-e89b-12d3-a456-426614174000"
    assert job.status == "queued"
    assert job.priority == 5


def test_submit_job_queue_full():
    def handler(request: httpx.Request):
        return httpx.Response(429, text="Queue full", headers={"Retry-After": "5"})

    transport = httpx.MockTransport(handler)
    client = ForgeClient(base_url="http://test")
    client.client = httpx.Client(transport=transport, base_url="http://test")

    with pytest.raises(QueueFullError):
        client.submit_job(idempotency_key="full-key")


def test_get_job_not_found():
    def handler(request: httpx.Request):
        return httpx.Response(404, json={"detail": "Job not found"})

    transport = httpx.MockTransport(handler)
    client = ForgeClient(base_url="http://test")
    client.client = httpx.Client(transport=transport, base_url="http://test")

    with pytest.raises(JobNotFoundError):
        client.get_job("invalid-id")
