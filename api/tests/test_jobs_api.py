from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health_check():
    res = client.get("/healthz")
    assert res.status_code == 200
    assert res.json() == {"status": "ok", "service": "forge-api"}


@patch("app.routes.jobs.get_db_connection_for_tenant")
@patch("app.routes.jobs.get_queue_depth")
@patch("app.routes.jobs.create_job")
def test_submit_job_success(mock_create_job, mock_get_depth, mock_get_conn_tenant):
    mock_conn = MagicMock()
    mock_get_conn_tenant.return_value = (mock_conn, "shard-1")
    mock_get_depth.return_value = 5
    mock_create_job.return_value = {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "idempotency_key": "key-123",
        "tenant_id": "tenant-alpha",
        "payload": {"type": "sleep"},
        "status": "queued",
        "priority": 2,
        "attempts": 0,
        "max_attempts": 3,
        "worker_id": None,
        "created_at": "2026-08-20T00:00:00Z",
        "updated_at": "2026-08-20T00:00:00Z",
    }

    response = client.post(
        "/v1/jobs",
        json={
            "idempotency_key": "key-123",
            "tenant_id": "tenant-alpha",
            "payload": {"type": "sleep"},
            "priority": 2,
        },
    )
    assert response.status_code == 201
    assert response.json()["tenant_id"] == "tenant-alpha"
    assert response.json()["shard_id"] == "shard-1"


@patch("app.routes.jobs.get_db_connection_for_tenant")
@patch("app.routes.jobs.get_queue_depth")
def test_submit_job_queue_depth_backpressure(mock_get_depth, mock_get_conn_tenant):
    mock_conn = MagicMock()
    mock_get_conn_tenant.return_value = (mock_conn, "shard-2")
    mock_get_depth.return_value = 1000  # max_queue_depth threshold

    response = client.post(
        "/v1/jobs",
        json={"idempotency_key": "overflow-key", "tenant_id": "tenant-beta", "payload": {}},
    )
    assert response.status_code == 429
    assert "Retry-After" in response.headers
    assert response.headers["Retry-After"] == "5"


@patch("app.routes.jobs.get_shards_map")
@patch("psycopg.connect")
@patch("app.routes.jobs.get_job_by_id")
def test_get_job_success(mock_get_job, mock_psycopg_connect, mock_get_shards_map):
    mock_get_shards_map.return_value = {"shard-1": "postgresql://localhost/s1"}
    mock_conn = MagicMock()
    mock_psycopg_connect.return_value.__enter__.return_value = mock_conn

    mock_get_job.return_value = {
        "id": "123e4567-e89b-12d3-a456-426614174000",
        "idempotency_key": "key-123",
        "tenant_id": "tenant-alpha",
        "payload": {},
        "status": "succeeded",
        "priority": 0,
        "attempts": 1,
        "max_attempts": 3,
        "worker_id": "worker-1",
        "created_at": "2026-08-20T00:00:00Z",
        "updated_at": "2026-08-20T00:00:00Z",
    }

    response = client.get("/v1/jobs/123e4567-e89b-12d3-a456-426614174000")
    assert response.status_code == 200
    assert response.json()["status"] == "succeeded"
    assert response.json()["shard_id"] == "shard-1"
