-- Database Schema for forge Distributed Task Queue

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT UNIQUE NOT NULL,
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('queued','running','succeeded','failed')),
    priority        SMALLINT NOT NULL DEFAULT 0,
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    worker_id       TEXT,
    last_heartbeat  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_priority ON jobs (status, priority DESC, created_at)
    WHERE status = 'queued';

CREATE TABLE IF NOT EXISTS worker_heartbeats (
    worker_id     TEXT PRIMARY KEY,
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    current_job   UUID REFERENCES jobs(id) ON DELETE SET NULL
);
