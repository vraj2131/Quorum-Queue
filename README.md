# `forge` — Distributed Task Queue & Scheduler

`forge` is a high-performance distributed job scheduling and execution engine built with **Go**, **etcd**, **PostgreSQL**, and **Python (FastAPI + Client SDK)**.

It demonstrates fault-tolerant distributed systems mechanisms including **etcd Raft leader election**, **split-brain prevention via active leadership re-validation**, **atomic database job claiming (`FOR UPDATE SKIP LOCKED`)**, **at-least-once delivery with idempotent execution**, and **dead-worker heartbeat reaping**.

---

## 🏗️ Architecture

```
                     ┌─────────────────────┐
   Python SDK /  ───▶│   API Service (Go)   │──▶ writes job row to Postgres
   FastAPI API       │   (job submission)   │       (status = "queued")
                     └─────────────────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │      Postgres        │◀── source of truth for job state
                     │  (jobs, leases,       │
                     │   worker_heartbeats)  │
                     └─────────────────────┘
                        ▲              ▲
             claim job  │              │  reap dead workers (only if leader)
          (FOR UPDATE   │              │
         SKIP LOCKED)   │              │
                     ┌───────┐    ┌───────────────────┐
                     │Worker │    │  Scheduler Nodes    │
                     │ Pool  │    │  (Go, N instances)  │
                     │ (Go)  │◀───│  leader election    │
                     └───────┘    │  via etcd           │
                                  └───────────────────┘
```

---

## 🔑 Core Distributed Systems Mechanisms

### 1. Leader Election & Active Re-Validation ( etcd )
- Scheduler instances compete for leadership using `go.etcd.io/etcd/client/v3/concurrency`.
- Only the elected leader runs the background dead-worker reaper.
- **Split-Brain Protection**: Before performing any reaper or dispatch operation, the scheduler active re-validates its leadership against etcd. If a network partition or GC pause invalidates the etcd lease session, the node immediately steps down without making un-coordinated mutations.

### 2. Atomic Job Claiming (Postgres `FOR UPDATE SKIP LOCKED`)
- Workers poll PostgreSQL directly using atomic row-level locking:
  ```sql
  SELECT * FROM jobs
  WHERE status = 'queued'
  ORDER BY priority DESC, created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED;
  ```
- PostgreSQL acts as the atomic coordinator for job assignments, guaranteeing zero duplicate job claims across concurrent worker nodes without requiring distributed locks on workers.

### 3. Idempotent Execution & At-Least-Once Delivery
- Jobs enforce unique `idempotency_key` identifiers.
- Re-submitting or re-running a job executes idempotently without duplicate side-effects.

### 4. Heartbeating & Dead-Worker Reaper
- Active workers send periodic heartbeats (`UPDATE jobs SET last_heartbeat = now()`) while running tasks.
- The active scheduler leader scans for jobs with expired heartbeats (> 10s timeout) and automatically requeues them or transitions them to `failed` if `max_attempts` is reached.

### 5. Backpressure & Observability
- Returns `HTTP 429 Too Many Requests` with a `Retry-After: 5` header when queue depth exceeds capacity thresholds.
- Exposes Prometheus metrics on `/metrics` (`forge_jobs_claimed_total`, `forge_jobs_completed_total`, `forge_job_execution_duration_seconds`, `forge_queue_depth`, `forge_leader_status`).

---

## 🚀 Quickstart with Docker Compose

Spin up a 3-node etcd cluster, PostgreSQL, 2 schedulers, 2 workers, and the FastAPI service with one command:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

---

## 🧪 Leader Failover Demo Instructions

1. Start the cluster using Docker Compose:
   ```bash
   docker compose -f deploy/docker-compose.yml up
   ```
2. Check scheduler logs to identify the active leader (`forge-scheduler-1` or `forge-scheduler-2`).
3. Kill the leader container:
   ```bash
   docker kill forge-scheduler-1
   ```
4. Observe failover: Within **~10 seconds** (etcd lease TTL), the standby scheduler instance detects lease expiration, wins the campaign, takes over leadership, and resumes dead-worker reaping seamlessly.

---

## 📊 Running Tests

### Go Test Suite (with Race Detector)
```bash
go test -v -race ./shared/... ./worker/... ./scheduler/...
```

### Python API & SDK Test Suite
```bash
pytest -v api/tests/ sdk/tests/
```

---

## ⚠️ Known Limitations & Tradeoffs

- **At-Least-Once Delivery**: Job execution provides at-least-once guarantees. Job handlers must maintain idempotent side-effects.
- **Single PostgreSQL Instance**: PostgreSQL is the single source of truth for job state in local dev. In production, highly available PostgreSQL (e.g. Patroni / AWS RDS Multi-AZ) would be recommended.
