# `forge` — Distributed Task Queue & Multi-Database Sharded Scheduler

`forge` is a high-performance, fault-tolerant distributed job scheduling and execution engine built with **Go**, **etcd**, **PostgreSQL**, and **Python (FastAPI + Client SDK)**.

It features **etcd Raft leader election**, **active leadership re-validation to prevent split-brain traps**, **atomic database job claiming (`FOR UPDATE SKIP LOCKED`)**, **Multi-Database Consistent Hash Ring Sharding (256 virtual nodes)**, **dedicated worker shard pools**, and **parallel multi-shard dead-worker reaping**.

---

## 🏗️ System Architecture

```
                      ┌─────────────────────────────────┐
                      │    Python SDK / Python API      │
                      │ (FastAPI In-Process Hash Router)│
                      └─────────────────────────────────┘
                                       │
                Reads Live Shard Map   │ Resolves Shard via
                & Watches etcd Updates │ Consistent Hashing (256 vnodes)
                                       ▼
                      ┌─────────────────────────────────┐
                      │      etcd 3-Node Cluster        │
                      │  (/forge/shards, /forge/leader, │
                      │   /forge/workers/{shard_id})    │
                      └─────────────────────────────────┘
                             ▲                   ▲
             Polls Assigned  │                   │ Parallel Reaper Scans
                Shard DB     │                   │ (Active Leader Only)
                             │                   │
         ┌───────────────────┴───┐           ┌───┴────────────────────┐
         │ Worker Pool Node      │           │ Scheduler Leader Node  │
         │ (Shard-Dedicated Pools│           │ (Parallel Multi-Shard) │
         └───────────────────────┘           └────────────────────────┘
                   │       │                             │       │
      FOR UPDATE   │       │ FOR UPDATE      FOR UPDATE  │       │ FOR UPDATE
     SKIP LOCKED   │       │ SKIP LOCKED    SKIP LOCKED  │       │ SKIP LOCKED
                   ▼       ▼                             ▼       ▼
           ┌──────────────┐ ┌──────────────┐     ┌──────────────┐ ┌──────────────┐
           │ Postgres DB  │ │ Postgres DB  │ ... │ Postgres DB  │ │ Postgres DB  │
           │   Shard 1    │ │   Shard 2    │     │   Shard N-1  │ │   Shard N    │
           └──────────────┘ └──────────────┘     └──────────────┘ └──────────────┘
```

---

## 🔑 Core Distributed Systems Mechanisms

### 1. Multi-Database Consistent Hash Ring Sharding
- **Consistent Hash Router**: Implemented in Go (`shared/router/hash_ring.go`) and Python (`api/app/router.py`) using 256 virtual nodes per physical shard.
- **Tenant Partitioning**: Jobs are partitioned deterministically across database shards by `tenant_id`, guaranteeing uniform load distribution without single-database bottlenecks.
- **Dynamic Shard Discovery**: Services query dynamic shard maps and maintain lock-free routing tables synchronized via etcd Watches.

### 2. Dedicated Shard Worker Pools
- Worker nodes register for dedicated database shards via `etcd` leases and configuration (`ASSIGNED_SHARDS="shard-1,shard-2"`).
- Each worker instance manages isolated database connection pools and goroutine pools per shard to eliminate cross-shard lock contention.

### 3. etcd Leader Election & Active Re-Validation
- Scheduler instances compete for leadership using `go.etcd.io/etcd/client/v3/concurrency`.
- **Split-Brain Protection**: Before performing any reaper or dispatch operation, the scheduler actively re-validates its leadership against etcd. If a network partition or GC pause invalidates the etcd lease session, the node immediately steps down.

### 4. Parallel Multi-Shard Dead-Worker Reaper
- The active Scheduler Leader reads the active shard topology from etcd and spawns parallel background goroutines across all database shards.
- Expired worker jobs (`last_heartbeat < now() - 10s`) are automatically requeued or transitioned to `failed` when `max_attempts` is reached.

### 5. Atomic Job Claiming (Postgres `FOR UPDATE SKIP LOCKED`)
- Workers poll PostgreSQL directly using atomic row-level locking:
  ```sql
  SELECT * FROM jobs
  WHERE status = 'queued'
  ORDER BY priority DESC, created_at ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED;
  ```

### 6. Observability & Backpressure
- Returns `HTTP 429 Too Many Requests` with a `Retry-After: 5` header when queue depth exceeds capacity thresholds.
- Exposes Prometheus metrics on `/metrics` (`forge_jobs_claimed_total`, `forge_jobs_completed_total`, `forge_job_execution_duration_seconds`, `forge_queue_depth`, `forge_leader_status`).

---

## 🚀 Quickstart

### 1. Multi-Database Sharded Cluster (Recommended)
Spin up a 3-shard PostgreSQL cluster, 3-node etcd cluster, 2 schedulers, 3 workers, and the FastAPI submission service:

```bash
docker compose -f deploy/docker-compose.sharded.yml up --build
```

### 2. Single Database Cluster
```bash
docker compose -f deploy/docker-compose.yml up --build
```

---

## 🧪 Leader Failover Demo Instructions

1. Start the cluster:
   ```bash
   docker compose -f deploy/docker-compose.sharded.yml up
   ```
2. Identify the active leader in logs (`forge-scheduler-1` or `forge-scheduler-2`).
3. Kill the leader container:
   ```bash
   docker kill forge-scheduler-1
   ```
4. **Failover Verification**: Within **~10 seconds** (etcd lease TTL), the standby scheduler instance detects lease expiration, wins the campaign, takes over leadership, and resumes multi-shard reaping seamlessly.

---

## 📊 Running Tests

### Go Test Suite (with Race Detector & Shard Routing Tests)
```bash
go test -v -race ./shared/... ./worker/... ./scheduler/...
```

### Python API & SDK Test Suite
```bash
pytest -v api/tests/ sdk/tests/
```

---

## ⚠️ Known Limitations & Tradeoffs

- **At-Least-Once Execution**: Handlers must maintain idempotent side-effects.
- **Resharding Drain Migration**: Adding new database shards marks virtual nodes as `DRAINING` to finish active jobs on old shards while new tenant submissions land on the new shard.
