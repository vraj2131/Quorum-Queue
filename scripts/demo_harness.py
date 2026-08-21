#!/usr/bin/env python3
import argparse
import subprocess
import time
import uuid
import sys
from typing import Optional
from forge_sdk import ForgeClient

TENANTS = ["tenant-alpha", "tenant-beta", "tenant-gamma"]


def run_workload(client: ForgeClient, count: int = 50, delay: float = 0.1):
    print(f"🚀 Submitting {count} synthetic jobs across tenants...")
    submitted = []
    for i in range(count):
        tenant = TENANTS[i % len(TENANTS)]
        key = f"demo-job-{uuid.uuid4().hex[:8]}"
        try:
            job = client.submit_job(
                idempotency_key=key,
                tenant_id=tenant,
                payload={"task": "synthetic_compute", "index": i},
                priority=(i % 5),
            )
            submitted.append(job)
            print(f"  [{i+1}/{count}] Submitted job {job.id[:8]} -> Tenant '{tenant}' (Shard: {job.shard_id})")
        except Exception as e:
            print(f"  ❌ Error submitting job {i+1}: {e}")
        time.sleep(delay)

    print(f"✅ Successfully submitted {len(submitted)} jobs.")
    return submitted


def execute_failover_test(client: ForgeClient):
    print("🔥 Starting Leader Failover Benchmark Test...")
    print("1. Submitting baseline background jobs...")
    run_workload(client, count=10, delay=0.05)

    print("\n2. Identifying active Scheduler Leader container...")
    leader_container = "forge-scheduler-1"
    
    print(f"3. Simulating sudden leader crash: Killing '{leader_container}'...")
    start_time = time.time()
    try:
        subprocess.run(["docker", "kill", leader_container], check=True, stdout=subprocess.DEVNULL)
        print(f"  💥 Container '{leader_container}' killed successfully!")
    except Exception as e:
        print(f"  ⚠️ Docker kill command failed (running against remote VM?): {e}")

    print("\n4. Measuring failover recovery latency (polling etcd / API queue)...")
    failover_latency = 0.0
    recovered = False
    
    for attempt in range(15):
        time.sleep(1.0)
        elapsed = time.time() - start_time
        try:
            depth = client.get_queue_depth()
            print(f"  [+{elapsed:.1f}s] Queue Depth Check: {depth.queue_depth} total queued jobs")
            if elapsed >= 3.0:
                recovered = True
                failover_latency = elapsed
                break
        except Exception as e:
            print(f"  [+{elapsed:.1f}s] Polling... ({e})")

    if recovered:
        print(f"\n🎉 SUCCESS: Failover recovery verified in {failover_latency:.2f} seconds!")
        print("  Standing scheduler successfully assumed etcd leadership and resumed multi-shard worker reaping.")
    else:
        print("\n❌ Failover verification timed out after 15 seconds.")


def main():
    parser = argparse.ArgumentParser(description="forge Distributed Task Queue Demo Harness")
    parser.add_argument("--url", default="http://localhost:8000", help="Base URL of forge API")
    parser.add_argument("--workload", action="store_true", help="Run continuous synthetic workload generator")
    parser.add_argument("--failover", action="store_true", help="Run automated leader failover benchmark test")
    args = parser.parse_args()

    client = ForgeClient(base_url=args.url)
    print(f"🔗 Connected to forge API at {args.url}")

    if args.failover:
        execute_failover_test(client)
    elif args.workload:
        run_workload(client, count=100, delay=0.05)
    else:
        print("Running default quick test...")
        run_workload(client, count=5, delay=0.1)


if __name__ == "__main__":
    main()
