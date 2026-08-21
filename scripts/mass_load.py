#!/usr/bin/env python3
import asyncio
import argparse
import time
import uuid
import httpx

TENANTS = ["tenant-alpha", "tenant-beta", "tenant-gamma", "tenant-delta", "tenant-epsilon", "tenant-acme", "tenant-cyber"]


async def submit_job(client: httpx.AsyncClient, url: str, index: int):
    tenant = TENANTS[index % len(TENANTS)]
    payload = {
        "idempotency_key": f"mass-job-{uuid.uuid4().hex[:12]}",
        "tenant_id": tenant,
        "payload": {"type": "benchmark", "batch_index": index},
        "priority": (index % 10),
    }
    try:
        resp = await client.post(f"{url}/v1/jobs", json=payload, timeout=10.0)
        return resp.status_code == 201
    except Exception:
        return False


async def run_mass_load(url: str, total_jobs: int = 5000, concurrency: int = 50):
    print(f"🚀 Launching mass load generator: {total_jobs} jobs with concurrency={concurrency}...")
    start_time = time.time()
    
    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency * 2)
    async with httpx.AsyncClient(limits=limits) as client:
        tasks = []
        for i in range(total_jobs):
            tasks.append(submit_job(client, url, i))
            if len(tasks) >= concurrency:
                await asyncio.gather(*tasks)
                tasks = []
                print(f"  Progress: [{i+1}/{total_jobs}] jobs submitted...", end="\r")
        if tasks:
            await asyncio.gather(*tasks)

    elapsed = time.time() - start_time
    rate = total_jobs / elapsed if elapsed > 0 else 0
    print(f"\n✅ Finished mass submission of {total_jobs} jobs in {elapsed:.2f}s ({rate:.1f} jobs/sec)!")


def main():
    parser = argparse.ArgumentParser(description="forge Mass Load Generator")
    parser.add_argument("--url", default="http://150.136.216.103:8000", help="Base URL of forge API")
    parser.add_argument("--count", type=int, default=5000, help="Total jobs to submit (default: 5000)")
    parser.add_argument("--concurrency", type=int, default=50, help="Async concurrency limit (default: 50)")
    args = parser.parse_args()

    asyncio.run(run_mass_load(args.url, args.count, args.concurrency))


if __name__ == "__main__":
    main()
