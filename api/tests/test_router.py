from app.router import ConsistentHashRing


def test_consistent_hash_ring_routing():
    ring = ConsistentHashRing(vnodes_per_node=100)
    ring.add_shard("shard-1")
    ring.add_shard("shard-2")
    ring.add_shard("shard-3")

    shard1 = ring.get_shard("tenant-123")
    assert shard1 in ("shard-1", "shard-2", "shard-3")

    # Verify deterministic routing for same tenant_id
    shard2 = ring.get_shard("tenant-123")
    assert shard1 == shard2


def test_consistent_hash_ring_distribution():
    ring = ConsistentHashRing(vnodes_per_node=256)
    ring.add_shard("shard-1")
    ring.add_shard("shard-2")
    ring.add_shard("shard-3")

    counts = {"shard-1": 0, "shard-2": 0, "shard-3": 0}
    total = 3000

    for i in range(total):
        s = ring.get_shard(f"tenant-{i}")
        counts[s] += 1

    for shard, count in counts.items():
        pct = (count / total) * 100
        assert 20.0 <= pct <= 45.0
