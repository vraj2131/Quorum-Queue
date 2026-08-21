package router

import (
	"fmt"
	"testing"
)

func TestHashRing_AddAndGetShard(t *testing.T) {
	ring := NewHashRing(100)
	ring.AddShard("shard-1")
	ring.AddShard("shard-2")
	ring.AddShard("shard-3")

	shards := ring.GetShards()
	if len(shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(shards))
	}

	shard1, err := ring.GetShard("tenant-abc")
	if err != nil {
		t.Fatalf("failed to get shard: %v", err)
	}
	if shard1 == "" {
		t.Fatalf("expected non-empty shard")
	}

	// Test consistent routing for same key
	shard2, _ := ring.GetShard("tenant-abc")
	if shard1 != shard2 {
		t.Fatalf("expected consistent shard routing for tenant-abc, got %s and %s", shard1, shard2)
	}
}

func TestHashRing_UniformDistribution(t *testing.T) {
	ring := NewHashRing(256)
	ring.AddShard("shard-1")
	ring.AddShard("shard-2")
	ring.AddShard("shard-3")

	counts := make(map[string]int)
	totalKeys := 3000

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("tenant-%d", i)
		shard, err := ring.GetShard(key)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		counts[shard]++
	}

	for shard, count := range counts {
		percentage := float64(count) / float64(totalKeys) * 100
		t.Logf("Shard %s received %d keys (%.2f%%)", shard, count, percentage)
		// Each shard should get roughly ~33% (+/- 10%)
		if percentage < 20.0 || percentage > 45.0 {
			t.Errorf("Shard %s distribution biased: %.2f%%", shard, percentage)
		}
	}
}
