package poller

import (
	"testing"
)

func TestParseShardsConfig(t *testing.T) {
	configStr := "shard-1=postgres://localhost:5432/s1, shard-2=postgres://localhost:5432/s2,shard-3=postgres://localhost:5432/s3"
	parsed := ParseShardsConfig(configStr)

	if len(parsed) != 3 {
		t.Fatalf("expected 3 shards parsed, got %d", len(parsed))
	}

	if parsed["shard-1"] != "postgres://localhost:5432/s1" {
		t.Errorf("unexpected value for shard-1: %s", parsed["shard-1"])
	}
	if parsed["shard-2"] != "postgres://localhost:5432/s2" {
		t.Errorf("unexpected value for shard-2: %s", parsed["shard-2"])
	}
	if parsed["shard-3"] != "postgres://localhost:5432/s3" {
		t.Errorf("unexpected value for shard-3: %s", parsed["shard-3"])
	}
}
