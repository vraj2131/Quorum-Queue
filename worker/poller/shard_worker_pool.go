package poller

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/forge/shared/db"
	"github.com/forge/worker/executor"
)

type MultiShardWorkerPool struct {
	workerID string
	shardStore *db.ShardStore
	pools     map[string]*WorkerPool
	logger    *slog.Logger
	mu        sync.Mutex
}

func ParseShardsConfig(configStr string) map[string]string {
	result := make(map[string]string)
	if strings.TrimSpace(configStr) == "" {
		return result
	}

	parts := strings.Split(configStr, ",")
	for _, part := range parts {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) == 2 {
			result[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return result
}

func NewMultiShardWorkerPool(workerID string, shardsConfig map[string]string, assignedShards []string, exec *executor.Executor, logger *slog.Logger) (*MultiShardWorkerPool, error) {
	if len(shardsConfig) == 0 {
		return nil, fmt.Errorf("shardsConfig cannot be empty")
	}

	shardStore, err := db.NewShardStore(shardsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create shard store: %w", err)
	}

	targetShards := assignedShards
	if len(targetShards) == 0 {
		for shardID := range shardsConfig {
			targetShards = append(targetShards, shardID)
		}
	}

	pools := make(map[string]*WorkerPool)
	for _, shardID := range targetShards {
		store, err := shardStore.GetStoreForShard(shardID)
		if err != nil {
			logger.Warn("Skipping unconfigured shard", "shard_id", shardID, "error", err)
			continue
		}

		cfg := Config{
			WorkerID:          fmt.Sprintf("%s-%s", workerID, shardID),
			Concurrency:       4,
			PollInterval:      150 * time.Millisecond,
			HeartbeatInterval: 3 * time.Second,
		}

		pools[shardID] = NewWorkerPool(cfg, store, exec, logger)
	}

	return &MultiShardWorkerPool{
		workerID:   workerID,
		shardStore: shardStore,
		pools:      pools,
		logger:     logger,
	}, nil
}

func (ms *MultiShardWorkerPool) Start() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.logger.Info("Starting MultiShardWorkerPool", "worker_id", ms.workerID, "active_shards", len(ms.pools))
	for shardID, pool := range ms.pools {
		ms.logger.Info("Starting worker pool for shard", "shard_id", shardID)
		pool.Start()
	}
}

func (ms *MultiShardWorkerPool) Stop() {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.logger.Info("Stopping MultiShardWorkerPool...", "worker_id", ms.workerID)
	var wg sync.WaitGroup
	for shardID, pool := range ms.pools {
		wg.Add(1)
		go func(id string, p *WorkerPool) {
			defer wg.Done()
			p.Stop()
			ms.logger.Info("Stopped worker pool for shard", "shard_id", id)
		}(shardID, pool)
	}
	wg.Wait()
	ms.shardStore.Close()
	ms.logger.Info("MultiShardWorkerPool cleanly stopped", "worker_id", ms.workerID)
}
