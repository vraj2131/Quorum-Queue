package reaper

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/forge/scheduler/election"
	"github.com/forge/shared/db"
)

type MultiShardReaper struct {
	cfg        Config
	shardStore *db.ShardStore
	elector    *election.Elector
	logger     *slog.Logger
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewMultiShardReaper(cfg Config, shardStore *db.ShardStore, elector *election.Elector, logger *slog.Logger) *MultiShardReaper {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = 5 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 15 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &MultiShardReaper{
		cfg:        cfg,
		shardStore: shardStore,
		elector:    elector,
		logger:     logger,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (ms *MultiShardReaper) Start() {
	ms.logger.Info("Starting Parallel MultiShardReaper loop", "scan_interval", ms.cfg.ScanInterval, "heartbeat_timeout", ms.cfg.HeartbeatTimeout)
	ms.wg.Add(1)
	go ms.loop()
}

func (ms *MultiShardReaper) Stop() {
	ms.logger.Info("Stopping Parallel MultiShardReaper loop...")
	ms.cancel()
	ms.wg.Wait()
}

func (ms *MultiShardReaper) loop() {
	defer ms.wg.Done()
	ticker := time.NewTicker(ms.cfg.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ms.ctx.Done():
			return
		case <-ticker.C:
			ms.reapAllShards()
		}
	}
}

func (ms *MultiShardReaper) reapAllShards() {
	// Critical Split-Brain Guard: Re-validate active leadership prior to dispatch/reap action!
	if ms.elector != nil && !ms.elector.IsLeader(ms.ctx) {
		ms.logger.Debug("Not leader, skipping multi-shard reaper scan")
		return
	}

	if ms.shardStore == nil {
		return
	}

	allShards := ms.shardStore.GetAllShards()
	if len(allShards) == 0 {
		return
	}

	var totalRequeued atomic.Int64
	var scanWg sync.WaitGroup

	for shardID, store := range allShards {
		scanWg.Add(1)
		go func(id string, s *db.Store) {
			defer scanWg.Done()
			requeued, err := s.RequeueStuckJobs(ms.ctx, ms.cfg.HeartbeatTimeout)
			if err != nil {
				ms.logger.Error("Failed during multi-shard reaper scan", "shard_id", id, "error", err)
				return
			}
			if requeued > 0 {
				totalRequeued.Add(int64(requeued))
				ms.logger.Info("Requeued stuck jobs on shard", "shard_id", id, "count", requeued)
			}
		}(shardID, store)
	}

	scanWg.Wait()

	if total := totalRequeued.Load(); total > 0 {
		ms.logger.Info("MultiShardReaper completed parallel scan across all shards", "total_requeued", total)
	}
}
