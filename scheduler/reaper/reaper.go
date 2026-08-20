package reaper

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/forge/scheduler/election"
	"github.com/forge/shared/db"
)

type Config struct {
	ScanInterval     time.Duration
	HeartbeatTimeout time.Duration
}

type Reaper struct {
	cfg     Config
	store   *db.Store
	elector *election.Elector
	logger  *slog.Logger
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewReaper(cfg Config, store *db.Store, elector *election.Elector, logger *slog.Logger) *Reaper {
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = 5 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 15 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Reaper{
		cfg:     cfg,
		store:   store,
		elector: elector,
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (r *Reaper) Start() {
	r.logger.Info("Starting dead-worker reaper loop", "scan_interval", r.cfg.ScanInterval, "heartbeat_timeout", r.cfg.HeartbeatTimeout)
	r.wg.Add(1)
	go r.loop()
}

func (r *Reaper) Stop() {
	r.logger.Info("Stopping dead-worker reaper loop...")
	r.cancel()
	r.wg.Wait()
}

func (r *Reaper) loop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.cfg.ScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.reapStuckJobs()
		}
	}
}

func (r *Reaper) reapStuckJobs() {
	// Critical Split-Brain Guard: Re-validate active leadership prior to dispatch/reap action!
	if r.elector != nil && !r.elector.IsLeader(r.ctx) {
		r.logger.Debug("Not leader, skipping reaper scan")
		return
	}

	if r.store == nil {
		return
	}

	requeued, err := r.store.RequeueStuckJobs(r.ctx, r.cfg.HeartbeatTimeout)
	if err != nil {
		r.logger.Error("Failed during dead-worker reaper scan", "error", err)
		return
	}

	if requeued > 0 {
		r.logger.Info("Requeued stuck jobs from dead/timed-out workers", "count", requeued)
	}
}
