package poller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/forge/shared/db"
	"github.com/forge/shared/models"
	"github.com/forge/worker/executor"
)

type Config struct {
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
}

type WorkerPool struct {
	cfg      Config
	store    *db.Store
	exec     *executor.Executor
	logger   *slog.Logger
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewWorkerPool(cfg Config, store *db.Store, exec *executor.Executor, logger *slog.Logger) *WorkerPool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 3 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		cfg:    cfg,
		store:  store,
		exec:   exec,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}
}

func (wp *WorkerPool) Start() {
	wp.logger.Info("Starting worker pool", "worker_id", wp.cfg.WorkerID, "concurrency", wp.cfg.Concurrency)
	for i := 0; i < wp.cfg.Concurrency; i++ {
		workerSlotID := fmt.Sprintf("%s-slot-%d", wp.cfg.WorkerID, i)
		wp.wg.Add(1)
		go wp.workerLoop(workerSlotID)
	}
}

func (wp *WorkerPool) Stop() {
	wp.logger.Info("Stopping worker pool...", "worker_id", wp.cfg.WorkerID)
	wp.cancel()
	wp.wg.Wait()
	wp.logger.Info("Worker pool stopped clean", "worker_id", wp.cfg.WorkerID)
}

func (wp *WorkerPool) workerLoop(slotID string) {
	defer wp.wg.Done()

	ticker := time.NewTicker(wp.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case <-ticker.C:
			job, err := wp.store.ClaimNextJob(wp.ctx, slotID)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					wp.logger.Error("Failed to claim job", "worker_slot", slotID, "error", err)
				}
				continue
			}
			if job == nil {
				// No queued job available
				continue
			}

			wp.processJob(slotID, job)
		}
	}
}

func (wp *WorkerPool) processJob(slotID string, job *models.Job) {
	wp.logger.Info("Claimed job successfully", "worker_slot", slotID, "job_id", job.ID.String())

	// Context for heartbeats and execution
	jobCtx, jobCancel := context.WithCancel(wp.ctx)
	defer jobCancel()

	// Start background heartbeat goroutine with synchronized WaitGroup
	var hbWg sync.WaitGroup
	hbWg.Add(1)
	go func() {
		defer hbWg.Done()
		hbTicker := time.NewTicker(wp.cfg.HeartbeatInterval)
		defer hbTicker.Stop()

		for {
			select {
			case <-jobCtx.Done():
				return
			case <-hbTicker.C:
				if err := wp.store.SendHeartbeat(jobCtx, slotID, job.ID); err != nil {
					if !errors.Is(err, context.Canceled) {
						wp.logger.Warn("Failed to send heartbeat", "worker_slot", slotID, "job_id", job.ID.String(), "error", err)
					}
				}
			}
		}
	}()

	// Execute job
	execErr := wp.exec.Execute(jobCtx, job)

	// Stop heartbeat loop before updating final status
	jobCancel()
	hbWg.Wait()

	// Final status transition
	if execErr != nil {
		wp.logger.Error("Job execution failed", "job_id", job.ID.String(), "error", execErr)
		if err := wp.store.FailJob(context.Background(), job.ID, slotID, execErr.Error()); err != nil {
			wp.logger.Error("Failed to mark job failed", "job_id", job.ID.String(), "error", err)
		}
	} else {
		wp.logger.Info("Job completed successfully", "job_id", job.ID.String())
		if err := wp.store.CompleteJob(context.Background(), job.ID, slotID); err != nil {
			wp.logger.Error("Failed to mark job complete", "job_id", job.ID.String(), "error", err)
		}
	}
}
