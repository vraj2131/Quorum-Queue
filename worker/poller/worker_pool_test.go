package poller

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/forge/worker/executor"
)

func TestWorkerPool_StartStop(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	exec := executor.NewExecutor(logger)

	cfg := Config{
		WorkerID:          "test-worker-1",
		Concurrency:       2,
		PollInterval:      50 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
	}

	// Pass nil store since no jobs will be polled without DB in unit test
	pool := NewWorkerPool(cfg, nil, exec, logger)
	pool.Start()
	time.Sleep(100 * time.Millisecond)
	pool.Stop()
}
