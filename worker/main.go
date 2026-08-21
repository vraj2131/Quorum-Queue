package main

import (
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/forge/shared/db"
	"github.com/forge/shared/metrics"
	"github.com/forge/worker/executor"
	"github.com/forge/worker/poller"
	"github.com/google/uuid"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = "worker-" + uuid.New().String()[:8]
	}

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "2112"
	}

	// Start Prometheus metrics server
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.MetricsHandler())
		logger.Info("Starting Prometheus metrics server", "port", metricsPort)
		if err := http.ListenAndServe(":"+metricsPort, mux); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server failed", "error", err)
		}
	}()

	exec := executor.NewExecutor(logger)

	shardsConfigEnv := os.Getenv("SHARDS_CONFIG")
	if shardsConfigEnv != "" {
		shardsConfig := poller.ParseShardsConfig(shardsConfigEnv)
		var assignedShards []string
		if assignedEnv := os.Getenv("ASSIGNED_SHARDS"); assignedEnv != "" {
			for _, s := range strings.Split(assignedEnv, ",") {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					assignedShards = append(assignedShards, trimmed)
				}
			}
		}

		multiPool, err := poller.NewMultiShardWorkerPool(workerID, shardsConfig, assignedShards, exec, logger)
		if err != nil {
			logger.Error("Failed to initialize multi-shard worker pool", "error", err)
			os.Exit(1)
		}

		multiPool.Start()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan

		logger.Info("Shutdown signal received")
		multiPool.Stop()
		return
	}

	// Legacy single DB fallback
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/forge?sslmode=disable"
	}

	database, err := db.Open(dbURL)
	if err != nil {
		logger.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	store := db.NewStore(database)

	poolCfg := poller.Config{
		WorkerID:          workerID,
		Concurrency:       5,
		PollInterval:      200 * time.Millisecond,
		HeartbeatInterval: 3 * time.Second,
	}

	pool := poller.NewWorkerPool(poolCfg, store, exec, logger)
	pool.Start()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutdown signal received")
	pool.Stop()
}
