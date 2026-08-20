package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/forge/scheduler/election"
	"github.com/forge/scheduler/reaper"
	"github.com/forge/shared/db"
	"github.com/forge/shared/metrics"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	schedulerID := os.Getenv("SCHEDULER_ID")
	if schedulerID == "" {
		schedulerID = "scheduler-" + uuid.New().String()[:8]
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/forge?sslmode=disable"
	}

	etcdEndpointsEnv := os.Getenv("ETCD_ENDPOINTS")
	if etcdEndpointsEnv == "" {
		etcdEndpointsEnv = "localhost:2379"
	}
	endpoints := strings.Split(etcdEndpointsEnv, ",")

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "2113"
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

	database, err := db.Open(dbURL)
	if err != nil {
		logger.Error("Failed to connect to postgres", "error", err)
		os.Exit(1)
	}
	defer database.Close()

	store := db.NewStore(database)

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Error("Failed to connect to etcd", "error", err)
		os.Exit(1)
	}
	defer etcdClient.Close()

	elector := election.NewElector(etcdClient, schedulerID, "/forge/leader", 10, logger)

	reaperCfg := reaper.Config{
		ScanInterval:     3 * time.Second,
		HeartbeatTimeout: 10 * time.Second,
	}
	reap := reaper.NewReaper(reaperCfg, store, elector, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				metrics.LeaderStatus.WithLabelValues(schedulerID).Set(0)
				logger.Info("Campaigning for leadership...", "scheduler_id", schedulerID)
				if err := elector.Campaign(ctx); err != nil {
					logger.Error("Campaign lost or error", "error", err)
					time.Sleep(2 * time.Second)
					continue
				}

				// Elected leader: set metric to 1 and start reaper
				metrics.LeaderStatus.WithLabelValues(schedulerID).Set(1)
				reap.Start()

				// Wait until campaign/session finishes or signal received
				<-ctx.Done()
				metrics.LeaderStatus.WithLabelValues(schedulerID).Set(0)
				reap.Stop()
				_ = elector.Resign(context.Background())
				return
			}
		}
	}()

	<-sigChan
	logger.Info("Shutdown signal received, shutting down scheduler...")
	cancel()
	time.Sleep(500 * time.Millisecond)
}
