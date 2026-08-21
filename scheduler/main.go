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
	"github.com/forge/worker/poller"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	schedulerID := os.Getenv("SCHEDULER_ID")
	if schedulerID == "" {
		schedulerID = "scheduler-" + uuid.New().String()[:8]
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

	shardsConfigEnv := os.Getenv("SHARDS_CONFIG")
	var multiReaper *reaper.MultiShardReaper
	var singleReaper *reaper.Reaper
	var database *db.Store

	if shardsConfigEnv != "" {
		shardsConfig := poller.ParseShardsConfig(shardsConfigEnv)
		shardStore, err := db.NewShardStore(shardsConfig)
		if err != nil {
			logger.Error("Failed to connect to multi-shard database pool", "error", err)
			os.Exit(1)
		}
		defer shardStore.Close()
		multiReaper = reaper.NewMultiShardReaper(reaperCfg, shardStore, elector, logger)
	} else {
		dbURL := os.Getenv("DATABASE_URL")
		if dbURL == "" {
			dbURL = "postgres://postgres:postgres@localhost:5432/forge?sslmode=disable"
		}
		dbConn, err := db.Open(dbURL)
		if err != nil {
			logger.Error("Failed to connect to postgres", "error", err)
			os.Exit(1)
		}
		defer dbConn.Close()
		database = db.NewStore(dbConn)
		singleReaper = reaper.NewReaper(reaperCfg, database, elector, logger)
	}

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
				if multiReaper != nil {
					multiReaper.Start()
				} else if singleReaper != nil {
					singleReaper.Start()
				}

				// Wait until campaign/session finishes or signal received
				<-ctx.Done()
				metrics.LeaderStatus.WithLabelValues(schedulerID).Set(0)
				if multiReaper != nil {
					multiReaper.Stop()
				} else if singleReaper != nil {
					singleReaper.Stop()
				}
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
