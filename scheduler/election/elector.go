package election

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Elector struct {
	client      *clientv3.Client
	schedulerID string
	electionKey string
	ttlSeconds  int
	logger      *slog.Logger

	mu        sync.RWMutex
	isLeader  bool
	session   *concurrency.Session
	election  *concurrency.Election
	leaderVal string
}

func NewElector(client *clientv3.Client, schedulerID string, electionKey string, ttlSeconds int, logger *slog.Logger) *Elector {
	if electionKey == "" {
		electionKey = "/forge/leader"
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 10
	}
	return &Elector{
		client:      client,
		schedulerID: schedulerID,
		electionKey: electionKey,
		ttlSeconds:  ttlSeconds,
		logger:      logger,
	}
}

// Campaign blocks until elected leader or context is canceled.
func (e *Elector) Campaign(ctx context.Context) error {
	session, err := concurrency.NewSession(e.client, concurrency.WithTTL(e.ttlSeconds))
	if err != nil {
		return fmt.Errorf("failed to create etcd session: %w", err)
	}

	election := concurrency.NewElection(session, e.electionKey)

	e.logger.Info("Starting leader campaign", "scheduler_id", e.schedulerID, "key", e.electionKey)
	if err := election.Campaign(ctx, e.schedulerID); err != nil {
		session.Close()
		return fmt.Errorf("campaign failed: %w", err)
	}

	e.mu.Lock()
	e.session = session
	e.election = election
	e.isLeader = true
	e.leaderVal = e.schedulerID
	e.mu.Unlock()

	e.logger.Info("Elected as LEADER!", "scheduler_id", e.schedulerID)
	return nil
}

// IsLeader re-validates current leadership state with etcd to prevent split-brain traps.
func (e *Elector) IsLeader(ctx context.Context) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.isLeader || e.session == nil || e.election == nil {
		return false
	}

	// Check if the etcd session context is still valid
	select {
	case <-e.session.Done():
		e.logger.Warn("Etcd session expired, stepping down", "scheduler_id", e.schedulerID)
		return false
	default:
	}

	// Active re-validation query against etcd
	resp, err := e.client.Get(ctx, e.electionKey, clientv3.WithFirstCreate()...)
	if err != nil || len(resp.Kvs) == 0 {
		return false
	}

	currentLeader := string(resp.Kvs[0].Value)
	return currentLeader == e.schedulerID
}

func (e *Elector) Resign(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isLeader || e.election == nil {
		return nil
	}

	e.isLeader = false
	err := e.election.Resign(ctx)
	if e.session != nil {
		e.session.Close()
	}
	e.logger.Info("Resigned leadership", "scheduler_id", e.schedulerID)
	return err
}
