package db

import (
	"fmt"
	"sync"

	"github.com/forge/shared/router"
)

type ShardStore struct {
	mu     sync.RWMutex
	ring   *router.HashRing
	stores map[string]*Store
}

func NewShardStore(shardsConfig map[string]string) (*ShardStore, error) {
	ring := router.NewHashRing(256)
	stores := make(map[string]*Store)

	for shardID, dbURL := range shardsConfig {
		dbConn, err := Open(dbURL)
		if err != nil {
			return nil, fmt.Errorf("failed to open database for shard %s (%s): %w", shardID, dbURL, err)
		}
		stores[shardID] = NewStore(dbConn)
		ring.AddShard(shardID)
	}

	return &ShardStore{
		ring:   ring,
		stores: stores,
	}, nil
}

func (ss *ShardStore) GetStoreForTenant(tenantID string) (*Store, string, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	shardID, err := ss.ring.GetShard(tenantID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to route tenant to shard: %w", err)
	}

	store, exists := ss.stores[shardID]
	if !exists {
		return nil, "", fmt.Errorf("shard %s not configured in store pool", shardID)
	}

	return store, shardID, nil
}

func (ss *ShardStore) GetStoreForShard(shardID string) (*Store, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	store, exists := ss.stores[shardID]
	if !exists {
		return nil, fmt.Errorf("shard %s not found in store pool", shardID)
	}
	return store, nil
}

func (ss *ShardStore) GetAllShards() map[string]*Store {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	result := make(map[string]*Store, len(ss.stores))
	for k, v := range ss.stores {
		result[k] = v
	}
	return result
}

func (ss *ShardStore) Close() {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, store := range ss.stores {
		if store.DB != nil {
			_ = store.DB.Close()
		}
	}
}
