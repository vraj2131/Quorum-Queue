package router

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type HashRing struct {
	mu           sync.RWMutex
	vnodesPerNode int
	ring         []uint32
	vnodeToShard map[uint32]string
	shards       map[string]bool
}

func NewHashRing(vnodesPerNode int) *HashRing {
	if vnodesPerNode <= 0 {
		vnodesPerNode = 256
	}
	return &HashRing{
		vnodesPerNode: vnodesPerNode,
		ring:          make([]uint32, 0),
		vnodeToShard:  make(map[uint32]string),
		shards:        make(map[string]bool),
	}
}

func (h *HashRing) hashKey(key string) uint32 {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return hasher.Sum32()
}

func (h *HashRing) AddShard(shardID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.shards[shardID] {
		return
	}
	h.shards[shardID] = true

	for i := 0; i < h.vnodesPerNode; i++ {
		vnodeKey := shardID + "#" + strconv.Itoa(i)
		hash := h.hashKey(vnodeKey)
		h.ring = append(h.ring, hash)
		h.vnodeToShard[hash] = shardID
	}

	sort.Slice(h.ring, func(i, j int) bool {
		return h.ring[i] < h.ring[j]
	})
}

func (h *HashRing) RemoveShard(shardID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if !h.shards[shardID] {
		return
	}
	delete(h.shards, shardID)

	newRing := make([]uint32, 0, len(h.ring))
	for _, hash := range h.ring {
		if h.vnodeToShard[hash] == shardID {
			delete(h.vnodeToShard, hash)
		} else {
			newRing = append(newRing, hash)
		}
	}
	h.ring = newRing
}

func (h *HashRing) GetShard(key string) (string, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(h.ring) == 0 {
		return "", fmt.Errorf("no shards available in hash ring")
	}

	hash := h.hashKey(key)

	// Binary search for first vnode with hash >= target hash
	idx := sort.Search(len(h.ring), func(i int) bool {
		return h.ring[i] >= hash
	})

	if idx == len(h.ring) {
		idx = 0 // Wrap around ring
	}

	shardID := h.vnodeToShard[h.ring[idx]]
	return shardID, nil
}

func (h *HashRing) GetShards() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]string, 0, len(h.shards))
	for shardID := range h.shards {
		result = append(result, shardID)
	}
	sort.Strings(result)
	return result
}
