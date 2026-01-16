package node

import (
	"sync"
)

// shardedNodeMap is a thread-safe map with sharding to reduce lock contention
// This is a local implementation to avoid circular dependency with assets package
type shardedNodeMap struct {
	shards    []*nodeMapShard
	shardMask uint32
}

type nodeMapShard struct {
	mu    sync.RWMutex
	items map[string]*Node
}

// newShardedNodeMap creates a new sharded map with the specified number of shards
func newShardedNodeMap(shardCount int) *shardedNodeMap {
	// Ensure shardCount is a power of 2
	if shardCount <= 0 || (shardCount&(shardCount-1)) != 0 {
		shardCount = 32 // default to 32 shards
	}

	sm := &shardedNodeMap{
		shards:    make([]*nodeMapShard, shardCount),
		shardMask: uint32(shardCount - 1),
	}

	for i := 0; i < shardCount; i++ {
		sm.shards[i] = &nodeMapShard{
			items: make(map[string]*Node, 100), // pre-allocate
		}
	}

	return sm
}

// getShard returns the shard for the given key
func (sm *shardedNodeMap) getShard(key string) *nodeMapShard {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return sm.shards[hash&sm.shardMask]
}

// Store stores a value for the given key
func (sm *shardedNodeMap) Store(key string, value *Node) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

// Load retrieves a value for the given key
func (sm *shardedNodeMap) Load(key string) (interface{}, bool) {
	shard := sm.getShard(key)
	shard.mu.RLock()
	val, ok := shard.items[key]
	shard.mu.RUnlock()
	return val, ok
}

// LoadOrStore loads the value for the given key if it exists, otherwise stores the new value
func (sm *shardedNodeMap) LoadOrStore(key string, value *Node) (interface{}, bool) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if actual, loaded := shard.items[key]; loaded {
		return actual, true
	}
	shard.items[key] = value
	return value, false
}

// Delete removes the value for the given key
func (sm *shardedNodeMap) LoadAndDelete(key string) (interface{}, bool) {
	shard := sm.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	value, loaded := shard.items[key]
	if loaded {
		delete(shard.items, key)
	}
	return value, loaded
}

// Range calls f sequentially for each key and value present in the map
func (sm *shardedNodeMap) Range(f func(key string, value interface{}) bool) {
	for _, shard := range sm.shards {
		shard.mu.RLock()
		// Copy pointers to avoid holding lock during callback
		nodes := make([]*Node, 0, len(shard.items))
		for _, v := range shard.items {
			nodes = append(nodes, v)
		}
		shard.mu.RUnlock()

		for _, node := range nodes {
			if !f(node.NodeID, node) {
				return
			}
		}
	}
}
