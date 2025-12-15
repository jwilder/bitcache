package bitcache

import "sync"

// partitionedKeyDir provides a sharded map with segment-based storage for efficient eviction.
//
// Structure: shards[shardIdx].segments[fileID][key] -> *keyEntry
//
// Keys are distributed across shards using FNV-1a hash for reduced lock contention.
// Within each shard, entries are organized by segment ID (fileID) for O(1) segment eviction.
// When a segment is deleted during compaction, we simply delete the segment map which
// removes all associated keys at once.
type partitionedKeyDir struct {
	shards    []*keyDirShard
	numShards int
}

// keyDirShard is a single partition containing segment-based maps
type keyDirShard struct {
	mu       sync.RWMutex
	segments map[uint32]map[string]*keyEntry // segmentID -> key -> entry
}

// newPartitionedKeyDir creates a new partitioned key directory
func newPartitionedKeyDir(numShards int) *partitionedKeyDir {
	if numShards <= 0 {
		numShards = 32 // Default to 32 shards
	}

	pkd := &partitionedKeyDir{
		shards:    make([]*keyDirShard, numShards),
		numShards: numShards,
	}

	for i := 0; i < numShards; i++ {
		pkd.shards[i] = &keyDirShard{
			segments: make(map[uint32]map[string]*keyEntry),
		}
	}

	return pkd
}

// getShardIndex returns the shard index for a key using FNV-1a hash
func (p *partitionedKeyDir) getShardIndex(key string) int {
	h := uint32(2166136261) // FNV offset basis
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619 // FNV prime
	}
	return int(h % uint32(p.numShards))
}

// Get retrieves an entry by iterating through all segments in the shard.
// Since this is a write-once cache, there should only be one entry per key.
func (p *partitionedKeyDir) Get(key string) (*keyEntry, bool) {
	shard := p.shards[p.getShardIndex(key)]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	// Iterate through all segments to find the key
	for _, segmentMap := range shard.segments {
		if entry, exists := segmentMap[key]; exists {
			return entry, true
		}
	}

	return nil, false
}

// Set stores an entry in the appropriate shard and segment
func (p *partitionedKeyDir) Set(key string, entry *keyEntry) {
	shard := p.shards[p.getShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Get or create segment map
	segmentMap := shard.segments[entry.fileID]
	if segmentMap == nil {
		segmentMap = make(map[string]*keyEntry)
		shard.segments[entry.fileID] = segmentMap
	}

	// Store the entry
	segmentMap[key] = entry
}

// Delete removes a single entry
func (p *partitionedKeyDir) Delete(key string) bool {
	shard := p.shards[p.getShardIndex(key)]
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Find and delete from the appropriate segment
	for _, segmentMap := range shard.segments {
		if _, exists := segmentMap[key]; exists {
			delete(segmentMap, key)
			return true
		}
	}

	return false
}

// EvictSegment removes all keys for a segment by deleting the segment map.
// This is O(1) per shard - we just delete the entire segment map without
// scanning individual keys.
func (p *partitionedKeyDir) EvictSegment(fileID uint32) int64 {
	keysRemoved := int64(0)

	// Delete the segment from each shard
	for _, shard := range p.shards {
		shard.mu.Lock()
		if segmentMap, exists := shard.segments[fileID]; exists {
			keysRemoved += int64(len(segmentMap))
			delete(shard.segments, fileID)
		}
		shard.mu.Unlock()
	}

	return keysRemoved
}

// Len returns the total number of entries across all shards and segments
func (p *partitionedKeyDir) Len() int {
	total := 0
	for _, shard := range p.shards {
		shard.mu.RLock()
		for _, segmentMap := range shard.segments {
			total += len(segmentMap)
		}
		shard.mu.RUnlock()
	}
	return total
}

// Range iterates over all entries (order not guaranteed)
func (p *partitionedKeyDir) Range(fn func(key string, entry *keyEntry) bool) {
	for _, shard := range p.shards {
		shard.mu.RLock()
		shouldStop := false
		for _, segmentMap := range shard.segments {
			for k, v := range segmentMap {
				if fn(k, v) {
					shouldStop = true
					break
				}
			}
			if shouldStop {
				break
			}
		}
		shard.mu.RUnlock()
		if shouldStop {
			return
		}
	}
}
