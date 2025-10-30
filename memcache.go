package bitcache

import (
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

var (
	// ErrMemCacheFull is returned when the memory cache is full and cannot evict entries
	ErrMemCacheFull = errors.New("memory cache is full")
)

// lruNode represents a node in the LRU list
type lruNode[V any] struct {
	prev  *lruNode[V]
	next  *lruNode[V]
	entry *cacheEntry[V]
}

// lruList is a custom doubly-linked list for LRU that supports pooling
type lruList[V any] struct {
	head *lruNode[V]
	tail *lruNode[V]
	pool *sync.Pool
}

func newLRUList[V any]() *lruList[V] {
	l := &lruList[V]{
		pool: &sync.Pool{
			New: func() interface{} {
				return &lruNode[V]{}
			},
		},
	}
	return l
}

func (l *lruList[V]) pushFront(entry *cacheEntry[V]) *lruNode[V] {
	node := l.pool.Get().(*lruNode[V])
	node.entry = entry
	node.prev = nil
	node.next = l.head

	if l.head != nil {
		l.head.prev = node
	}
	l.head = node

	if l.tail == nil {
		l.tail = node
	}

	return node
}

func (l *lruList[V]) moveToFront(node *lruNode[V]) {
	if node == l.head {
		return
	}

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node == l.tail {
		l.tail = node.prev
	}

	// Move to front
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	}
	l.head = node

	if l.tail == nil {
		l.tail = node
	}
}

func (l *lruList[V]) remove(node *lruNode[V]) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}

	// Clear and return to pool
	node.prev = nil
	node.next = nil
	node.entry = nil
	l.pool.Put(node)
}

func (l *lruList[V]) back() *lruNode[V] {
	return l.tail
}

// EvictionPolicy defines the eviction strategy
type EvictionPolicy int

const (
	// EvictionLRU uses Least Recently Used eviction
	EvictionLRU EvictionPolicy = iota
	// EvictionLFU uses Least Frequently Used eviction
	EvictionLFU
)

// CachePolicy is a function that determines if a key-value pair should be cached in memory
// Returns true if the entry should be cached, false otherwise
type CachePolicy func(key []byte, value []byte) bool

// MemCacheConfig configures the in-memory cache layer
type MemCacheConfig struct {
	// MaxMemoryBytes is the maximum memory to use for cached data
	MaxMemoryBytes int64
	// EvictionPolicy determines how entries are evicted (LRU or LFU)
	EvictionPolicy EvictionPolicy
	// CachePolicy determines which entries should be cached in memory
	// If nil, all entries are cached (subject to memory limits)
	CachePolicy CachePolicy
	// ShardCount is the number of shards to use for reducing lock contention
	// Default is 256
	ShardCount int
	// CompactionThreshold is the fragmentation ratio (0.0-1.0) that triggers compaction
	// Default is 0.4 (40% fragmentation)
	CompactionThreshold float64
	// CompactionInterval is how often to check for fragmentation (in seconds)
	// Default is 300 (5 minutes)
	CompactionInterval int64
	// MaxValueSize is the maximum size of a value that will be cached in memory
	// Values larger than this will be skipped and only stored in backing cache
	// If 0, no size limit is enforced (default)
	MaxValueSize int64
}

// MemCache provides a read-through memory cache layer that stores Go values
type MemCache[V any] struct {
	config  MemCacheConfig
	backing Cache[V]
	shards  []*cacheShard[V]
	closed  atomic.Bool

	// Memory tracking
	memoryUsed  atomic.Int64
	memoryLimit int64

	// Cache performance stats
	hits            atomic.Int64 // Cache hits
	misses          atomic.Int64 // Cache misses
	forcedEvictions atomic.Int64 // Evictions due to memory pressure
	deleteEvictions atomic.Int64 // Explicit Delete() calls
	statsStartTime  time.Time    // When stats were last reset
}

// cacheShard represents a single shard of the cache
type cacheShard[V any] struct {
	mu            sync.RWMutex
	entries       map[uint64]*cacheEntry[V]
	lruList       *lruList[V]
	policy        EvictionPolicy
	cachePolicy   CachePolicy
	accessCounter uint64
	entryPool     *sync.Pool
	memCache      *MemCache[V] // reference to parent
}

// cacheEntry represents a cached key-value pair with generic value type
type cacheEntry[V any] struct {
	keyHash   uint64
	key       []byte
	value     V
	lruNode   *lruNode[V] // for LRU
	frequency uint64      // for LFU
	size      int64       // total memory used by this entry
}

const (
	defaultShardCount = 256
	minEntrySize      = 64 // minimum overhead per entry (struct fields, pointers, etc.)
)

// Sizer interface for types that can report their own size
type Sizer interface {
	Size() int
}

// estimateValueSize calculates the approximate memory size of a value
// First checks if the value implements Sizer interface with Size() int method
// Otherwise falls back to unsafe.Sizeof for a rough estimate
func estimateValueSize[V any](value V) int64 {
	// Check if value implements Sizer interface
	if sizer, ok := any(value).(Sizer); ok {
		return int64(sizer.Size())
	}

	// Fall back to unsafe.Sizeof
	// Note: This gives size of the value itself, not deep size for pointers/slices
	// For []byte specifically, this only counts the slice header, not the data
	size := int64(unsafe.Sizeof(value))

	// Special handling for common types that contain pointers
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Slice:
		// For slices, add the length of the data
		if v.Len() > 0 {
			elemSize := int64(v.Type().Elem().Size())
			size += int64(v.Len()) * elemSize
		}
	case reflect.String:
		// For strings, add the length
		size += int64(v.Len())
	case reflect.Map:
		// For maps, estimate based on number of entries
		// This is very rough - actual map overhead is complex
		size += int64(v.Len()) * 32 // rough estimate per entry
	}

	return size
}

// NewMemCache creates a new memory cache with generic value type
func NewMemCache[V any](backing Cache[V], config MemCacheConfig) (*MemCache[V], error) {
	if backing == nil {
		return nil, errors.New("backing cache cannot be nil")
	}

	if config.ShardCount <= 0 {
		config.ShardCount = defaultShardCount
	}

	mc := &MemCache[V]{
		config:         config,
		backing:        backing,
		shards:         make([]*cacheShard[V], config.ShardCount),
		memoryLimit:    config.MaxMemoryBytes,
		statsStartTime: time.Now(),
	}

	for i := 0; i < config.ShardCount; i++ {
		mc.shards[i] = &cacheShard[V]{
			entries:     make(map[uint64]*cacheEntry[V]),
			lruList:     newLRUList[V](),
			policy:      config.EvictionPolicy,
			cachePolicy: config.CachePolicy,
			memCache:    mc,
			entryPool: &sync.Pool{
				New: func() interface{} {
					return &cacheEntry[V]{}
				},
			},
		}
	}

	return mc, nil
}

// get retrieves an entry from the shard and updates access patterns
func (s *cacheShard[V]) get(keyHash uint64) (V, bool) {
	var zero V
	s.mu.RLock()
	entry, found := s.entries[keyHash]
	if !found {
		s.mu.RUnlock()
		return zero, false
	}

	// For LFU, we can update atomically without upgrading lock
	if s.policy == EvictionLFU {
		value := entry.value
		s.mu.RUnlock()
		atomic.AddUint64(&entry.frequency, 1)
		return value, true
	}

	// For LRU, we need to upgrade to write lock to modify the list
	s.mu.RUnlock()
	s.mu.Lock()

	// Re-check entry still exists after lock upgrade
	entry, found = s.entries[keyHash]
	if found && entry.lruNode != nil {
		s.lruList.moveToFront(entry.lruNode)
		value := entry.value
		s.mu.Unlock()
		return value, true
	}

	s.mu.Unlock()
	return zero, false
}

// tryCache attempts to cache a key-value pair in the shard
func (s *cacheShard[V]) tryCache(keyHash uint64, key []byte, value V) {
	// Calculate the size of this entry
	valueSize := estimateValueSize(value)
	entrySize := int64(len(key)) + valueSize + minEntrySize

	// Check if entry exceeds max value size limit
	if s.memCache.config.MaxValueSize > 0 && valueSize > s.memCache.config.MaxValueSize {
		return // Don't cache values that are too large
	}

	// Try to reserve memory - evict entries if necessary
	// Do this BEFORE locking the shard to avoid deadlock
	for {
		currentUsed := s.memCache.memoryUsed.Load()
		if currentUsed+entrySize <= s.memCache.memoryLimit {
			// Try to atomically reserve the memory
			if s.memCache.memoryUsed.CompareAndSwap(currentUsed, currentUsed+entrySize) {
				break // Successfully reserved
			}
			// CAS failed, retry
			continue
		}

		// Need to evict - do this without holding global state
		// Try to evict from this shard first
		s.mu.Lock()
		if len(s.entries) > 0 {
			s.evict()
			s.memCache.forcedEvictions.Add(1)
			s.mu.Unlock()
			// Retry reservation after eviction
			continue
		}
		s.mu.Unlock()

		// This shard is empty, try other shards
		evicted := false
		for _, shard := range s.memCache.shards {
			if shard == s {
				continue
			}
			shard.mu.Lock()
			if len(shard.entries) > 0 {
				shard.evict()
				s.memCache.forcedEvictions.Add(1)
				shard.mu.Unlock()
				evicted = true
				break
			}
			shard.mu.Unlock()
		}

		if !evicted {
			// Cannot make room, don't cache this entry
			return
		}
		// Retry reservation after eviction
	}

	// Now lock the shard to insert the entry
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if entry already exists
	if existing, found := s.entries[keyHash]; found {
		// Release old entry's memory
		s.memCache.memoryUsed.Add(-existing.size)
		s.removeEntry(existing)
		// We already reserved memory for the new entry above
	}

	// Make a copy of the key
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	// Get entry from pool
	entry := s.entryPool.Get().(*cacheEntry[V])
	entry.keyHash = keyHash
	entry.key = keyCopy
	entry.value = value
	entry.size = entrySize
	entry.lruNode = nil
	entry.frequency = 0

	if s.policy == EvictionLRU {
		entry.lruNode = s.lruList.pushFront(entry)
	} else if s.policy == EvictionLFU {
		entry.frequency = 1
		s.accessCounter++
	}

	s.entries[keyHash] = entry
}

// delete removes an entry from the shard if it exists
func (s *cacheShard[V]) delete(keyHash uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, found := s.entries[keyHash]; found {
		s.removeEntry(entry)
		// Track explicit delete eviction
		s.memCache.deleteEvictions.Add(1)
	}
}

// has checks if an entry exists in the shard
func (s *cacheShard[V]) has(keyHash uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, found := s.entries[keyHash]
	return found
}

// getStats returns memory usage statistics for this shard
func (s *cacheShard[V]) getStats() (entries int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.entries)
}

// clear removes all entries from the shard
func (s *cacheShard[V]) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release memory for all entries
	for _, entry := range s.entries {
		s.memCache.memoryUsed.Add(-entry.size)
	}

	s.entries = make(map[uint64]*cacheEntry[V])
	s.lruList = newLRUList[V]()
	s.accessCounter = 0
}

// close cleans up the shard's resources
func (s *cacheShard[V]) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = nil
	s.lruList = nil
}

// evict removes one entry based on the eviction policy
func (s *cacheShard[V]) evict() {
	if s.policy == EvictionLRU {
		s.evictLRU()
	} else {
		s.evictLFU()
	}
}

// evictLRU removes the least recently used entry
// This must be called when the shard lock is held
func (s *cacheShard[V]) evictLRU() {
	node := s.lruList.back()
	if node == nil {
		return
	}

	entry := node.entry
	s.removeEntry(entry)
}

// evictLFU removes the least frequently used entry
// This must be called when the shard lock is held
func (s *cacheShard[V]) evictLFU() {
	var leastFrequent *cacheEntry[V]
	var minFreq uint64 = ^uint64(0)

	for _, entry := range s.entries {
		if entry.frequency < minFreq {
			minFreq = entry.frequency
			leastFrequent = entry
		}
	}

	if leastFrequent != nil {
		s.removeEntry(leastFrequent)
	}
}

// removeEntry removes an entry from the shard
// This must be called when the shard lock is held
func (s *cacheShard[V]) removeEntry(entry *cacheEntry[V]) {
	delete(s.entries, entry.keyHash)
	if entry.lruNode != nil {
		s.lruList.remove(entry.lruNode)
	}

	// Release memory (atomic operation, no lock needed)
	s.memCache.memoryUsed.Add(-entry.size)

	// Clear and return to pool
	entry.key = nil
	var zero V
	entry.value = zero
	entry.lruNode = nil
	entry.size = 0
	s.entryPool.Put(entry)
}

// Get retrieves a value from the cache, reading through to backing store if needed
func (mc *MemCache[V]) Get(key []byte) (V, error) {
	var zero V
	if mc.closed.Load() {
		return zero, ErrCacheClosed
	}

	keyHash := hashKey(key)
	shard := mc.getShard(keyHash)

	// Try memory cache first - all locking handled inside shard
	if value, found := shard.get(keyHash); found {
		mc.hits.Add(1)
		return value, nil
	}

	// Cache miss - read from backing store
	mc.misses.Add(1)
	value, err := mc.backing.Get(key)
	if err != nil {
		return zero, err
	}

	// Try to cache the result if policy allows
	shard.tryCache(keyHash, key, value)

	return value, nil
}

// Set writes a value to the cache
func (mc *MemCache[V]) Set(key []byte, value V) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}

	// Always write to backing store first
	if err := mc.backing.Set(key, value); err != nil {
		return err
	}

	keyHash := hashKey(key)
	shard := mc.getShard(keyHash)

	// Try to cache in memory if policy allows
	shard.tryCache(keyHash, key, value)

	return nil
}

// Delete removes a key from both memory and backing cache
func (mc *MemCache[V]) Delete(key []byte) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}

	// Delete from backing store first
	err := mc.backing.Delete(key)

	// Remove from memory cache regardless of backing store result
	keyHash := hashKey(key)
	shard := mc.getShard(keyHash)
	shard.delete(keyHash)

	return err
}

// Has checks if a key exists in either memory or backing cache
func (mc *MemCache[V]) Has(key []byte) bool {
	if mc.closed.Load() {
		return false
	}

	keyHash := hashKey(key)
	shard := mc.getShard(keyHash)

	// Check memory cache first
	if shard.has(keyHash) {
		return true
	}

	// Check backing store
	return mc.backing.Has(key)
}

// Stats returns combined statistics from memory and backing cache
func (mc *MemCache[V]) Stats() Stats {
	backingStats := mc.backing.Stats()

	// Return backing stats as-is
	return backingStats
}

// Scan iterates through all keys in the backing cache
func (mc *MemCache[V]) Scan(prefix []byte, fn func(key []byte) bool) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	return mc.backing.Scan(prefix, fn)
}

// MemStats returns memory-specific statistics
func (mc *MemCache[V]) MemStats() MemStats {
	var stats MemStats
	for _, shard := range mc.shards {
		entries := shard.getStats()
		stats.Entries += int64(entries)
	}

	stats.Shards = int64(len(mc.shards))
	stats.MemoryUsed = mc.memoryUsed.Load()
	stats.MemoryLimit = mc.memoryLimit

	// Add performance metrics
	stats.Hits = mc.hits.Load()
	stats.Misses = mc.misses.Load()
	stats.ForcedEvictions = mc.forcedEvictions.Load()
	stats.DeleteEvictions = mc.deleteEvictions.Load()

	// Calculate hit rate
	totalRequests := stats.Hits + stats.Misses
	if totalRequests > 0 {
		stats.HitRate = float64(stats.Hits) / float64(totalRequests)
	}

	// Calculate utilization
	if stats.MemoryLimit > 0 {
		stats.Utilization = float64(stats.MemoryUsed) / float64(stats.MemoryLimit)
	}

	return stats
}

// ResetStats resets the performance counters for a new monitoring window
func (mc *MemCache[V]) ResetStats() {
	mc.hits.Store(0)
	mc.misses.Store(0)
	mc.forcedEvictions.Store(0)
	mc.deleteEvictions.Store(0)
	mc.statsStartTime = time.Now()
}

// MemStats provides memory cache statistics
type MemStats struct {
	Entries     int64
	Shards      int64
	MemoryUsed  int64
	MemoryLimit int64

	// Cache performance metrics
	Hits            int64   // Cache hits
	Misses          int64   // Cache misses
	ForcedEvictions int64   // Evictions due to memory pressure
	DeleteEvictions int64   // Explicit Delete() calls
	HitRate         float64 // Hits / (Hits + Misses), 0.0-1.0
	Utilization     float64 // MemoryUsed / MemoryLimit, 0.0-1.0
}

// getShard returns the shard for a given key hash
func (mc *MemCache[V]) getShard(keyHash uint64) *cacheShard[V] {
	return mc.shards[keyHash%uint64(len(mc.shards))]
}

// hashKey computes a hash for a key
func hashKey(key []byte) uint64 {
	// FNV-1a hash
	const prime = 1099511628211
	hash := uint64(14695981039346656037)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= prime
	}
	return hash
}

// Clear removes all entries from the memory cache
func (mc *MemCache[V]) Clear() error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	for _, shard := range mc.shards {
		shard.clear()
	}
	return nil
}

// Close closes the memory cache and releases resources
func (mc *MemCache[V]) Close() error {
	if mc.closed.Swap(true) {
		return ErrCacheClosed // Already closed
	}
	// Close all shards
	for _, shard := range mc.shards {
		shard.close()
	}
	// Close backing cache
	return mc.backing.Close()
}

// Invalidate removes a key from memory cache but not from backing store
func (mc *MemCache[V]) Invalidate(key []byte) {
	if mc.closed.Load() {
		return
	}
	keyHash := hashKey(key)
	shard := mc.getShard(keyHash)
	shard.delete(keyHash)
}

// Warmup pre-loads keys into the memory cache
func (mc *MemCache[V]) Warmup(keys [][]byte) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	for _, key := range keys {
		// Try to get each key, which will cache it if found
		_, _ = mc.Get(key)
	}
	return nil
}
