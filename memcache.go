package bitcache

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrMemCacheFull is returned when the memory cache is full and cannot evict entries
	ErrMemCacheFull = errors.New("memory cache is full")
)

// lruNode represents a node in the LRU list
type lruNode struct {
	prev  *lruNode
	next  *lruNode
	entry *cacheEntry
}

// lruList is a custom doubly-linked list for LRU that supports pooling
type lruList struct {
	head *lruNode
	tail *lruNode
	pool *sync.Pool
}

func newLRUList() *lruList {
	l := &lruList{
		pool: &sync.Pool{
			New: func() interface{} {
				return &lruNode{}
			},
		},
	}
	return l
}

func (l *lruList) pushFront(entry *cacheEntry) *lruNode {
	node := l.pool.Get().(*lruNode)
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

func (l *lruList) moveToFront(node *lruNode) {
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

func (l *lruList) remove(node *lruNode) {
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

func (l *lruList) back() *lruNode {
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

// MemCache provides a read-through memory cache layer
type MemCache struct {
	config         MemCacheConfig
	backing        Cache
	shards         []*cacheShard
	closed         atomic.Bool
	compactionDone chan struct{}
	// Global memory tracking
	globalMu    sync.RWMutex
	arena       *memoryArena
	memoryUsed  atomic.Int64
	memoryLimit int64

	// Rate limiting
	lastCompaction atomic.Int64 // Unix timestamp in milliseconds
	lastEviction   atomic.Int64 // Unix timestamp in milliseconds
	memoryPressure atomic.Int32 // 0-100, percentage of memory used

	// Cache performance stats
	hits            atomic.Int64 // Cache hits
	misses          atomic.Int64 // Cache misses
	forcedEvictions atomic.Int64 // Evictions due to memory pressure
	deleteEvictions atomic.Int64 // Explicit Delete() calls
	statsStartTime  time.Time    // When stats were last reset
}

// cacheShard represents a single shard of the cache
type cacheShard struct {
	mu            sync.RWMutex
	entries       map[uint64]*cacheEntry
	lruList       *lruList
	policy        EvictionPolicy
	cachePolicy   CachePolicy
	maxValueSize  int64
	accessCounter uint64
	entryPool     *sync.Pool
	memCache      *MemCache // reference to parent for global memory tracking

	// Backoff tracking
	consecutiveFailures atomic.Int32
	backoffUntil        atomic.Int64 // Unix timestamp in milliseconds
}

// cacheEntry represents a cached key-value pair
type cacheEntry struct {
	keyHash   uint64
	key       []byte
	value     []byte
	lruNode   *lruNode // for LRU
	frequency uint64   // for LFU
	size      int64    // total memory used by this entry
}

// memoryArena manages a pool of large byte slices to minimize GC
type memoryArena struct {
	slabs     []*slab
	slabSize  int64
	allocated int64
	tierIndex int       // current tier level (0-4)
	memCache  *MemCache // reference to parent for memory limit checks
}

// slab represents a single large byte slice allocation
type slab struct {
	data   []byte
	offset int64
}

const (
	defaultSlabSize   = 32 * 1024 * 1024 // 32MB per slab (max tier)
	defaultShardCount = 256
	minEntrySize      = 32 // minimum bytes per entry (overhead)
)

// slabSizeTiers defines the progressive slab sizes - power of 2 for better alignment
var slabSizeTiers = []int64{
	1 * 1024 * 1024,  // 1MB  - Tier 0: 2^20
	2 * 1024 * 1024,  // 2MB  - Tier 1: 2^21
	4 * 1024 * 1024,  // 4MB  - Tier 2: 2^22
	8 * 1024 * 1024,  // 8MB  - Tier 3: 2^23
	16 * 1024 * 1024, // 16MB - Tier 4: 2^24
	defaultSlabSize,  // 32MB - Tier 5: 2^25 (max)
}

// memoryArena manages a pool of large byte slices to minimize GC
func NewMemCache(backing Cache, config MemCacheConfig) (*MemCache, error) {
	if backing == nil {
		return nil, errors.New("backing cache cannot be nil")
	}

	if config.MaxMemoryBytes <= 0 {
		return nil, errors.New("MaxMemoryBytes must be positive")
	}

	if config.ShardCount <= 0 {
		config.ShardCount = defaultShardCount
	}

	if config.CompactionThreshold <= 0 {
		config.CompactionThreshold = 0.4 // Default: compact at 40% fragmentation
	}
	if config.CompactionInterval <= 0 {
		config.CompactionInterval = 300 // Default: check every 5 minutes
	}

	mc := &MemCache{
		config:         config,
		backing:        backing,
		shards:         make([]*cacheShard, config.ShardCount),
		compactionDone: make(chan struct{}),
		memoryLimit:    config.MaxMemoryBytes,
		statsStartTime: time.Now(),
	}

	// Initialize arena after mc is created so it can reference mc
	mc.arena = newMemoryArena(mc)

	for i := 0; i < config.ShardCount; i++ {
		mc.shards[i] = &cacheShard{
			entries:      make(map[uint64]*cacheEntry),
			lruList:      newLRUList(),
			policy:       config.EvictionPolicy,
			cachePolicy:  config.CachePolicy,
			maxValueSize: config.MaxValueSize,
			memCache:     mc,
			entryPool: &sync.Pool{
				New: func() interface{} {
					return &cacheEntry{}
				},
			},
		}
	}

	// Start background compaction worker
	mc.startCompactionWorker()

	return mc, nil
}

// get retrieves an entry from the shard and updates access patterns
func (s *cacheShard) get(keyHash uint64) ([]byte, bool) {
	s.mu.RLock()
	entry, found := s.entries[keyHash]
	if !found {
		s.mu.RUnlock()
		return nil, false
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
	return nil, false
}

// tryCache attempts to cache a key-value pair in the shard
func (s *cacheShard) tryCache(keyHash uint64, key []byte, value []byte) {
	// Check if we're in backoff period
	if backoffTime := s.backoffUntil.Load(); backoffTime > 0 {
		nowMs := time.Now().UnixMilli()
		if nowMs < backoffTime {
			return
		}
		// Backoff expired, reset
		s.backoffUntil.Store(0)
		s.consecutiveFailures.Store(0)
	}

	// Check memory pressure - skip caching when critically high (> 95%)
	if s.memCache.memoryPressure.Load() > 95 {
		return
	}

	// Check if value exceeds maximum size limit
	if s.maxValueSize > 0 && int64(len(value)) > s.maxValueSize {
		return
	}

	// Check cache policy
	if s.cachePolicy != nil && !s.cachePolicy(key, value) {
		return
	}

	entrySize := int64(len(key) + len(value) + minEntrySize)

	// Early rejection - don't try to cache entries that are too large
	// relative to the total cache size (> 10% of total memory)
	if entrySize > s.memCache.memoryLimit/10 {
		return
	}

	// Try to reserve global memory - evict entries if necessary
	// Do this BEFORE locking the shard to avoid deadlock
	if !s.memCache.reserveMemoryWithAllocation(entrySize, len(key), len(value), s) {
		// Track failures and implement exponential backoff
		failures := s.consecutiveFailures.Add(1)
		if failures >= 3 {
			// After 3 consecutive failures, enter backoff period
			// Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 32s
			backoffSeconds := int64(1 << min(int(failures-3), 5))
			s.backoffUntil.Store(time.Now().UnixMilli() + (backoffSeconds * 1000))
		}
		return // Cannot fit even after eviction
	}

	// Reset failure counter on success
	s.consecutiveFailures.Store(0)

	// Allocate memory for key and value in global arena
	s.memCache.globalMu.Lock()
	keyCopy := s.memCache.arena.allocate(len(key))
	valueCopy := s.memCache.arena.allocate(len(value))
	s.memCache.globalMu.Unlock()

	if keyCopy == nil || valueCopy == nil {
		// This should not happen since we checked in reserveMemoryWithAllocation
		// But if it does, release the reserved memory
		s.memCache.releaseMemory(entrySize)
		return
	}
	copy(keyCopy, key)
	copy(valueCopy, value)

	// Now lock the shard to insert the entry
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if entry already exists (it might have been added while we were allocating)
	if existing, found := s.entries[keyHash]; found {
		s.removeEntry(existing)
	}

	// Get entry from pool
	entry := s.entryPool.Get().(*cacheEntry)
	entry.keyHash = keyHash
	entry.key = keyCopy
	entry.value = valueCopy
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
func (s *cacheShard) delete(keyHash uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, found := s.entries[keyHash]; found {
		s.removeEntry(entry)
		// Track explicit delete eviction
		s.memCache.deleteEvictions.Add(1)
	}
}

// has checks if an entry exists in the shard
func (s *cacheShard) has(keyHash uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, found := s.entries[keyHash]
	return found
}

// getStats returns memory usage statistics for this shard
func (s *cacheShard) getStats() (entries int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.entries)
}

// clear removes all entries from the shard
func (s *cacheShard) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Release memory for all entries
	for _, entry := range s.entries {
		s.memCache.releaseMemory(entry.size)
	}

	s.entries = make(map[uint64]*cacheEntry)
	s.lruList = newLRUList()
	s.accessCounter = 0
}

// close cleans up the shard's resources
func (s *cacheShard) close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.entries = nil
	s.lruList = nil
}

// evict removes one entry based on the eviction policy
func (s *cacheShard) evict() {
	if s.policy == EvictionLRU {
		s.evictLRU()
	} else {
		s.evictLFU()
	}
}

// evictLRU removes the least recently used entry
// This must be called when the shard lock is held
func (s *cacheShard) evictLRU() {
	node := s.lruList.back()
	if node == nil {
		return
	}

	entry := node.entry
	s.removeEntry(entry)
	// Track forced eviction due to memory pressure
	s.memCache.forcedEvictions.Add(1)
}

// evictLFU removes the least frequently used entry
// This must be called when the shard lock is held
func (s *cacheShard) evictLFU() {
	var leastFrequent *cacheEntry
	var minFreq uint64 = ^uint64(0)

	for _, entry := range s.entries {
		if entry.frequency < minFreq {
			minFreq = entry.frequency
			leastFrequent = entry
		}
	}

	if leastFrequent != nil {
		s.removeEntry(leastFrequent)
		// Track forced eviction due to memory pressure
		s.memCache.forcedEvictions.Add(1)
	}
}

// removeEntry removes an entry from the shard
// This must be called when the shard lock is held
func (s *cacheShard) removeEntry(entry *cacheEntry) {
	delete(s.entries, entry.keyHash)
	if entry.lruNode != nil {
		s.lruList.remove(entry.lruNode)
	}

	// Release global memory
	s.memCache.releaseMemory(entry.size)

	// Clear and return to pool
	entry.key = nil
	entry.value = nil
	entry.lruNode = nil
	s.entryPool.Put(entry)
}

// newMemoryArena creates a new memory arena
func newMemoryArena(mc *MemCache) *memoryArena {
	return &memoryArena{
		slabs:     make([]*slab, 0, 16),
		slabSize:  slabSizeTiers[0], // Start with smallest tier
		tierIndex: 0,
		memCache:  mc,
	}
}

// newMemoryArenaWithSize creates a new memory arena with a specific initial slab size
func newMemoryArenaWithSize(size int64, mc *MemCache) *memoryArena {
	// Use the provided size, but ensure it's at least 1KB
	slabSize := size
	if slabSize < 1024 {
		slabSize = 1024
	}
	// Cap at default slab size to avoid excessive allocations
	if slabSize > defaultSlabSize {
		slabSize = defaultSlabSize
	}

	return &memoryArena{
		slabs:    make([]*slab, 0, 16),
		slabSize: slabSize,
		memCache: mc,
	}
}

// nextPowerOfTwo returns the next power of 2 greater than or equal to n
func nextPowerOfTwo(n int64) int64 {
	if n <= 0 {
		return 1
	}
	// If already a power of 2, return it
	if n&(n-1) == 0 {
		return n
	}
	// Find the next power of 2
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// allocate allocates a slice of the given size from the arena
func (a *memoryArena) allocate(size int) []byte {
	if size <= 0 {
		return nil
	}

	// Find a slab with enough space or create a new one
	for _, s := range a.slabs {
		if int64(len(s.data))-s.offset >= int64(size) {
			start := s.offset
			s.offset += int64(size)
			return s.data[start : start+int64(size) : start+int64(size)]
		}
	}

	// Need a new slab - determine size based on tiering strategy
	slabSize := a.slabSize

	// If the requested size is larger than current tier, upgrade to appropriate tier
	if int64(size) > slabSize {
		// Find the smallest tier that fits, or use next power of 2 if larger than max tier
		upgraded := false
		for i := a.tierIndex + 1; i < len(slabSizeTiers); i++ {
			if int64(size) <= slabSizeTiers[i] {
				a.tierIndex = i
				a.slabSize = slabSizeTiers[i]
				slabSize = a.slabSize
				upgraded = true
				break
			}
		}
		// If size exceeds max tier, use next power of 2
		if !upgraded {
			slabSize = nextPowerOfTwo(int64(size))
		}
	}

	// Check if allocating this slab would exceed the memory limit
	if a.memCache != nil {
		if a.allocated+slabSize > a.memCache.memoryLimit {
			// Would exceed limit - try to use a smaller slab that fits
			remainingSpace := a.memCache.memoryLimit - a.allocated
			if remainingSpace < int64(size) {
				// Not even enough space for the requested size
				return nil
			}
			// For custom slabs, use next power of 2 that fits within remaining space
			// but ensure it's at least big enough for the requested size
			slabSize = nextPowerOfTwo(int64(size))
			if slabSize > remainingSpace {
				// Power of 2 is too large, use the remaining space
				slabSize = remainingSpace
			}
		}
	}

	newSlab := &slab{
		data:   make([]byte, slabSize),
		offset: int64(size),
	}
	a.slabs = append(a.slabs, newSlab)
	a.allocated += slabSize

	return newSlab.data[0:size:size]
}

// Get retrieves a value from the cache, reading through to backing store if needed
func (mc *MemCache) Get(key []byte) ([]byte, error) {
	if mc.closed.Load() {
		return nil, ErrCacheClosed
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
		return nil, err
	}

	// Try to cache the result if policy allows
	shard.tryCache(keyHash, key, value)

	return value, nil
}

// Set writes a value to the cache
func (mc *MemCache) Set(key []byte, value []byte) error {
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
func (mc *MemCache) Delete(key []byte) error {
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
func (mc *MemCache) Has(key []byte) bool {
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
func (mc *MemCache) Stats() Stats {
	backingStats := mc.backing.Stats()

	// Add memory cache stats
	var memKeys int64
	for _, shard := range mc.shards {
		entries := shard.getStats()
		memKeys += int64(entries)
	}

	memSize := mc.memoryUsed.Load()

	// Keep the backing cache's key count (authoritative source)
	// memKeys represents only what's cached in memory, not total keys
	backingStats.IndexSize = memSize

	return backingStats
}

// Scan iterates through all keys in the backing cache
func (mc *MemCache) Scan(prefix []byte, fn func(key []byte) bool) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}
	return mc.backing.Scan(prefix, fn)
}

// MemStats returns memory-specific statistics
func (mc *MemCache) MemStats() MemStats {
	var stats MemStats
	for _, shard := range mc.shards {
		entries := shard.getStats()
		stats.Entries += int64(entries)
	}

	mc.globalMu.RLock()
	stats.MemoryUsed = mc.memoryUsed.Load()
	stats.MemoryAllocated = mc.arena.allocated

	// Collect slab breakdown
	stats.SlabBreakdown = make(map[int64]int)
	for _, slab := range mc.arena.slabs {
		slabSize := int64(len(slab.data))
		stats.SlabBreakdown[slabSize]++
	}
	mc.globalMu.RUnlock()

	// Calculate global fragmentation
	if stats.MemoryAllocated > 0 {
		wasted := stats.MemoryAllocated - stats.MemoryUsed
		stats.Fragmentation = float64(wasted) / float64(stats.MemoryAllocated)
	}

	stats.MemoryLimit = mc.config.MaxMemoryBytes
	stats.Shards = int64(len(mc.shards))

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

	// Calculate eviction rate (evictions per second)
	elapsed := time.Since(mc.statsStartTime).Seconds()
	if elapsed > 0 {
		stats.EvictionRate = float64(stats.ForcedEvictions) / elapsed
	}

	// Calculate average item size
	if stats.Entries > 0 {
		stats.AvgItemSize = stats.MemoryUsed / stats.Entries
	}

	return stats
}

// ResetStats resets the performance counters for a new monitoring window
func (mc *MemCache) ResetStats() {
	mc.hits.Store(0)
	mc.misses.Store(0)
	mc.forcedEvictions.Store(0)
	mc.deleteEvictions.Store(0)
	mc.statsStartTime = time.Now()
}

// MemStats provides memory cache statistics
type MemStats struct {
	Entries         int64
	MemoryUsed      int64
	MemoryAllocated int64
	MemoryLimit     int64
	Shards          int64
	Fragmentation   float64       // Ratio of wasted space (0.0-1.0)
	SlabBreakdown   map[int64]int // Map of slab size -> count

	// Cache performance metrics
	Hits            int64   // Cache hits
	Misses          int64   // Cache misses
	ForcedEvictions int64   // Evictions due to memory pressure
	DeleteEvictions int64   // Explicit Delete() calls
	HitRate         float64 // Hits / (Hits + Misses), 0.0-1.0
	Utilization     float64 // MemoryUsed / MemoryLimit, 0.0-1.0
	EvictionRate    float64 // Forced evictions per second
	AvgItemSize     int64   // Average item size in bytes
}

// reserveMemory attempts to reserve memory for a new entry
// It will evict entries across all shards if necessary
// Returns true if memory was successfully reserved
func (mc *MemCache) reserveMemory(size int64, requestingShard *cacheShard) bool {
	// Try to make room by evicting from any shard
	for {
		mc.globalMu.Lock()
		needToEvict := mc.memoryUsed.Load()+size > mc.memoryLimit
		if !needToEvict {
			// We have enough space, reserve it
			mc.memoryUsed.Add(size)
			mc.globalMu.Unlock()
			return true
		}
		mc.globalMu.Unlock()

		// Need to evict - do this without holding global lock to avoid deadlock
		evicted := false

		// First try to evict from the requesting shard
		if requestingShard != nil {
			requestingShard.mu.Lock()
			if len(requestingShard.entries) > 0 {
				requestingShard.evict()
				evicted = true
			}
			requestingShard.mu.Unlock()
		}

		// If that didn't work, try other shards
		if !evicted {
			for _, shard := range mc.shards {
				if shard == requestingShard {
					continue
				}
				shard.mu.Lock()
				if len(shard.entries) > 0 {
					shard.evict()
					evicted = true
					shard.mu.Unlock()
					break
				}
				shard.mu.Unlock()
			}
		}

		// If we couldn't evict anything, we can't make room
		if !evicted {
			return false
		}
	}
}

// reserveMemoryWithAllocation attempts to reserve memory for a new entry,
// taking into account both logical memory usage and physical arena allocation.
// It ensures that the arena will be able to allocate the required slabs.
// Returns true if memory was successfully reserved.
func (mc *MemCache) reserveMemoryWithAllocation(size int64, keySize int, valueSize int, requestingShard *cacheShard) bool {
	const (
		maxEvictionAttempts   = 10   // Reduced from 100 to limit churn
		minEvictionInterval   = 100  // milliseconds between evictions
		minCompactionInterval = 1000 // milliseconds between compactions (1 second)
	)

	compactionAttempted := false
	evictionAttempts := 0
	nowMs := time.Now().UnixMilli()

	// Try to make room by evicting from any shard
	for evictionAttempts < maxEvictionAttempts {
		mc.globalMu.Lock()

		// Calculate the worst-case slab overhead
		worstCaseOverhead := estimateSlabOverhead(keySize, valueSize, mc.arena)

		// Check if we have enough memory budget for both logical usage and arena allocation
		// We need to ensure: memoryUsed + size <= memoryLimit
		// AND: arena.allocated + worstCaseOverhead <= memoryLimit
		needToEvictLogical := mc.memoryUsed.Load()+size > mc.memoryLimit
		needToEvictArena := mc.arena.allocated+worstCaseOverhead > mc.memoryLimit

		if !needToEvictLogical && !needToEvictArena {
			// We have enough space, reserve it
			mc.memoryUsed.Add(size)
			mc.globalMu.Unlock()
			mc.updateMemoryPressure()
			return true
		}

		// Check if we recently evicted - if so, give up instead of churning
		lastEvict := mc.lastEviction.Load()
		if nowMs-lastEvict < minEvictionInterval {
			mc.globalMu.Unlock()
			return false
		}

		// If arena is the problem and we haven't tried compaction yet, try it
		// This handles the case where we have logical memory available but arena is fragmented
		if needToEvictArena && !compactionAttempted {
			lastCompact := mc.lastCompaction.Load()
			if nowMs-lastCompact > minCompactionInterval {
				mc.globalMu.Unlock()
				mc.lastCompaction.Store(nowMs)
				mc.compactGlobal()
				compactionAttempted = true
				continue
			}
		}

		mc.globalMu.Unlock()
		evictionAttempts++

		// Need to evict - do this without holding global lock to avoid deadlock
		evicted := false

		// First try to evict from the requesting shard
		if requestingShard != nil {
			requestingShard.mu.Lock()
			if len(requestingShard.entries) > 0 {
				requestingShard.evict()
				evicted = true
			}
			requestingShard.mu.Unlock()
		}

		// If that didn't work, try other shards
		if !evicted {
			for _, shard := range mc.shards {
				if shard == requestingShard {
					continue
				}
				shard.mu.Lock()
				if len(shard.entries) > 0 {
					shard.evict()
					evicted = true
					shard.mu.Unlock()
					break
				}
				shard.mu.Unlock()
			}
		}

		// If we couldn't evict anything, we can't make room
		if !evicted {
			return false
		}

		// Update last eviction time
		mc.lastEviction.Store(time.Now().UnixMilli())

		// After evicting, if the problem was arena space (not logical memory),
		// we need to compact to actually reclaim the arena space
		if needToEvictArena && !compactionAttempted {
			lastCompact := mc.lastCompaction.Load()
			nowMs = time.Now().UnixMilli()
			if nowMs-lastCompact > minCompactionInterval {
				mc.lastCompaction.Store(nowMs)
				mc.compactGlobal()
				compactionAttempted = true
			}
		}
	}

	return false
}

// estimateSlabOverhead estimates the worst-case slab allocation overhead
// for allocating a key and value of the given sizes
func estimateSlabOverhead(keySize int, valueSize int, arena *memoryArena) int64 {
	// Check if key fits in existing slabs
	keyFits := false
	valueFits := false
	bothFitInSameSlab := false

	for _, s := range arena.slabs {
		availableSpace := int64(len(s.data)) - s.offset
		if availableSpace >= int64(keySize) {
			keyFits = true
		}
		if availableSpace >= int64(valueSize) {
			valueFits = true
		}
		// Check if both can fit in the same slab
		if availableSpace >= int64(keySize)+int64(valueSize) {
			bothFitInSameSlab = true
		}
	}

	// If both fit in the same existing slab, no overhead
	if bothFitInSameSlab {
		return 0
	}

	// If both fit in existing slabs (but not the same one), no overhead
	if keyFits && valueFits {
		return 0
	}

	// Calculate slab size needed if we need a new slab
	totalSize := int64(keySize + valueSize)

	// If key doesn't fit but value does, we only need a slab for the key
	if !keyFits && valueFits {
		keySlabSize := arena.slabSize
		if int64(keySize) > keySlabSize {
			found := false
			for i := arena.tierIndex + 1; i < len(slabSizeTiers); i++ {
				if int64(keySize) <= slabSizeTiers[i] {
					keySlabSize = slabSizeTiers[i]
					found = true
					break
				}
			}
			if !found {
				keySlabSize = nextPowerOfTwo(int64(keySize))
			}
		}
		// Cap at remaining memory capacity
		if arena.memCache != nil {
			remainingCapacity := arena.memCache.memoryLimit - arena.allocated
			if keySlabSize > remainingCapacity {
				if int64(keySize) > remainingCapacity {
					return keySlabSize // Return full size, caller will handle eviction
				}
				keySlabSize = remainingCapacity
			}
		}
		return keySlabSize
	}

	// If value doesn't fit but key does, we only need a slab for the value
	if keyFits && !valueFits {
		valueSlabSize := arena.slabSize
		if int64(valueSize) > valueSlabSize {
			found := false
			for i := arena.tierIndex + 1; i < len(slabSizeTiers); i++ {
				if int64(valueSize) <= slabSizeTiers[i] {
					valueSlabSize = slabSizeTiers[i]
					found = true
					break
				}
			}
			if !found {
				valueSlabSize = nextPowerOfTwo(int64(valueSize))
			}
		}
		// Cap at remaining memory capacity
		if arena.memCache != nil {
			remainingCapacity := arena.memCache.memoryLimit - arena.allocated
			if valueSlabSize > remainingCapacity {
				if int64(valueSize) > remainingCapacity {
					return valueSlabSize // Return full size, caller will handle eviction
				}
				valueSlabSize = remainingCapacity
			}
		}
		return valueSlabSize
	}

	// Neither fits - check if both can fit in a single new slab
	slabSize := arena.slabSize
	if totalSize > slabSize {
		// Need a larger slab
		found := false
		for i := arena.tierIndex + 1; i < len(slabSizeTiers); i++ {
			if totalSize <= slabSizeTiers[i] {
				slabSize = slabSizeTiers[i]
				found = true
				break
			}
		}
		if !found {
			slabSize = nextPowerOfTwo(totalSize)
		}
	}

	// Cap slab size at available memory capacity
	if arena.memCache != nil {
		remainingCapacity := arena.memCache.memoryLimit - arena.allocated
		if slabSize > remainingCapacity {
			// If we can't even fit the data in remaining space, return the full slab size
			// (caller will handle eviction)
			if totalSize > remainingCapacity {
				return slabSize
			}
			// Otherwise cap at remaining capacity
			slabSize = remainingCapacity
		}
	}

	// If both fit in one slab, return that slab size
	if totalSize <= slabSize {
		return slabSize
	}

	// Otherwise, we need separate slabs
	keySlabSize := arena.slabSize
	if int64(keySize) > keySlabSize {
		found := false
		for i := arena.tierIndex + 1; i < len(slabSizeTiers); i++ {
			if int64(keySize) <= slabSizeTiers[i] {
				keySlabSize = slabSizeTiers[i]
				found = true
				break
			}
		}
		if !found {
			keySlabSize = nextPowerOfTwo(int64(keySize))
		}
	}

	valueSlabSize := arena.slabSize
	if int64(valueSize) > valueSlabSize {
		found := false
		for i := arena.tierIndex + 1; i < len(slabSizeTiers); i++ {
			if int64(valueSize) <= slabSizeTiers[i] {
				valueSlabSize = slabSizeTiers[i]
				found = true
				break
			}
		}
		if !found {
			valueSlabSize = nextPowerOfTwo(int64(valueSize))
		}
	}

	return keySlabSize + valueSlabSize
}

// releaseMemory releases memory when an entry is removed
func (mc *MemCache) releaseMemory(size int64) {
	mc.memoryUsed.Add(-size)
	mc.updateMemoryPressure()
}

// updateMemoryPressure updates the memory pressure metric (0-100)
func (mc *MemCache) updateMemoryPressure() {
	pressure := int32(0)
	if mc.memoryLimit > 0 {
		pressure = int32((mc.memoryUsed.Load() * 100) / mc.memoryLimit)
		if pressure > 100 {
			pressure = 100
		}
	}
	mc.memoryPressure.Store(pressure)
}

// getShard returns the shard for a given key hash
func (mc *MemCache) getShard(keyHash uint64) *cacheShard {
	return mc.shards[keyHash%uint64(len(mc.shards))]
}

// hashKey computes a hash for a key
func hashKey(key []byte) uint64 {
	// FNV-1a hash
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, b := range key {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

// Clear removes all entries from the memory cache
func (mc *MemCache) Clear() error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}

	for _, shard := range mc.shards {
		shard.clear()
	}

	return nil
}

// Close closes the memory cache and the backing cache
func (mc *MemCache) Close() error {
	if mc.closed.Swap(true) {
		return ErrCacheClosed
	}

	// Stop compaction worker
	close(mc.compactionDone)

	// Clear memory cache
	for _, shard := range mc.shards {
		shard.close()
	}

	// Close backing cache if it has a Close method
	if closer, ok := mc.backing.(interface{ Close() error }); ok {
		return closer.Close()
	}

	return nil
}

// Invalidate removes a key from the memory cache without touching backing store
func (mc *MemCache) Invalidate(key []byte) {
	if mc.closed.Load() {
		return
	}

	keyHash := hashKey(key)
	shard := mc.getShard(keyHash)
	shard.delete(keyHash)
}

// Warmup pre-loads keys from backing cache into memory
func (mc *MemCache) Warmup(keys [][]byte) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}

	for _, key := range keys {
		value, err := mc.backing.Get(key)
		if err != nil {
			continue // Skip keys that don't exist
		}

		keyHash := hashKey(key)
		shard := mc.getShard(keyHash)
		shard.tryCache(keyHash, key, value)
	}

	return nil
}

// startCompactionWorker starts a background goroutine that periodically checks
// for fragmentation and compacts shards when necessary
func (mc *MemCache) startCompactionWorker() {
	go func() {
		ticker := time.NewTicker(time.Duration(mc.config.CompactionInterval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-mc.compactionDone:
				return
			case <-ticker.C:
				mc.checkAndCompact()
			}
		}
	}()
}

// checkAndCompact checks for global fragmentation and compacts if needed
func (mc *MemCache) checkAndCompact() {
	if mc.closed.Load() {
		return
	}

	mc.globalMu.RLock()
	if mc.arena.allocated == 0 {
		mc.globalMu.RUnlock()
		return
	}

	// Calculate global fragmentation
	wasted := mc.arena.allocated - mc.memoryUsed.Load()
	fragmentation := float64(wasted) / float64(mc.arena.allocated)

	// Calculate memory pressure
	pressure := float64(mc.memoryUsed.Load()) / float64(mc.memoryLimit)
	mc.globalMu.RUnlock()

	// Adaptive threshold based on memory pressure
	// When memory is tight (>90% used), only compact if fragmentation is severe (>60%)
	// to avoid unnecessary work. When memory is comfortable (<70% used), compact at
	// normal threshold to keep things clean.
	threshold := mc.config.CompactionThreshold
	if pressure > 0.9 {
		threshold = 0.6 // Only compact if fragmentation is severe
	} else if pressure > 0.8 {
		threshold = 0.5 // Moderately higher threshold
	}

	// If fragmentation exceeds threshold, perform global compaction
	if fragmentation >= threshold {
		mc.compactGlobal()
	}
}

// compactGlobal rebuilds the global arena to eliminate fragmentation
func (mc *MemCache) compactGlobal() {
	if mc.closed.Load() {
		return
	}

	mc.globalMu.Lock()
	defer mc.globalMu.Unlock()

	memUsed := mc.memoryUsed.Load()
	if memUsed == 0 {
		return
	}

	// Create a new arena with appropriately sized slab
	// Add 20% buffer to reduce the chance of needing multiple slabs
	totalNeeded := memUsed + (memUsed / 5)
	newArena := newMemoryArenaWithSize(totalNeeded, mc)

	// Re-allocate all entries across all shards in the new arena
	for _, shard := range mc.shards {
		shard.mu.Lock()
		for _, entry := range shard.entries {
			// Allocate new key
			newKey := newArena.allocate(len(entry.key))
			if newKey == nil {
				continue // Skip if allocation fails
			}
			copy(newKey, entry.key)

			// Allocate new value
			newValue := newArena.allocate(len(entry.value))
			if newValue == nil {
				continue // Skip if allocation fails
			}
			copy(newValue, entry.value)

			// Update entry pointers
			entry.key = newKey
			entry.value = newValue
		}
		shard.mu.Unlock()
	}

	// Replace old arena with new one
	mc.arena = newArena
}

// Compact manually triggers compaction on all shards that exceed the threshold
func (mc *MemCache) Compact() {
	if mc.closed.Load() {
		return
	}

	mc.checkAndCompact()
}

// CompactN compacts up to N shards that exceed the fragmentation threshold.
// If count is 0 or negative, all eligible shards are compacted.
// Shards are prioritized by fragmentation ratio (highest first).
func (mc *MemCache) CompactN(count int) error {
	if mc.closed.Load() {
		return ErrCacheClosed
	}

	// If count is 0 or negative, compact all eligible shards
	if count <= 0 {
		mc.checkAndCompact()
		return nil
	}

	// Calculate fragmentation for each shard
	type shardFragmentation struct {
		index         int
		fragmentation float64
		allocated     int64
		used          int64
	}

	shardFrags := make([]shardFragmentation, 0, len(mc.shards))
	threshold := mc.config.CompactionThreshold

	for i, shard := range mc.shards {
		shard.mu.RLock()
		var used int64
		for _, entry := range shard.entries {
			used += entry.size
		}
		shard.mu.RUnlock()

		// Get allocated memory for this shard (estimated)
		mc.globalMu.RLock()
		totalAllocated := mc.arena.allocated
		totalUsed := mc.memoryUsed.Load()
		mc.globalMu.RUnlock()

		// Estimate shard's portion of allocated memory
		var allocated int64
		if totalUsed > 0 {
			allocated = (used * totalAllocated) / totalUsed
		} else {
			allocated = 0
		}

		// Calculate fragmentation for this shard
		var fragmentation float64
		if allocated > 0 {
			wasted := allocated - used
			fragmentation = float64(wasted) / float64(allocated)
		}

		// Only consider shards that exceed threshold
		if fragmentation >= threshold {
			shardFrags = append(shardFrags, shardFragmentation{
				index:         i,
				fragmentation: fragmentation,
				allocated:     allocated,
				used:          used,
			})
		}
	}

	// If no shards need compaction, return early
	if len(shardFrags) == 0 {
		return nil
	}

	// Sort by fragmentation (highest first)
	sort.Slice(shardFrags, func(i, j int) bool {
		return shardFrags[i].fragmentation > shardFrags[j].fragmentation
	})

	// Limit to N shards
	if count < len(shardFrags) {
		shardFrags = shardFrags[:count]
	}

	// Perform global compaction if we have eligible shards
	// Since MemCache uses a global arena, we compact globally
	mc.compactGlobal()

	return nil
}
