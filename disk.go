package bitcache

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edsrzf/mmap-go"
)

// cachedFile represents a memory-mapped cached file
type cachedFile struct {
	file  *os.File
	mmap  mmap.MMap
	mutex sync.RWMutex
}

// DiskCache provides a simple embedded key/value store inspired by the Bitcask design.
//
// Design highlights:
//   - Append-only log segments: All mutations (Set) are written sequentially
//     to the active segment file. When the file reaches a size threshold it is
//     rotated and a new active segment is created.
//   - Write-once semantics: Keys can only be written once. Subsequent Set calls
//     for existing keys are no-ops. This simplifies the cache significantly.
//   - Segment-level deletion: Instead of individual key deletes, entire segments
//     age out when they haven't been accessed for a configurable duration. Use
//     PruneSegments() to remove old, unaccessed segments.
//   - Access tracking: Each segment tracks access count and last access time
//     (updated on both reads and writes) to support intelligent aging.
//   - In-memory key directory: The complete key space lives in memory for O(1)
//     lookups. We store it in a partitioned map structure organized by segment ID
//     which provides efficient lookups with reduced lock contention and O(1) segment
//     eviction. Since this is a write-once cache, keys are never updated.
//   - Crash recovery with hint data: Historical segment files can contain an
//     embedded hint section (an index of key -> file offset metadata) appended
//     at rotation time. On startup we first try to rebuild the key directory
//     from these hints; if unavailable we fall back to scanning the segment.
//   - Compaction: Old segment files are periodically compacted. Only the latest
//     version of each key is rewritten into the active log; obsolete entries
//     are discarded, reclaiming disk space.
//   - Data integrity: Each log record stores a CRC32 checksum (IEEE polynomial)
//     over its metadata and payload allowing detection of partial/corrupt writes.
//   - Concurrency: A coarse RW mutex protects writers & structural changes; the
//     btree pointer is updated atomically for mostly lock-free reads. File
//     handles for historical segments are cached with per-file RW locks to avoid
//     repeated open/close costs under read load.
//
// The structure implements the Cache interface (see cache.go) and is intended
// for workloads where the full key index fits in memory and fast point lookups
// are required, while values are stored on disk.
type DiskCache[V any] struct {
	mu              sync.RWMutex
	dir             string
	activeFile      *os.File
	activeWriter    *bufio.Writer
	activeFileID    uint32
	activeOffset    int64
	keydir          *partitionedKeyDir
	stats           Stats
	closed          atomic.Bool
	compactionMutex sync.Mutex
	lastCompaction  time.Time
	// File caching for efficient reads
	fileCache      map[uint32]*cachedFile
	fileCacheMutex sync.RWMutex
	// In-memory segment tracking for access-based pruning
	segments      map[segmentID]*segmentInfo
	segmentsMutex sync.RWMutex
	// Auto-compaction
	autoCompactEnabled  bool
	autoCompactInterval time.Duration
	autoCompactMaxAge   time.Duration
	autoCompactDone     chan struct{}
	// Buffer pools for reducing allocations
	headerPool     *sync.Pool
	entryPool      *sync.Pool
	marshalBufPool *BufferPool // Pool for marshal operations
	// Configuration
	maxSegmentSize int64
	maxDiskUsage   int64
	minSegmentAge  time.Duration
	// Marshaling
	marshaler Marshaler[V]
}

// DiskCacheConfig holds configuration options for DiskCache
type DiskCacheConfig struct {
	// MaxSegmentSize is the maximum size of a segment file before rotation
	// If 0, defaults to 16MB
	MaxSegmentSize int64
	// MaxDiskUsage is the maximum total disk usage in bytes before compaction removes old segments
	// If 0, no limit is enforced
	MaxDiskUsage int64
	// MinSegmentAge is the minimum age before a segment can be removed during pruning
	// This protects new segments from being removed before they accumulate access stats
	// If 0, defaults to 1 hour
	MinSegmentAge time.Duration
	// AutoCompactEnabled enables automatic background compaction
	// If true, compaction runs periodically based on AutoCompactInterval
	AutoCompactEnabled bool
	// AutoCompactInterval is the time between automatic compaction checks
	// If 0, defaults to 5 minutes
	AutoCompactInterval time.Duration
	// AutoCompactMaxAge is the maximum age for segments during auto-compaction
	// If 0, only disk-usage-based compaction is performed (no age-based removal)
	// If > 0, segments older than this will be removed during auto-compaction
	AutoCompactMaxAge time.Duration
}

// keyEntry represents an entry in the in-memory key directory
type keyEntry struct {
	fileID    uint32
	offset    int64
	size      uint32
	timestamp uint32
}

// logEntry represents a record in the log file
type logEntry struct {
	crc       uint32
	timestamp uint32
	keySize   uint32
	valueSize uint32
	key       []byte
	value     []byte
}

// hintEntry represents an entry in a hint file
type hintEntry struct {
	timestamp uint32
	keySize   uint32
	valueSize uint32
	offset    int64
	key       []byte
}

// fileHeader represents the header at the beginning of each segment file
type fileHeader struct {
	hintOffset int64 // Offset to hint data within the file (0 if no hints written yet)
}

// CompactionResult contains information about a completed compaction
type CompactionResult struct {
	// Type of compaction performed ("access-based-pruning", "disk-usage-reduction", or "none")
	Type string
	// Level is deprecated (always 0 for write-once design)
	Level uint8
	// Input segments that were compacted/removed
	InputSegments []string
	// Output segment created (if any)
	OutputSegment string
	// Number of live entries written to output
	LiveEntries int
	// Total bytes written to output
	BytesWritten int64
	// Segments deleted after compaction
	DeletedSegments []string
}

// segmentID represents a segment file identifier
// In the write-once design, level is always 0 (no LSM-style levels)
type segmentID struct {
	generation uint32
	level      uint8 // Always 0 for write-once design
}

// String returns the filename for this segment
func (s segmentID) String() string {
	return fmt.Sprintf("%08d-%02d.log", s.generation, s.level)
}

// parseSegmentID parses a segment filename into generation and level
func parseSegmentID(filename string) (segmentID, error) {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]

	parts := strings.Split(name, "-")
	if len(parts) != 2 {
		// Try legacy format (backwards compatibility)
		if fileID, err := strconv.ParseUint(name, 10, 32); err == nil {
			// Convert legacy format to generation-level format
			return segmentID{generation: uint32(fileID), level: 0}, nil
		}
		return segmentID{}, fmt.Errorf("invalid segment filename format: %s", filename)
	}

	gen, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return segmentID{}, fmt.Errorf("invalid generation: %w", err)
	}

	level, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return segmentID{}, fmt.Errorf("invalid level: %w", err)
	}

	return segmentID{generation: uint32(gen), level: uint8(level)}, nil
}

// segmentInfo tracks metadata about a segment for access-based pruning
type segmentInfo struct {
	id           segmentID
	path         string
	totalBytes   int64
	accessCount  int64
	lastAccessed time.Time
	createdAt    time.Time
	mu           sync.RWMutex
}

// recordAccess increments the access count and updates last access time
func (s *segmentInfo) recordAccess() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accessCount++
	s.lastAccessed = time.Now()
}

// getAccessStats returns the access count and last access time
func (s *segmentInfo) getAccessStats() (count int64, lastAccess time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.accessCount, s.lastAccessed
}

const (
	// File header size: hintOffset(8) = 8 bytes
	fileHeaderSize = 8
	// Header size: crc(4) + timestamp(4) + keySize(4) + valueSize(4) = 16 bytes
	headerSize = 16
	// Hint entry size: timestamp(4) + keySize(4) + valueSize(4) + offset(8) = 20 bytes + key
	hintHeaderSize = 20
	// Maximum file size before creating a new segment (16MB for faster startup)
	maxFileSize = 16 * 1024 * 1024
	// Minimum time between compactions (1 hour)
	minCompactionInterval = time.Hour
)

// NewDiskCache creates a new bitcask cache in the specified directory with a marshaler
func NewDiskCache[V any](dir string, marshaler Marshaler[V]) (*DiskCache[V], error) {
	return NewDiskCacheWithConfig(dir, DiskCacheConfig{}, marshaler)
}

// NewDiskCacheWithConfig creates a new bitcask cache with custom configuration and marshaler
func NewDiskCacheWithConfig[V any](dir string, config DiskCacheConfig, marshaler Marshaler[V]) (*DiskCache[V], error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Set default segment size if not specified
	maxSegSize := config.MaxSegmentSize
	if maxSegSize <= 0 {
		maxSegSize = maxFileSize
	}

	// Set default min segment age if not specified
	minSegAge := config.MinSegmentAge
	if minSegAge <= 0 {
		minSegAge = 1 * time.Hour
	}

	// Set default auto-compaction interval if not specified
	autoCompactInterval := config.AutoCompactInterval
	if autoCompactInterval <= 0 {
		autoCompactInterval = 5 * time.Minute
	}

	cache := &DiskCache[V]{
		dir: dir,
		// Initialize the file cache
		fileCache:           make(map[uint32]*cachedFile),
		maxSegmentSize:      maxSegSize,
		maxDiskUsage:        config.MaxDiskUsage,
		minSegmentAge:       minSegAge,
		marshaler:           marshaler,
		segments:            make(map[segmentID]*segmentInfo),
		autoCompactEnabled:  config.AutoCompactEnabled,
		autoCompactInterval: autoCompactInterval,
		autoCompactMaxAge:   config.AutoCompactMaxAge,
		// Initialize buffer pools
		headerPool: &sync.Pool{
			New: func() interface{} {
				buf := make([]byte, headerSize)
				return &buf
			},
		},
		entryPool: &sync.Pool{
			New: func() interface{} {
				return &logEntry{}
			},
		},
		marshalBufPool: NewBufferPool(32 * 4096), // 4KB initial size for marshal buffers
	}

	// Initialize the keydir with a partitioned map (32 shards)
	cache.keydir = newPartitionedKeyDir(32)

	// Load existing data from disk
	if err := cache.loadFromDisk(); err != nil {
		return nil, fmt.Errorf("failed to load existing data: %w", err)
	}

	// Open current log file for appending
	if err := cache.openLogFile(); err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	// Start auto-compaction if enabled
	if cache.autoCompactEnabled {
		cache.autoCompactDone = make(chan struct{})
		go cache.autoCompactionLoop()
	}

	return cache, nil
}

// isClosed checks if the cache is closed (lock-free)
func (c *DiskCache[V]) isClosed() bool {
	return c.closed.Load()
}

// autoCompactionLoop runs in a separate goroutine and periodically calls Compact
func (c *DiskCache[V]) autoCompactionLoop() {
	ticker := time.NewTicker(c.autoCompactInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.autoCompactDone:
			return
		case <-ticker.C:
			// Skip if cache is closed
			if c.isClosed() {
				return
			}

			// Run compaction with configured maxAge
			result, err := c.Compact(c.autoCompactMaxAge)
			if err != nil {
				// Log error but continue
				fmt.Printf("Auto-compaction error: %v\n", err)
			} else if result != nil && result.Type != "none" {
				// Log successful compaction
				fmt.Printf("Auto-compaction completed: type=%s segments=%d bytes_freed=%d\n",
					result.Type, len(result.DeletedSegments), calculateBytesFreed(result))
			}
		}
	}
}

// calculateBytesFreed is a helper to calculate total bytes freed from a CompactionResult
func calculateBytesFreed(result *CompactionResult) int64 {
	// This is an approximation - we'd need to track bytes in the result
	// For now, just return 0 as a placeholder
	return 0
}

// Get retrieves the value for the given key
func (c *DiskCache[V]) Get(key []byte) (V, error) {
	var zero V
	// Check closed status with minimal locking
	if c.isClosed() {
		return zero, ErrCacheClosed
	}

	// Lock-free keydir read using atomic pointer
	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	if !exists {
		return zero, ErrKeyNotFound
	}

	// Check if we need to flush the active writer (only if reading from active file)
	// Acquire lock only if needed
	c.mu.RLock()
	needsFlush := false
	if entry.fileID == c.activeFileID && c.activeWriter != nil {
		entryEnd := entry.offset + int64(entry.size)
		bufferedBytes := int64(c.activeWriter.Buffered())

		// If the entry extends into the buffered region, we need to flush
		if entryEnd > c.activeOffset-bufferedBytes {
			needsFlush = true
		}
	}
	c.mu.RUnlock()

	// Upgrade to write lock if we need to flush
	if needsFlush {
		c.mu.Lock()
		// Double-check after acquiring write lock
		if c.activeWriter != nil {
			if err := c.activeWriter.Flush(); err != nil {
				c.mu.Unlock()
				return zero, fmt.Errorf("failed to flush active writer: %w", err)
			}
		}
		c.mu.Unlock()
	}

	// Final closed check before disk I/O
	if c.isClosed() {
		return zero, ErrCacheClosed
	}

	atomic.AddInt64(&c.stats.Reads, 1)

	// Track segment access
	c.trackSegmentAccess(entry.fileID)

	// Read the value from disk WITHOUT holding any cache-level locks
	// File caching has its own fine-grained locks
	data, err := c.readValueFromDisk(entry)
	if err != nil {
		return zero, fmt.Errorf("failed to read value: %w", err)
	}

	// Unmarshal the bytes to the target type
	var value V
	err = c.marshaler.Unmarshal(data, &value)
	if err != nil {
		return zero, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return value, nil
}

// GetInto retrieves the value for the given key into the provided target
// This allows the caller to reuse/pool V objects to avoid allocations
func (c *DiskCache[V]) GetInto(key []byte, target *V) error {
	// Check closed status with minimal locking
	if c.isClosed() {
		return ErrCacheClosed
	}

	// Lock-free keydir read using atomic pointer
	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	if !exists {
		return ErrKeyNotFound
	}

	// Check if we need to flush the active writer (only if reading from active file)
	c.mu.RLock()
	needsFlush := false
	if entry.fileID == c.activeFileID && c.activeWriter != nil {
		entryEnd := entry.offset + int64(entry.size)
		bufferedBytes := int64(c.activeWriter.Buffered())

		if entryEnd > c.activeOffset-bufferedBytes {
			needsFlush = true
		}
	}
	c.mu.RUnlock()

	// Upgrade to write lock if we need to flush
	if needsFlush {
		c.mu.Lock()
		if c.activeWriter != nil {
			if err := c.activeWriter.Flush(); err != nil {
				c.mu.Unlock()
				return fmt.Errorf("failed to flush active writer: %w", err)
			}
		}
		c.mu.Unlock()
	}

	// Final closed check before disk I/O
	if c.isClosed() {
		return ErrCacheClosed
	}

	atomic.AddInt64(&c.stats.Reads, 1)

	// Read the value from disk
	data, err := c.readValueFromDisk(entry)
	if err != nil {
		return fmt.Errorf("failed to read value: %w", err)
	}

	// Unmarshal directly into the caller's target
	return c.marshaler.Unmarshal(data, target)
}

// Set stores a key-value pair in the cache
// This is write-once only - if the key already exists, this is a no-op
func (c *DiskCache[V]) Set(key []byte, value V) error {
	// Quick closed check without lock
	if c.isClosed() {
		return ErrCacheClosed
	}

	// Lock-free keydir read to check if key already exists (write-once semantic)
	keyStr := string(key)
	_, exists := c.getKeyEntry(keyStr)
	if exists {
		// Key already exists - this is a no-op for write-once
		return nil
	}

	// Get a buffer from the pool for marshaling
	buf := c.marshalBufPool.Get()
	defer c.marshalBufPool.Put(buf)

	// Marshal the value to bytes using the pooled buffer
	data, err := c.marshaler.Marshal(value, buf)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	// Prepare entry and calculate CRC WITHOUT holding any lock
	entry := &logEntry{
		timestamp: uint32(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(data)),
		key:       key,
		value:     data,
	}
	entry.crc = c.calculateCRC(entry)

	// Now acquire write lock ONLY for file operations
	c.mu.Lock()

	// Double-check closed after acquiring lock
	if c.isClosed() {
		c.mu.Unlock()
		return ErrCacheClosed
	}

	// Write to log file (this is the critical section)
	offset, err := c.writeLogEntry(entry)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to write log entry: %w", err)
	}

	// Capture values we need before releasing lock
	fileID := c.activeFileID
	c.mu.Unlock()

	// Track segment access (write)
	c.trackSegmentAccess(fileID)

	// Update keydir WITHOUT holding c.mu (keydir updates are lock-free)
	c.setKeyEntry(keyStr, &keyEntry{
		fileID:    fileID,
		offset:    offset,
		size:      headerSize + entry.keySize + entry.valueSize,
		timestamp: entry.timestamp,
	})

	// Update stats (atomic operations)
	// Only count as new key if it didn't exist
	if !exists {
		atomic.AddInt64(&c.stats.Keys, 1)
	}
	atomic.AddInt64(&c.stats.Writes, 1)
	atomic.AddInt64(&c.stats.DataSize, int64(headerSize+entry.keySize+entry.valueSize))

	return nil
}

// BatchSet performs bulk inserts for maximum performance
func (c *DiskCache[V]) BatchSet(entries []struct {
	Key   []byte
	Value V
}) error {
	if c.isClosed() {
		return ErrCacheClosed
	}

	if len(entries) == 0 {
		return nil
	}

	// Prepare all entries and marshal values WITHOUT holding lock
	type preparedEntry struct {
		keyStr    string
		logEntry  *logEntry
		oldEntry  *keyEntry
		oldExists bool
	}

	prepared := make([]preparedEntry, 0, len(entries))
	timestamp := uint32(time.Now().Unix())

	// Get a buffer from the pool for marshaling (reused across all entries)
	buf := c.marshalBufPool.Get()
	defer c.marshalBufPool.Put(buf)

	for _, entry := range entries {
		// Marshal the value to bytes using the pooled buffer
		data, err := c.marshaler.Marshal(entry.Value, buf)
		if err != nil {
			return fmt.Errorf("failed to marshal value: %w", err)
		}

		// Make a copy of marshaled data since we're reusing the buffer
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		// Prepare log entry and calculate CRC
		logEnt := &logEntry{
			timestamp: timestamp,
			keySize:   uint32(len(entry.Key)),
			valueSize: uint32(len(dataCopy)),
			key:       entry.Key,
			value:     dataCopy,
		}
		logEnt.crc = c.calculateCRC(logEnt)

		// Check if key exists (lock-free)
		keyStr := string(entry.Key)
		oldEntry, oldExists := c.getKeyEntry(keyStr)

		prepared = append(prepared, preparedEntry{
			keyStr:    keyStr,
			logEntry:  logEnt,
			oldEntry:  oldEntry,
			oldExists: oldExists,
		})
	}

	// Now acquire write lock and write all entries
	c.mu.Lock()

	// Double-check closed after acquiring lock
	if c.isClosed() {
		c.mu.Unlock()
		return ErrCacheClosed
	}

	// Write all entries and collect their offsets
	type writeResult struct {
		keyStr   string
		fileID   uint32
		offset   int64
		size     uint32
		isNewKey bool
	}

	results := make([]writeResult, 0, len(prepared))

	for _, prep := range prepared {
		offset, err := c.writeLogEntry(prep.logEntry)
		if err != nil {
			c.mu.Unlock()
			return fmt.Errorf("failed to write log entry: %w", err)
		}

		isNewKey := !prep.oldExists

		results = append(results, writeResult{
			keyStr:   prep.keyStr,
			fileID:   c.activeFileID,
			offset:   offset,
			size:     headerSize + prep.logEntry.keySize + prep.logEntry.valueSize,
			isNewKey: isNewKey,
		})
	}

	c.mu.Unlock()

	// Update keydir for all entries WITHOUT holding c.mu
	var newKeys int64
	var totalSize int64

	for _, result := range results {
		c.setKeyEntry(result.keyStr, &keyEntry{
			fileID:    result.fileID,
			offset:    result.offset,
			size:      result.size,
			timestamp: timestamp,
		})

		if result.isNewKey {
			newKeys++
		}
		totalSize += int64(result.size)
	}

	// Update stats (atomic operations)
	atomic.AddInt64(&c.stats.Keys, newKeys)
	atomic.AddInt64(&c.stats.Writes, int64(len(entries)))
	atomic.AddInt64(&c.stats.DataSize, totalSize)

	return nil
}

// Has checks if a key exists in the cache
func (c *DiskCache[V]) Has(key []byte) bool {
	if c.isClosed() {
		return false
	}

	// Lock-free keydir read
	keyStr := string(key)
	_, exists := c.getKeyEntry(keyStr)
	return exists
}

// Close closes the cache and flushes any pending writes
func (c *DiskCache[V]) Close() error {
	// Set closed flag FIRST (before acquiring lock) to reject any new writes
	// This prevents race condition where a write can pass the closed check,
	// acquire the lock, and partially write data before Close() flushes
	if !c.closed.CompareAndSwap(false, true) {
		// Already closed
		return nil
	}

	// Stop auto-compaction goroutine if it's running (without holding mu)
	if c.autoCompactEnabled && c.autoCompactDone != nil {
		close(c.autoCompactDone)
	}

	// Now acquire lock to safely flush and close files
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.activeWriter != nil {
		if err := c.activeWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush writer: %w", err)
		}
	}

	if c.activeFile != nil {
		if err := c.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
		if err := c.activeFile.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
	}

	// Close all cached files and unmap them
	c.fileCacheMutex.Lock()
	for fileID, cached := range c.fileCache {
		cached.mutex.Lock()
		if cached.mmap != nil {
			cached.mmap.Unmap()
		}
		cached.file.Close()
		cached.mutex.Unlock()
		delete(c.fileCache, fileID)
	}
	c.fileCacheMutex.Unlock()

	return nil
}

// registerSegment adds a segment to the in-memory tracking
func (c *DiskCache[V]) registerSegment(id segmentID, path string, totalBytes int64) {
	c.segmentsMutex.Lock()
	defer c.segmentsMutex.Unlock()

	now := time.Now()
	c.segments[id] = &segmentInfo{
		id:           id,
		path:         path,
		totalBytes:   totalBytes,
		accessCount:  0,
		lastAccessed: now,
		createdAt:    now,
	}
}

// unregisterSegment removes a segment from in-memory tracking
func (c *DiskCache[V]) unregisterSegment(id segmentID) {
	c.segmentsMutex.Lock()
	defer c.segmentsMutex.Unlock()

	delete(c.segments, id)
}

// trackSegmentAccess records an access to a segment (read or write)
func (c *DiskCache[V]) trackSegmentAccess(fileID uint32) {
	c.segmentsMutex.RLock()
	defer c.segmentsMutex.RUnlock()

	// Find segment by generation (fileID is the generation)
	for id, info := range c.segments {
		if id.generation == fileID {
			info.recordAccess()
			return
		}
	}
}

// Sync forces a sync of any pending writes to disk
func (c *DiskCache[V]) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed() {
		return ErrCacheClosed
	}

	if c.activeWriter != nil {
		if err := c.activeWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush writer: %w", err)
		}
	}

	if c.activeFile != nil {
		if err := c.activeFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync file: %w", err)
		}
	}

	return nil
}

// Stats returns cache statistics
func (c *DiskCache[V]) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := c.stats
	// Calculate index size by getting the map length
	count := c.keydir.Len()
	stats.IndexSize = int64(count * 64) // Rough estimate

	// Count the number of segments from in-memory tracking
	c.segmentsMutex.RLock()
	stats.Segments = int64(len(c.segments))
	c.segmentsMutex.RUnlock()

	// Add 1 for the active file if it exists
	if c.activeFile != nil {
		stats.Segments++
	}

	return stats
}

// Compact removes segments based on configured age and disk usage constraints.
// The compaction strategy depends on the maxAge parameter and maxDiskUsage configuration:
// - If maxAge > 0 only: Removes segments older than maxAge (based on creation time)
// - If maxDiskUsage > 0 only: Removes least-accessed segments to get under disk limit
// - If both configured: Applies age filter first, then disk limit if needed
// - If neither: No-op
//
// Segments are prioritized for removal by:
// 1. Creation age threshold (if maxAge > 0) - segments older than maxAge are removed
// 2. Zero accesses (never accessed) - for disk-usage-based removal
// 3. Fewest accesses - for disk-usage-based removal
// 4. Oldest last access time - for disk-usage-based removal
//
// All segments must be at least minSegmentAge old to be eligible for removal.
//
// Returns CompactionResult with details about what was removed, or Type="none" if no compaction was needed.
func (c *DiskCache[V]) Compact(maxAge time.Duration) (*CompactionResult, error) {
	if c == nil {
		return &CompactionResult{Type: "none"}, nil
	}

	c.compactionMutex.Lock()
	defer c.compactionMutex.Unlock()

	if c.isClosed() {
		return nil, ErrCacheClosed
	}

	now := time.Now()
	hasAgeLimit := maxAge > 0
	hasDiskLimit := c.maxDiskUsage > 0

	// If no limits configured, nothing to do
	if !hasAgeLimit && !hasDiskLimit {
		return &CompactionResult{Type: "none"}, nil
	}

	// Collect eligible segments and calculate disk usage
	totalDiskUsage := int64(0)
	var candidateSegments []*segmentInfo

	c.segmentsMutex.RLock()
	activeID := c.activeFileID
	for id, info := range c.segments {
		// Skip active segment
		if id.generation == activeID {
			continue
		}

		totalDiskUsage += info.totalBytes

		// Check if segment is old enough to be eligible for removal (based on creation time)
		info.mu.RLock()
		segmentAge := now.Sub(info.createdAt)
		info.mu.RUnlock()

		if segmentAge >= c.minSegmentAge {
			candidateSegments = append(candidateSegments, info)
		}
	}
	c.segmentsMutex.RUnlock()

	if len(candidateSegments) == 0 {
		// No eligible segments (all too new)
		return &CompactionResult{Type: "none"}, nil
	}

	// Phase 1: Apply age-based filtering if configured
	var segmentsToRemove []*segmentInfo
	compactionType := ""

	if hasAgeLimit {
		for _, seg := range candidateSegments {
			seg.mu.RLock()
			segmentAge := now.Sub(seg.createdAt)
			seg.mu.RUnlock()

			if segmentAge >= maxAge {
				segmentsToRemove = append(segmentsToRemove, seg)
			}
		}
		compactionType = "age-based-pruning"
	}

	// Phase 2: Apply disk usage filtering if configured and needed
	if hasDiskLimit && totalDiskUsage > c.maxDiskUsage {
		targetReduction := totalDiskUsage - c.maxDiskUsage

		// If we already have segments from age filtering, calculate how much they'll free
		bytesFromAge := int64(0)
		if hasAgeLimit && len(segmentsToRemove) > 0 {
			for _, seg := range segmentsToRemove {
				bytesFromAge += seg.totalBytes
			}
		}

		// If age-based removal isn't enough, add more segments
		if bytesFromAge < targetReduction {
			// Start with candidates not already marked for removal
			remainingCandidates := make([]*segmentInfo, 0)
			markedForRemoval := make(map[segmentID]bool)
			for _, seg := range segmentsToRemove {
				markedForRemoval[seg.id] = true
			}
			for _, seg := range candidateSegments {
				if !markedForRemoval[seg.id] {
					remainingCandidates = append(remainingCandidates, seg)
				}
			}

			// Sort by access pattern for removal priority
			sort.Slice(remainingCandidates, func(i, j int) bool {
				countI, lastAccessI := remainingCandidates[i].getAccessStats()
				countJ, lastAccessJ := remainingCandidates[j].getAccessStats()

				// Zero accesses come first
				if countI == 0 && countJ > 0 {
					return true
				}
				if countJ == 0 && countI > 0 {
					return false
				}

				// If both have accesses, prefer fewer accesses
				if countI != countJ {
					return countI < countJ
				}

				// If same access count, prefer older last access
				return lastAccessI.Before(lastAccessJ)
			})

			// Add segments until we meet the target reduction
			bytesFreed := bytesFromAge
			for _, seg := range remainingCandidates {
				if bytesFreed >= targetReduction {
					break
				}
				segmentsToRemove = append(segmentsToRemove, seg)
				bytesFreed += seg.totalBytes
			}

			if hasAgeLimit {
				compactionType = "age-and-disk-based-pruning"
			} else {
				compactionType = "disk-usage-reduction"
			}
		}
	}

	// If nothing to remove, we're done
	if len(segmentsToRemove) == 0 {
		return &CompactionResult{Type: "none"}, nil
	}

	// Remove the segments
	segmentsRemoved := make([]string, 0)
	keysRemoved := int64(0)
	bytesFreed := int64(0)

	for _, seg := range segmentsToRemove {
		// Evict all keys for this segment using O(1) deletion
		keysRemoved += c.keydir.EvictSegment(seg.id.generation)

		// Remove from file cache if present
		c.removeCachedFile(seg.id.generation)

		// Delete the file
		if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: failed to remove segment %s: %v\n", seg.path, err)
			continue
		}

		// Unregister from tracking
		c.unregisterSegment(seg.id)

		bytesFreed += seg.totalBytes
		segmentsRemoved = append(segmentsRemoved, seg.path)
	}

	// Update stats
	if keysRemoved > 0 {
		atomic.AddInt64(&c.stats.Keys, -keysRemoved)
	}
	if bytesFreed > 0 {
		atomic.AddInt64(&c.stats.DataSize, -bytesFreed)
	}

	result := &CompactionResult{
		Type:            compactionType,
		InputSegments:   segmentsRemoved,
		DeletedSegments: segmentsRemoved,
		BytesWritten:    0, // We don't rewrite data, just remove
	}

	return result, nil
}

// loadFromDisk loads existing data files and rebuilds the keydir
func (c *DiskCache[V]) loadFromDisk() error {
	// Clean up any temporary files from incomplete compactions
	// These files are not recoverable and should be removed
	tmpFiles, err := filepath.Glob(filepath.Join(c.dir, "*.tmp"))
	if err == nil {
		for _, tmpFile := range tmpFiles {
			os.Remove(tmpFile) // Ignore errors, best effort cleanup
		}
	}

	// Try to load from segment files with embedded hints first
	if err := c.loadFromSegmentFiles(); err == nil {
		return nil
	}

	// Fallback to loading from log files by scanning (slower)
	files, err := filepath.Glob(filepath.Join(c.dir, "*.log"))
	if err != nil {
		return err
	}

	// Sort files by ID to load them in order
	sort.Strings(files)

	for _, filename := range files {
		if err := c.loadLogFile(filename); err != nil {
			return fmt.Errorf("failed to load log file %s: %w", filename, err)
		}
	}

	return nil
}

// loadFromSegmentFiles attempts to load keydir from segment files with embedded hints
func (c *DiskCache[V]) loadFromSegmentFiles() error {
	files, err := filepath.Glob(filepath.Join(c.dir, "*.log"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		return fmt.Errorf("no segment files found")
	}

	// Sort files by name
	sort.Strings(files)

	var maxFileID uint32

	for _, filename := range files {
		// Extract file ID from filename (supports both LSM and legacy formats)
		fileID, err := parseFileID(filename)
		if err != nil {
			continue // Skip invalid files
		}

		if err := c.loadSegmentFile(filename, fileID); err != nil {
			return fmt.Errorf("failed to load segment file %s: %w", filename, err)
		}

		// Track the maximum file ID
		if fileID > maxFileID {
			maxFileID = fileID
		}
	}

	// Set active file ID to the maximum file ID found (will reuse the last segment)
	c.activeFileID = maxFileID

	return nil
}

// loadSegmentFile loads keydir entries from a segment file with embedded hints
func (c *DiskCache[V]) loadSegmentFile(filename string, fileID uint32) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Get file size for segment tracking
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// Parse segment ID from filename
	segID, err := parseSegmentID(filename)
	if err != nil {
		// Fallback for legacy format
		segID = segmentID{generation: fileID, level: 0}
	}

	// Register this segment in memory
	c.registerSegment(segID, filename, stat.Size())

	// Read file header to get hint offset
	header, err := c.readFileHeader(file)
	if err != nil {
		// If header is corrupted, try to scan from the beginning
		fmt.Printf("Warning: corrupted header in segment %d, attempting scan\n", fileID)
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to seek after header error: %w", err)
		}
		// Try scanning from offset 0 (no header)
		file.Close() // Close before calling scanSegmentFile which may truncate
		if err := c.scanSegmentFile(filename, fileID); err != nil {
			return fmt.Errorf("failed to scan segment file with corrupted header: %w", err)
		}
		return nil
	}

	if header.hintOffset > 0 {
		// Hints are available, try to read them for keydir
		if err := c.loadHintsFromSegment(file, header.hintOffset, fileID); err != nil {
			// Hints failed, fall back to scanning the data section
			fmt.Printf("Warning: hints corrupted in segment %d, falling back to full scan\n", fileID)
			file.Close() // Close before calling scanSegmentFile which may truncate
			if err := c.scanSegmentFile(filename, fileID); err != nil {
				return fmt.Errorf("failed to scan segment file after hint failure: %w", err)
			}
		}
	} else {
		// No hints available, scan the file
		file.Close() // Close before calling scanSegmentFile which may truncate
		if err := c.scanSegmentFile(filename, fileID); err != nil {
			return fmt.Errorf("failed to scan segment file: %w", err)
		}
	}

	return nil
}

// readFileHeader reads the file header from the beginning of a segment file
func (c *DiskCache[V]) readFileHeader(file *os.File) (*fileHeader, error) {
	// Seek to beginning of file
	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}

	headerBytes := make([]byte, fileHeaderSize)
	n, err := file.Read(headerBytes)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// If file is too small or empty, assume it has no header (old format)
	if n < fileHeaderSize {
		return &fileHeader{hintOffset: 0}, nil
	}

	header := &fileHeader{
		hintOffset: int64(binary.LittleEndian.Uint64(headerBytes[0:8])),
	}

	// Validate hint offset is reasonable (not corrupted)
	// It should be 0 (no hints) or >= fileHeaderSize and < file size
	if header.hintOffset != 0 {
		stat, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat file for header validation: %w", err)
		}

		// Hint offset should be within file bounds and after the header
		if header.hintOffset < fileHeaderSize || header.hintOffset > stat.Size() {
			return nil, fmt.Errorf("invalid hint offset %d in header (file size: %d)", header.hintOffset, stat.Size())
		}
	}

	return header, nil
}

// writeFileHeader writes the file header to the beginning of a segment file
func (c *DiskCache[V]) writeFileHeader(file *os.File, header *fileHeader) error {
	// Seek to beginning of file
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}

	headerBytes := make([]byte, fileHeaderSize)
	binary.LittleEndian.PutUint64(headerBytes[0:8], uint64(header.hintOffset))

	_, err := file.Write(headerBytes)
	return err
}

// loadHintsFromSegment loads keydir entries from hints embedded in a segment file
func (c *DiskCache[V]) loadHintsFromSegment(file *os.File, hintOffset int64, fileID uint32) error {
	// Get file size to determine hints section size
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	fileSize := stat.Size()

	if hintOffset >= fileSize {
		return fmt.Errorf("hint offset %d beyond file size %d", hintOffset, fileSize)
	}

	// Calculate hints section size
	hintsSize := fileSize - hintOffset
	if hintsSize <= 0 {
		return nil // No hints to load
	}

	// Read entire hints section into memory as one contiguous byte slice
	hintsData := make([]byte, hintsSize)
	if _, err := file.ReadAt(hintsData, hintOffset); err != nil {
		return fmt.Errorf("failed to read hints section: %w", err)
	}

	var newKeys int64
	var totalSize int64
	hintsLoaded := 0

	// Parse hints from the contiguous byte slice
	offset := 0
	for offset < len(hintsData) {
		// Check if we have enough bytes for hint header
		if offset+hintHeaderSize > len(hintsData) {
			if hintsLoaded == 0 {
				// No hints loaded yet, hints section is likely corrupted from the start
				return fmt.Errorf("hints corrupted, falling back to scan: incomplete hint header")
			}
			// Partial hint at end, stop here
			fmt.Printf("Warning: hints partially corrupted in segment %d, loaded %d entries\n", fileID, hintsLoaded)
			break
		}

		// Parse hint header directly from the byte slice (no allocation)
		header := hintsData[offset : offset+hintHeaderSize]
		timestamp := binary.LittleEndian.Uint32(header[0:4])
		keySize := binary.LittleEndian.Uint32(header[4:8])
		valueSize := binary.LittleEndian.Uint32(header[8:12])
		entryOffset := int64(binary.LittleEndian.Uint64(header[12:20]))

		offset += hintHeaderSize

		// Check if we have enough bytes for the key
		if offset+int(keySize) > len(hintsData) {
			if hintsLoaded == 0 {
				return fmt.Errorf("hints corrupted, falling back to scan: incomplete key data")
			}
			fmt.Printf("Warning: hints partially corrupted in segment %d, loaded %d entries\n", fileID, hintsLoaded)
			break
		}

		// Get key as a subslice (no allocation - references original hintsData)
		keyBytes := hintsData[offset : offset+int(keySize)]
		offset += int(keySize)

		// Convert to string for map key (unavoidable allocation in Go)
		keyStr := string(keyBytes)
		_, existed := c.keydir.Get(keyStr)

		// Insert directly into the keydir
		c.keydir.Set(keyStr, &keyEntry{
			fileID:    fileID,
			offset:    entryOffset,
			size:      headerSize + keySize + valueSize,
			timestamp: timestamp,
		})

		// Track stats for updates after commit
		if !existed {
			newKeys++
		}
		totalSize += int64(valueSize + headerSize)
		hintsLoaded++
	}

	// Update stats after successful commit
	if hintsLoaded > 0 {
		atomic.AddInt64(&c.stats.Keys, newKeys)
		atomic.AddInt64(&c.stats.DataSize, totalSize)
	}

	return nil
}

// scanSegmentFile scans a segment file to build the keydir (fallback when no hints)
func (c *DiskCache[V]) scanSegmentFile(filename string, fileID uint32) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Start after the file header
	if _, err := file.Seek(fileHeaderSize, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	offset := int64(fileHeaderSize)
	skippedRecords := 0
	lastValidOffset := int64(fileHeaderSize) // Track last known good position
	shouldTruncate := false
	truncateOffset := int64(0)

	for {
		entry, entrySize, err := c.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Warning: failed to read log entry for segment %d at offset %d: %s\n", fileID, offset, err)
			// Immediately truncate at first corruption - don't try to recover more records
			skippedRecords++
			shouldTruncate = true
			truncateOffset = lastValidOffset
			fmt.Printf("Warning: corruption detected in segment %d, will truncate from offset %d\n", fileID, lastValidOffset)
			break
		}

		keyStr := string(entry.key)
		_, existed := c.getKeyEntry(keyStr)
		c.setKeyEntry(keyStr, &keyEntry{
			fileID:    fileID,
			offset:    offset,
			size:      uint32(entrySize),
			timestamp: entry.timestamp,
		})

		// Update stats based on the entry type
		// Only count as new key if it didn't exist
		if !existed {
			atomic.AddInt64(&c.stats.Keys, 1)
		}
		atomic.AddInt64(&c.stats.DataSize, int64(entrySize))

		// Release entry back to pool
		c.releaseLogEntry(entry)

		offset += int64(entrySize)
		lastValidOffset = offset // Update last valid position after successful read
	}

	if skippedRecords > 0 {
		fmt.Printf("Warning: skipped %d corrupted records in segment %d\n", skippedRecords, fileID)
	}

	// Close file before truncating
	file.Close()

	// Truncate file if corruption was detected and no more valid records found
	if shouldTruncate {
		// Remove file from cache first to ensure stale handles are cleared
		if err := c.removeCachedFile(fileID); err != nil {
			fmt.Printf("Warning: failed to remove cached file %d: %v\n", fileID, err)
		}

		originalSize, _ := os.Stat(filename)
		var originalBytes int64
		if originalSize != nil {
			originalBytes = originalSize.Size()
		}

		if err := os.Truncate(filename, truncateOffset); err != nil {
			fmt.Printf("Error: failed to truncate segment %d at offset %d: %v\n", fileID, truncateOffset, err)
			return fmt.Errorf("failed to truncate corrupted segment: %w", err)
		} else {
			removedBytes := originalBytes - truncateOffset
			fmt.Printf("Successfully truncated segment %d to offset %d (removed %d corrupted bytes)\n",
				fileID, truncateOffset, removedBytes)
		}
	}

	return nil
}

// openLogFile opens the current log file for appending
func (c *DiskCache[V]) openLogFile() error {
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return err
	}

	// Use generation-level format: %08d-%02d.log
	// Level 0 for all rotations (compaction will create higher levels)
	filename := filepath.Join(c.dir, fmt.Sprintf("%08d-00.log", c.activeFileID))

	// Check if file exists
	fileExists := false
	if stat, err := os.Stat(filename); err == nil && stat.Size() > 0 {
		fileExists = true
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	// If this is a new file, write the file header
	if !fileExists {
		header := &fileHeader{hintOffset: 0}
		if err := c.writeFileHeader(file, header); err != nil {
			file.Close()
			return fmt.Errorf("failed to write file header: %w", err)
		}
	}

	// Seek to end of file for appending
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	if _, err := file.Seek(0, 2); err != nil { // Seek to end
		file.Close()
		return err
	}

	c.activeFile = file
	c.activeWriter = bufio.NewWriter(file)
	c.activeOffset = stat.Size()

	return nil
}

// writeLogEntry writes a log entry to the current log file
func (c *DiskCache[V]) writeLogEntry(entry *logEntry) (int64, error) {
	// Check if we need to rotate to a new file
	entrySize := headerSize + int64(entry.keySize) + int64(entry.valueSize)
	if c.activeOffset+entrySize > c.maxSegmentSize {
		if err := c.rotateLogFile(); err != nil {
			return 0, err
		}
	}

	currentOffset := c.activeOffset

	// Write header
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], entry.crc)
	binary.LittleEndian.PutUint32(header[4:8], entry.timestamp)
	binary.LittleEndian.PutUint32(header[8:12], entry.keySize)
	binary.LittleEndian.PutUint32(header[12:16], entry.valueSize)

	if _, err := c.activeWriter.Write(header); err != nil {
		return 0, err
	}

	// Write key
	if _, err := c.activeWriter.Write(entry.key); err != nil {
		return 0, err
	}

	// Write value
	if entry.valueSize > 0 {
		if _, err := c.activeWriter.Write(entry.value); err != nil {
			return 0, err
		}
	}

	c.activeOffset += entrySize

	return currentOffset, nil
}

// rotateLogFile creates a new log file and writes hints to the old one
func (c *DiskCache[V]) rotateLogFile() error {
	// Before closing the current file, write hints to it
	if c.activeFile != nil && c.activeWriter != nil {
		oldFileID := c.activeFileID
		// Flush the writer first to ensure all data is written
		if err := c.activeWriter.Flush(); err != nil {
			return err
		}

		// Get current file size before writing hints (this will be the hint offset)
		stat, err := c.activeFile.Stat()
		if err != nil {
			return err
		}
		hintOffset := stat.Size()

		// Write hints using the active writer
		if err := c.writeHintsToActiveFile(oldFileID, hintOffset); err != nil {
			// Log error but don't fail rotation
			// The file will still be readable by scanning
			fmt.Printf("Warning: failed to write hints to segment %d: %v\n", oldFileID, err)
		}

		// Flush again after writing hints
		if err := c.activeWriter.Flush(); err != nil {
			return err
		}

		// Sync to disk
		if err := c.activeFile.Sync(); err != nil {
			return err
		}

		// Get final file size and register the segment for tracking
		finalStat, err := c.activeFile.Stat()
		if err == nil {
			oldSegID := segmentID{generation: oldFileID, level: 0}
			oldPath := filepath.Join(c.dir, oldSegID.String())
			c.registerSegment(oldSegID, oldPath, finalStat.Size())
		}
	}

	// Close current file
	if c.activeFile != nil {
		if err := c.activeFile.Close(); err != nil {
			return err
		}
	}

	// Create new file
	c.activeFileID++
	return c.openLogFile()
}

// writeHintsToActiveFile appends hints to the active file and updates its header
// This must be called after flushing the activeWriter and before closing the file
func (c *DiskCache[V]) writeHintsToActiveFile(fileID uint32, hintOffset int64) error {
	// Write all hints for this file to the active writer
	c.keydir.Range(func(k string, entry *keyEntry) bool {
		// Only write hints for this specific file
		if entry.fileID != fileID {
			return false // continue
		}

		hintEntry := &hintEntry{
			timestamp: entry.timestamp,
			keySize:   uint32(len(k)),
			valueSize: uint32(entry.size - uint32(headerSize) - uint32(len(k))),
			offset:    entry.offset,
			key:       []byte(k),
		}

		if err := c.writeHintEntry(c.activeWriter, hintEntry); err != nil {
			// Can't return error from Range callback, will handle below
			return false // continue
		}
		return false // continue
	})

	// Flush hints to ensure they're written before we update the header
	if err := c.activeWriter.Flush(); err != nil {
		return err
	}

	// Update file header with hint offset
	header := &fileHeader{hintOffset: hintOffset}
	if err := c.writeFileHeader(c.activeFile, header); err != nil {
		return err
	}

	return nil
}

// loadLogFile loads a single log file and updates the keydir (fallback method)
func (c *DiskCache[V]) loadLogFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Extract file ID from filename (supports both LSM and legacy formats)
	fileID, err := parseFileID(filename)
	if err != nil {
		return fmt.Errorf("invalid log file name: %s", filename)
	}

	// Try to read header first
	header, err := c.readFileHeader(file)
	if err != nil {
		return fmt.Errorf("failed to read file header: %w", err)
	}

	// Start reading after the header (or from beginning if no header)
	startOffset := int64(0)
	if header.hintOffset >= fileHeaderSize {
		startOffset = fileHeaderSize
	}

	if _, err := file.Seek(startOffset, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	offset := startOffset

	for {
		entry, entrySize, err := c.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read log entry: %w", err)
		}

		keyStr := string(entry.key)
		c.setKeyEntry(keyStr, &keyEntry{
			fileID:    fileID,
			offset:    offset,
			size:      uint32(entrySize),
			timestamp: entry.timestamp,
		})

		atomic.AddInt64(&c.stats.Keys, 1)
		atomic.AddInt64(&c.stats.DataSize, int64(entrySize))

		// Release entry back to pool
		c.releaseLogEntry(entry)

		offset += int64(entrySize)
	}

	// Update current file ID to the maximum file ID found (will reuse the last segment)
	if fileID > c.activeFileID {
		c.activeFileID = fileID
	}

	return nil
}

// readValueFromDisk reads a value from disk given a key entry using memory-mapped files
func (c *DiskCache[V]) readValueFromDisk(entry *keyEntry) ([]byte, error) {
	// Check if this is the active file - if so, use regular file I/O instead of mmap
	c.mu.RLock()
	isActiveFile := entry.fileID == c.activeFileID
	c.mu.RUnlock()

	if isActiveFile {
		// For active file, use traditional file I/O with seeking
		return c.readValueFromActiveFile(entry)
	}

	// For inactive files, use mmap
	cachedFile, err := c.getCachedFile(entry.fileID)
	if err != nil {
		return nil, err
	}

	// Lock the cached file for reading
	cachedFile.mutex.RLock()
	defer cachedFile.mutex.RUnlock()

	// Check if the offset is valid
	if entry.offset < 0 || int(entry.offset)+int(entry.size) > len(cachedFile.mmap) {
		return nil, fmt.Errorf("invalid offset %d or size %d for mmap of length %d", entry.offset, entry.size, len(cachedFile.mmap))
	}

	// Read directly from memory-mapped region
	data := cachedFile.mmap[entry.offset : entry.offset+int64(entry.size)]

	// Parse the log entry header from the mmap'd data
	if len(data) < headerSize {
		return nil, fmt.Errorf("insufficient data for header")
	}

	crc := binary.LittleEndian.Uint32(data[0:4])
	timestamp := binary.LittleEndian.Uint32(data[4:8])
	keySize := binary.LittleEndian.Uint32(data[8:12])
	valueSize := binary.LittleEndian.Uint32(data[12:16])

	// Verify we have enough data
	expectedSize := headerSize + int(keySize) + int(valueSize)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("insufficient data: expected %d, got %d", expectedSize, len(data))
	}

	// Extract value from mmap (skip header and key)
	valueStart := headerSize + int(keySize)
	valueEnd := valueStart + int(valueSize)
	value := data[valueStart:valueEnd]

	// Verify CRC
	logEntry := &logEntry{
		timestamp: timestamp,
		keySize:   keySize,
		valueSize: valueSize,
		key:       data[headerSize : headerSize+keySize],
		value:     value,
	}
	expectedCRC := c.calculateCRC(logEntry)
	if crc != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, crc)
	}

	// Make a copy of the value since we're returning it outside the mmap
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	return valueCopy, nil
}

// readValueFromActiveFile reads a value from the active file using traditional file I/O
func (c *DiskCache[V]) readValueFromActiveFile(entry *keyEntry) ([]byte, error) {
	c.mu.RLock()
	file := c.activeFile
	c.mu.RUnlock()

	if file == nil {
		return nil, fmt.Errorf("active file is nil")
	}

	// Create a temporary read buffer
	buf := make([]byte, entry.size)

	// Read from file at the specified offset
	n, err := file.ReadAt(buf, entry.offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read from active file: %w", err)
	}
	if n < int(entry.size) {
		return nil, fmt.Errorf("short read from active file: expected %d, got %d", entry.size, n)
	}

	// Parse the log entry header
	if len(buf) < headerSize {
		return nil, fmt.Errorf("insufficient data for header")
	}

	crc := binary.LittleEndian.Uint32(buf[0:4])
	timestamp := binary.LittleEndian.Uint32(buf[4:8])
	keySize := binary.LittleEndian.Uint32(buf[8:12])
	valueSize := binary.LittleEndian.Uint32(buf[12:16])

	// Verify we have enough data
	expectedSize := headerSize + int(keySize) + int(valueSize)
	if len(buf) < expectedSize {
		return nil, fmt.Errorf("insufficient data: expected %d, got %d", expectedSize, len(buf))
	}

	// Extract value (skip header and key)
	valueStart := headerSize + int(keySize)
	valueEnd := valueStart + int(valueSize)
	value := buf[valueStart:valueEnd]

	// Verify CRC
	logEntry := &logEntry{
		timestamp: timestamp,
		keySize:   keySize,
		valueSize: valueSize,
		key:       buf[headerSize : headerSize+keySize],
		value:     value,
	}
	expectedCRC := c.calculateCRC(logEntry)
	if crc != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, crc)
	}

	// Make a copy of the value
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	return valueCopy, nil
}

// getCachedFile returns a memory-mapped cached file for the given file ID
func (c *DiskCache[V]) getCachedFile(fileID uint32) (*cachedFile, error) {
	c.fileCacheMutex.RLock()
	cached, exists := c.fileCache[fileID]
	c.fileCacheMutex.RUnlock()

	if exists {
		return cached, nil
	}

	// File not in cache, need to open and mmap it
	c.fileCacheMutex.Lock()
	defer c.fileCacheMutex.Unlock()

	// Double-check in case another goroutine added it while we were waiting for the lock
	if cached, exists := c.fileCache[fileID]; exists {
		return cached, nil
	}

	// Open the file - try LSM format at all levels, then legacy
	var file *os.File
	var filename string
	var err error

	// Try all LSM levels (0-4)
	for level := uint8(0); level <= 4; level++ {
		filename = filepath.Join(c.dir, fmt.Sprintf("%08d-%02d.log", fileID, level))
		file, err = os.Open(filename)
		if err == nil {
			break // Found it!
		}
	}

	// If not found in LSM format, try legacy format
	if err != nil {
		filename = filepath.Join(c.dir, fmt.Sprintf("%016d.log", fileID))
		file, err = os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("file not found for ID %d: %w", fileID, err)
		}
	}

	// Memory-map the file for fast reads
	mmapData, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to mmap file %s: %w", filename, err)
	}

	cached = &cachedFile{
		file:  file,
		mmap:  mmapData,
		mutex: sync.RWMutex{},
	}

	c.fileCache[fileID] = cached
	return cached, nil
}

// removeCachedFile removes a file from the cache, unmaps it, and closes it
func (c *DiskCache[V]) removeCachedFile(fileID uint32) error {
	c.fileCacheMutex.Lock()
	defer c.fileCacheMutex.Unlock()

	cached, exists := c.fileCache[fileID]
	if !exists {
		return nil // File not in cache, nothing to do
	}

	// Lock the file for writing to ensure no reads are in progress
	cached.mutex.Lock()
	defer cached.mutex.Unlock()

	// Unmap the memory-mapped region
	var mmapErr error
	if cached.mmap != nil {
		mmapErr = cached.mmap.Unmap()
	}

	// Close the file
	fileErr := cached.file.Close()

	// Remove from cache
	delete(c.fileCache, fileID)

	// Return first error encountered
	if mmapErr != nil {
		return mmapErr
	}
	return fileErr
}

// readLogEntry reads a complete log entry from a reader
func (c *DiskCache[V]) readLogEntry(reader *bufio.Reader) (*logEntry, int, error) {
	// Get header buffer from pool
	headerPtr := c.headerPool.Get().(*[]byte)
	header := *headerPtr
	defer c.headerPool.Put(headerPtr)

	// Read header
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, 0, err
	}

	// Get entry from pool
	entry := c.entryPool.Get().(*logEntry)
	entry.crc = binary.LittleEndian.Uint32(header[0:4])
	entry.timestamp = binary.LittleEndian.Uint32(header[4:8])
	entry.keySize = binary.LittleEndian.Uint32(header[8:12])
	entry.valueSize = binary.LittleEndian.Uint32(header[12:16])

	// Allocate or reuse key buffer
	if cap(entry.key) < int(entry.keySize) {
		entry.key = make([]byte, entry.keySize)
	} else {
		entry.key = entry.key[:entry.keySize]
	}

	// Read key
	if _, err := io.ReadFull(reader, entry.key); err != nil {
		c.entryPool.Put(entry)
		return nil, 0, err
	}

	// Read value
	if entry.valueSize > 0 {
		// Allocate or reuse value buffer
		if cap(entry.value) < int(entry.valueSize) {
			entry.value = make([]byte, entry.valueSize)
		} else {
			entry.value = entry.value[:entry.valueSize]
		}

		if _, err := io.ReadFull(reader, entry.value); err != nil {
			c.entryPool.Put(entry)
			return nil, 0, err
		}
	} else {
		// Clear value if not needed
		entry.value = entry.value[:0]
	}

	// Verify CRC
	expectedCRC := c.calculateCRC(entry)
	if entry.crc != expectedCRC {
		c.entryPool.Put(entry)
		return nil, 0, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, entry.crc)
	}

	entrySize := headerSize + int(entry.keySize) + int(entry.valueSize)
	return entry, entrySize, nil
}

// releaseLogEntry returns a log entry to the pool for reuse
func (c *DiskCache[V]) releaseLogEntry(entry *logEntry) {
	if entry != nil {
		c.entryPool.Put(entry)
	}
}

// calculateCRC calculates the CRC32 checksum for a log entry
func (c *DiskCache[V]) calculateCRC(entry *logEntry) uint32 {
	crc := crc32.NewIEEE()

	// Pre-allocate a small buffer for the header fields to avoid allocations
	var buf [12]byte

	// Include timestamp (4 bytes)
	binary.LittleEndian.PutUint32(buf[0:4], entry.timestamp)
	// Include key size (4 bytes)
	binary.LittleEndian.PutUint32(buf[4:8], entry.keySize)
	// Include value size (4 bytes)
	binary.LittleEndian.PutUint32(buf[8:12], entry.valueSize)

	crc.Write(buf[:])

	// Include key and value
	crc.Write(entry.key)
	if len(entry.value) > 0 {
		crc.Write(entry.value)
	}

	return crc.Sum32()
}

// getKeyEntry retrieves a key entry from the keydir (concurrent read-safe)
func (c *DiskCache[V]) getKeyEntry(key string) (*keyEntry, bool) {
	return c.keydir.Get(key)
}

// setKeyEntry sets a key entry in the keydir
func (c *DiskCache[V]) setKeyEntry(key string, entry *keyEntry) {
	c.keydir.Set(key, entry)
}

// parseFileID extracts the file ID from a filename (supports both LSM and legacy formats)
func parseFileID(filename string) (uint32, error) {
	base := filepath.Base(filename)
	base = strings.TrimSuffix(base, ".log")

	// Try LSM format first (NNNNNNNN-LL)
	if strings.Contains(base, "-") {
		parts := strings.Split(base, "-")
		if len(parts) == 2 {
			gen, err := strconv.ParseUint(parts[0], 10, 32)
			if err == nil {
				return uint32(gen), nil
			}
		}
	}

	// Try legacy format (NNNNNNNNNNNNNNNN)
	id, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid file ID format: %s", base)
	}
	return uint32(id), nil
}

// writeHintEntry writes a hint entry to a writer
func (c *DiskCache[V]) writeHintEntry(writer *bufio.Writer, hint *hintEntry) error {
	header := make([]byte, hintHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], hint.timestamp)
	binary.LittleEndian.PutUint32(header[4:8], hint.keySize)
	binary.LittleEndian.PutUint32(header[8:12], hint.valueSize)
	binary.LittleEndian.PutUint64(header[12:20], uint64(hint.offset))

	if _, err := writer.Write(header); err != nil {
		return err
	}

	if _, err := writer.Write(hint.key); err != nil {
		return err
	}

	return nil
}
