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
	"github.com/tidwall/btree"
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
//   - Append-only log segments: All mutations (Set/Delete) are written sequentially
//     to the active segment file. When the file reaches a size threshold it is
//     rotated and a new active segment is created.
//   - In-memory key directory: The complete key space lives in memory for O(1)
//     lookups. We store it in a B-tree (github.com/tidwall/btree) which provides
//     efficient lookups, low memory overhead, and lock-free reads by atomically
//     swapping the tree root on updates using copy-on-write.
//   - Crash recovery with hint data: Historical segment files can contain an
//     embedded hint section (an index of key -> file offset metadata) appended
//     at rotation time. On startup we first try to rebuild the key directory
//     from these hints; if unavailable we fall back to scanning the segment.
//   - Compaction: Old segment files are periodically compacted. Only the latest
//     non-deleted version of each key is rewritten into the active log; obsolete
//     and deleted entries are discarded, reclaiming disk space.
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
	keydir          atomic.Pointer[btree.Map[string, *keyEntry]]
	keydirMu        sync.Mutex // Protects keydir updates
	stats           Stats
	closed          atomic.Bool
	compactionMutex sync.Mutex
	lastCompaction  time.Time
	// File caching for efficient reads
	fileCache      map[uint32]*cachedFile
	fileCacheMutex sync.RWMutex
	// In-memory segment tracking for efficient compaction
	segments      map[segmentID]*segmentInfo
	segmentsMutex sync.RWMutex
	// Auto-compaction
	autoCompactDone chan struct{} // Signals auto-compaction goroutine to stop
	// Buffer pools for reducing allocations
	headerPool     *sync.Pool
	entryPool      *sync.Pool
	marshalBufPool *BufferPool // Pool for marshal operations
	// Configuration
	maxSegmentSize      int64
	autoCompactEnabled  bool
	autoCompactInterval time.Duration
	// Marshaling
	marshaler Marshaler[V]
}

// DiskCacheConfig holds configuration options for DiskCache
type DiskCacheConfig struct {
	// MaxSegmentSize is the maximum size of a segment file before rotation
	// If 0, defaults to 16MB
	MaxSegmentSize int64
	// AutoCompactEnabled enables automatic background compaction
	// If false, compaction must be triggered manually via Compact()
	AutoCompactEnabled bool
	// AutoCompactInterval is the time between automatic compaction checks
	// If 0, defaults to 5 minutes
	AutoCompactInterval time.Duration
}

// keyEntry represents an entry in the in-memory key directory
type keyEntry struct {
	fileID    uint32
	offset    int64
	size      uint32
	timestamp uint32
	deleted   bool
}

// logEntry represents a record in the log file
type logEntry struct {
	crc       uint32
	timestamp uint32
	keySize   uint32
	valueSize uint32
	key       []byte
	value     []byte
	deleted   bool
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

const (
	// File header size: hintOffset(8) = 8 bytes
	fileHeaderSize = 8
	// Header size: crc(4) + timestamp(4) + keySize(4) + valueSize(4) + deleted(1) = 17 bytes
	headerSize = 17
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
		marshaler:           marshaler,
		segments:            make(map[segmentID]*segmentInfo),
		autoCompactEnabled:  config.AutoCompactEnabled,
		autoCompactInterval: autoCompactInterval,
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

	// Initialize the keydir with an empty btree
	tree := btree.NewMap[string, *keyEntry](64) // degree 64 for good performance
	cache.keydir.Store(tree)

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
	if !exists || entry.deleted {
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
	if !exists || entry.deleted {
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
func (c *DiskCache[V]) Set(key []byte, value V) error {
	// Quick closed check without lock
	if c.isClosed() {
		return ErrCacheClosed
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
		deleted:   false,
	}
	entry.crc = c.calculateCRC(entry)

	// Lock-free keydir read to check if this is a new key
	keyStr := string(key)
	oldEntry, _ := c.getKeyEntry(keyStr)

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

	// Update keydir WITHOUT holding c.mu (keydir updates are lock-free)
	c.setKeyEntry(keyStr, &keyEntry{
		fileID:    fileID,
		offset:    offset,
		size:      headerSize + entry.keySize + entry.valueSize,
		timestamp: entry.timestamp,
		deleted:   false,
	})

	// Update stats (atomic operations)
	if oldEntry == nil || oldEntry.deleted {
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
			deleted:   false,
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

		isNewKey := !prep.oldExists || (prep.oldEntry != nil && prep.oldEntry.deleted)

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
			deleted:   false,
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

// Delete removes a key from the cache
func (c *DiskCache[V]) Delete(key []byte) error {
	// Quick closed check without lock
	if c.isClosed() {
		return ErrCacheClosed
	}

	// Lock-free check if key exists
	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	if !exists || entry.deleted {
		return ErrKeyNotFound
	}

	// Prepare delete record and calculate CRC WITHOUT holding lock
	logEntry := &logEntry{
		timestamp: uint32(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: 0,
		key:       key,
		value:     nil,
		deleted:   true,
	}
	logEntry.crc = c.calculateCRC(logEntry)

	// Now acquire write lock ONLY for file operations
	c.mu.Lock()

	// Double-check closed and existence after acquiring lock
	if c.isClosed() {
		c.mu.Unlock()
		return ErrCacheClosed
	}

	// Re-check existence under lock (key might have been deleted)
	entry, exists = c.getKeyEntry(keyStr)
	if !exists || entry.deleted {
		c.mu.Unlock()
		return ErrKeyNotFound
	}

	// Write delete record
	offset, err := c.writeLogEntry(logEntry)
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("failed to write delete record: %w", err)
	}

	// Capture fileID before releasing lock
	fileID := c.activeFileID
	c.mu.Unlock()

	// Update keydir WITHOUT holding c.mu (keydir updates are lock-free)
	c.setKeyEntry(keyStr, &keyEntry{
		fileID:    fileID,
		offset:    offset,
		size:      headerSize + logEntry.keySize,
		timestamp: logEntry.timestamp,
		deleted:   true,
	})

	// Update stats (atomic operations)
	atomic.AddInt64(&c.stats.Keys, -1)
	atomic.AddInt64(&c.stats.Deletes, 1)
	atomic.AddInt64(&c.stats.DataSize, int64(headerSize+logEntry.keySize))

	return nil
}

// Has checks if a key exists in the cache
func (c *DiskCache[V]) Has(key []byte) bool {
	if c.isClosed() {
		return false
	}

	// Lock-free keydir read
	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	return exists && !entry.deleted
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

	// Stop auto-compaction (without holding mu)
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

			// Run compaction (errors are logged but don't stop the loop)
			result, err := c.Compact()
			if err != nil {
				// Log error but continue
				fmt.Printf("Auto-compaction error: %v\n", err)
			} else if result != nil && result.Type != "none" {
				// Log successful compaction
				fmt.Printf("Auto-compaction completed: type=%s level=%d live=%d bytes=%d\n",
					result.Type, result.Level, result.LiveEntries, result.BytesWritten)
			}
		}
	}
}

// registerSegment adds a segment to the in-memory tracking
func (c *DiskCache[V]) registerSegment(id segmentID, path string, totalBytes int64) {
	c.segmentsMutex.Lock()
	defer c.segmentsMutex.Unlock()

	c.segments[id] = &segmentInfo{
		id:         id,
		path:       path,
		totalBytes: totalBytes,
	}
}

// unregisterSegment removes a segment from in-memory tracking
func (c *DiskCache[V]) unregisterSegment(id segmentID) {
	c.segmentsMutex.Lock()
	defer c.segmentsMutex.Unlock()

	delete(c.segments, id)
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
	// Calculate index size by getting the tree length
	tree := c.keydir.Load()
	if tree != nil {
		count := tree.Len()
		stats.IndexSize = int64(count * 64) // Rough estimate
	}

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

// Scan iterates through all entries in physical storage order (by segment, then offset within each segment).
// The callback receives each key-value pair and should return true to stop iteration, false to continue.
// Deleted entries are included with a zero value (nil for pointers/slices). Corrupted records are skipped.
// Note: Duplicates are expected when keys appear in multiple segments.
func (c *DiskCache[V]) Scan(fn func(key []byte, value *V) bool) error {
	if c.isClosed() {
		return ErrCacheClosed
	}

	// Get active segment info first
	c.mu.RLock()
	activeID := c.activeFileID
	hasActive := c.activeFile != nil
	c.mu.RUnlock()

	// Get all rotated segments sorted by generation and level
	c.segmentsMutex.RLock()
	segmentIDs := make([]segmentID, 0, len(c.segments))
	for id := range c.segments {
		segmentIDs = append(segmentIDs, id)
	}
	c.segmentsMutex.RUnlock()

	// Sort segments by generation (oldest first), then by level
	sort.Slice(segmentIDs, func(i, j int) bool {
		if segmentIDs[i].generation != segmentIDs[j].generation {
			return segmentIDs[i].generation < segmentIDs[j].generation
		}
		return segmentIDs[i].level < segmentIDs[j].level
	})

	// Scan rotated segments first
	for _, segID := range segmentIDs {
		stopped, err := c.scanSegmentPhysical(segID, fn)
		if err != nil {
			// Log error but continue with next segment
			continue
		}
		if stopped {
			return nil // User requested stop
		}
	}

	// Finally scan the active segment (most recent data)
	// We need to flush it first to ensure data is on disk
	if hasActive {
		c.mu.Lock()
		if c.activeWriter != nil {
			c.activeWriter.Flush()
		}
		c.mu.Unlock()

		activeSegID := segmentID{generation: activeID, level: 0}
		stopped, _ := c.scanSegmentPhysical(activeSegID, fn)
		if stopped {
			return nil
		}
	}

	return nil
}

// scanSegmentPhysical scans a single segment file in physical order
// Returns (stopped, error) where stopped indicates if the user callback requested stop
func (c *DiskCache[V]) scanSegmentPhysical(segID segmentID, fn func([]byte, *V) bool) (bool, error) {
	// Check if this is the active segment
	c.mu.RLock()
	isActive := segID.generation == c.activeFileID
	c.mu.RUnlock()

	// For active segment, use traditional buffered I/O
	// For inactive segments, try to use mmap for better performance
	if isActive {
		return c.scanSegmentWithBufferedIO(segID, fn)
	}

	// Try to use mmap for inactive segments
	cachedFile, err := c.getCachedFile(segID.generation)
	if err != nil {
		// Fall back to buffered I/O if mmap fails
		return c.scanSegmentWithBufferedIO(segID, fn)
	}

	// Scan using mmap
	cachedFile.mutex.RLock()
	defer cachedFile.mutex.RUnlock()

	data := cachedFile.mmap
	offset := int64(0)

	// Skip file header if present
	if len(data) >= fileHeaderSize {
		offset = fileHeaderSize
	}

	for offset < int64(len(data)) {
		// Check if we have enough data for header
		if offset+headerSize > int64(len(data)) {
			break // End of file
		}

		// Parse header directly from mmap
		headerData := data[offset : offset+headerSize]
		crc := binary.LittleEndian.Uint32(headerData[0:4])
		timestamp := binary.LittleEndian.Uint32(headerData[4:8])
		keySize := binary.LittleEndian.Uint32(headerData[8:12])
		valueSize := binary.LittleEndian.Uint32(headerData[12:16])
		deleted := headerData[16] == 1

		entrySize := int64(headerSize + keySize + valueSize)
		if offset+entrySize > int64(len(data)) {
			break // Incomplete entry at end of file
		}

		entryData := data[offset : offset+entrySize]
		keyData := entryData[headerSize : headerSize+keySize]
		valueData := entryData[headerSize+keySize : headerSize+keySize+valueSize]

		// Verify CRC
		logEntry := &logEntry{
			timestamp: timestamp,
			keySize:   keySize,
			valueSize: valueSize,
			key:       keyData,
			value:     valueData,
			deleted:   deleted,
		}
		expectedCRC := c.calculateCRC(logEntry)
		if crc != expectedCRC {
			// Skip corrupted entry
			offset += entrySize
			continue
		}

		// Handle deleted entries
		if deleted {
			// Make a copy of the key since we're returning it outside mmap
			keyCopy := make([]byte, len(keyData))
			copy(keyCopy, keyData)

			// Pass nil pointer for deleted entries
			if fn(keyCopy, nil) {
				return true, nil // Stop requested
			}
			offset += entrySize
			continue
		}

		// Make copies before unmarshaling (data will escape mmap scope)
		valueCopy := make([]byte, len(valueData))
		copy(valueCopy, valueData)

		// Unmarshal the value into a new V
		var value V
		err = c.marshaler.Unmarshal(valueCopy, &value)
		if err != nil {
			// Skip entries that fail to unmarshal
			offset += entrySize
			continue
		}

		// Make a copy of the key
		keyCopy := make([]byte, len(keyData))
		copy(keyCopy, keyData)

		// Call user callback
		if fn(keyCopy, &value) {
			return true, nil // Stop requested
		}

		offset += entrySize
	}

	return false, nil
}

// scanSegmentWithBufferedIO scans a segment using traditional buffered I/O
func (c *DiskCache[V]) scanSegmentWithBufferedIO(segID segmentID, fn func([]byte, *V) bool) (bool, error) {
	filename := filepath.Join(c.dir, segID.String())
	file, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer file.Close()

	// Read and validate header
	_, err = c.readFileHeader(file)
	if err != nil {
		// Try scanning from beginning if header is invalid
		if _, err := file.Seek(0, 0); err != nil {
			return false, err
		}
	} else {
		// Skip header to start reading data
		if _, err := file.Seek(fileHeaderSize, 0); err != nil {
			return false, err
		}
	}

	// Use a large buffer for sequential reads
	reader := bufio.NewReaderSize(file, 256*1024) // 256KB buffer

	for {
		logEntry, _, err := c.readLogEntry(reader)
		if err == io.EOF {
			break // End of segment
		}
		if err != nil {
			// Skip corrupted entries and continue
			continue
		}

		// Handle deleted entries - call callback with nil pointer
		if logEntry.deleted {
			key := logEntry.key
			c.releaseLogEntry(logEntry)

			// Pass nil pointer for deleted entries
			if fn(key, nil) {
				return true, nil // Stop requested
			}
			continue
		}

		// Unmarshal the value
		key := logEntry.key
		var value V
		err = c.marshaler.Unmarshal(logEntry.value, &value)
		c.releaseLogEntry(logEntry)

		if err != nil {
			// Skip entries that fail to unmarshal
			continue
		}

		// Call user callback - return all entries, including duplicates
		if fn(key, &value) {
			return true, nil // Stop requested
		}
	}

	return false, nil
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
	// Seek to hint section
	if _, err := file.Seek(hintOffset, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	hintErrors := 0

	// Collect all hint entries first
	type pendingEntry struct {
		key       []byte
		keyEntry  *keyEntry
		oldEntry  *keyEntry
		existed   bool
		valueSize uint32
	}
	var entries []pendingEntry

	for {
		entry, err := c.readHintEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			hintErrors++
			// If we get errors reading hints, return error to trigger fallback to scanning
			if len(entries) == 0 {
				// No hints loaded yet, hints section is likely corrupted from the start
				return fmt.Errorf("hints corrupted, falling back to scan: %w", err)
			}
			// We loaded some hints but then hit corruption, stop here
			fmt.Printf("Warning: hints partially corrupted in segment %d, loaded %d entries\n", fileID, len(entries))
			break
		}

		// Make a copy of the key since we'll reuse the buffer
		keyCopy := make([]byte, len(entry.key))
		copy(keyCopy, entry.key)

		keyStr := string(keyCopy)
		oldEntry, existed := c.getKeyEntry(keyStr)

		// Hints don't contain deletion status, so we assume non-deleted
		// Deleted entries should be filtered out during compaction
		entries = append(entries, pendingEntry{
			key: keyCopy,
			keyEntry: &keyEntry{
				fileID:    fileID,
				offset:    entry.offset,
				size:      headerSize + entry.keySize + entry.valueSize,
				timestamp: entry.timestamp,
				deleted:   false,
			},
			oldEntry:  oldEntry,
			existed:   existed,
			valueSize: entry.valueSize,
		})
	}

	// Batch insert all entries in a single transaction
	if len(entries) > 0 {
		c.keydirMu.Lock()
		oldTree := c.keydir.Load()
		var newTree *btree.Map[string, *keyEntry]
		if oldTree == nil {
			// Create a new tree if none exists
			newTree = btree.NewMap[string, *keyEntry](64)
		} else {
			newTree = oldTree.Copy()
		}

		// Insert all entries in the transaction
		for _, e := range entries {
			newTree.Set(string(e.key), e.keyEntry)
		}

		// Commit the transaction
		c.keydir.Store(newTree)
		c.keydirMu.Unlock()

		// Update stats after successful commit
		for _, e := range entries {
			// Only increment Keys counter for new keys, not overwrites
			if !e.existed || (e.oldEntry != nil && e.oldEntry.deleted) {
				atomic.AddInt64(&c.stats.Keys, 1)
			}
			atomic.AddInt64(&c.stats.DataSize, int64(e.valueSize+headerSize))
		}
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
		oldEntry, existed := c.getKeyEntry(keyStr)
		c.setKeyEntry(keyStr, &keyEntry{
			fileID:    fileID,
			offset:    offset,
			size:      uint32(entrySize),
			timestamp: entry.timestamp,
			deleted:   entry.deleted,
		})

		// Update stats based on the entry type
		if !entry.deleted {
			// Adding a non-deleted entry
			if !existed || (oldEntry != nil && oldEntry.deleted) {
				atomic.AddInt64(&c.stats.Keys, 1)
			}
		} else {
			// Adding a deleted entry
			if existed && oldEntry != nil && !oldEntry.deleted {
				// Marking an existing non-deleted key as deleted
				atomic.AddInt64(&c.stats.Keys, -1)
			}
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
	if entry.deleted {
		header[16] = 1
	} else {
		header[16] = 0
	}

	if _, err := c.activeWriter.Write(header); err != nil {
		return 0, err
	}

	// Write key
	if _, err := c.activeWriter.Write(entry.key); err != nil {
		return 0, err
	}

	// Write value (if not deleted)
	if !entry.deleted && entry.valueSize > 0 {
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
	tree := c.keydir.Load()
	if tree != nil {
		tree.Scan(func(k string, entry *keyEntry) bool {
			// Only write hints for this specific file, and skip deleted entries
			if entry.fileID != fileID {
				return true // continue scanning
			}

			// Skip deleted entries - they shouldn't be in hints
			if entry.deleted {
				return true // continue scanning
			}

			hintEntry := &hintEntry{
				timestamp: entry.timestamp,
				keySize:   uint32(len(k)),
				valueSize: uint32(entry.size - uint32(headerSize) - uint32(len(k))),
				offset:    entry.offset,
				key:       []byte(k),
			}

			if err := c.writeHintEntry(c.activeWriter, hintEntry); err != nil {
				return false // stop scanning on error
			}
			return true // continue scanning
		})
	}

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
			deleted:   entry.deleted,
		})

		if !entry.deleted {
			atomic.AddInt64(&c.stats.Keys, 1)
		}
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

// compactSegment processes a single segment file
func (c *DiskCache[V]) compactSegment(fileID uint32) error {
	// Try LSM format first, then legacy
	filename := filepath.Join(c.dir, fmt.Sprintf("%08d-00.log", fileID))
	file, err := os.Open(filename)
	if err != nil {
		// Try legacy format
		filename = filepath.Join(c.dir, fmt.Sprintf("%016d.log", fileID))
		file, err = os.Open(filename)
		if err != nil {
			return fmt.Errorf("failed to open segment file: %w", err)
		}
	}
	defer file.Close()

	// Read file header to determine where data starts and where hints begin
	header, err := c.readFileHeader(file)
	if err != nil {
		return fmt.Errorf("failed to read file header: %w", err)
	}

	// Determine the end offset for reading log entries
	// If hints exist, stop before them; otherwise read to EOF
	endOffset := int64(0)
	if header.hintOffset > 0 {
		endOffset = header.hintOffset
	} else {
		// No hints, read entire file
		stat, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file: %w", err)
		}
		endOffset = stat.Size()
	}

	// Start reading after the header
	if _, err := file.Seek(fileHeaderSize, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	offset := int64(fileHeaderSize)

	for offset < endOffset {
		entry, entrySize, err := c.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Handle unexpected EOF and other read errors gracefully during compaction
			// Skip corrupted entries and continue processing
			if err == io.ErrUnexpectedEOF || err.Error() == "unexpected EOF" {
				// Log the error but continue with compaction
				fmt.Printf("Warning: skipping corrupted entry in segment %d at offset %d: %v\n", fileID, offset, err)
				break // Stop processing this segment but don't fail compaction
			}
			return fmt.Errorf("failed to read log entry from segment: %w", err)
		}

		keyStr := string(entry.key)

		// Skip deleted entries
		if entry.deleted {
			c.releaseLogEntry(entry)
			offset += int64(entrySize)
			continue
		}

		// Check if this entry is still the latest version according to keydir
		// We need to do this atomically with the potential write operation
		currentEntry, exists := c.getKeyEntry(keyStr)
		isLatest := exists &&
			currentEntry.fileID == fileID &&
			currentEntry.offset == offset &&
			!currentEntry.deleted

		if isLatest {
			// This is the latest version, unmarshal then write it to the active file
			// We need to unlock the compaction mutex temporarily to avoid deadlock
			// since Set also needs to take the write lock
			var value V
			err = c.marshaler.Unmarshal(entry.value, &value)
			if err != nil {
				// Skip corrupted entries
				c.releaseLogEntry(entry)
				offset += int64(entrySize)
				continue
			}

			c.compactionMutex.Unlock()
			err = c.Set(entry.key, value)
			c.compactionMutex.Lock()

			if err != nil {
				c.releaseLogEntry(entry)
				return fmt.Errorf("failed to rewrite entry during compaction: %w", err)
			}
		}

		// Release entry back to pool
		c.releaseLogEntry(entry)

		offset += int64(entrySize)
	}

	// Close the file before deleting it
	file.Close()

	// Remove the file from the cache before deleting it from disk
	c.removeCachedFile(fileID)

	// Delete the processed segment file
	if err := os.Remove(filename); err != nil {
		return fmt.Errorf("failed to remove segment file %s: %w", filename, err)
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
	deleted := data[16] == 1

	if deleted {
		return nil, ErrKeyNotFound
	}

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
		deleted:   deleted,
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
	deleted := buf[16] == 1

	if deleted {
		return nil, ErrKeyNotFound
	}

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
		deleted:   deleted,
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
	entry.deleted = header[16] == 1

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

	// Read value (if not deleted)
	if !entry.deleted && entry.valueSize > 0 {
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
	var buf [13]byte

	// Include timestamp (4 bytes)
	binary.LittleEndian.PutUint32(buf[0:4], entry.timestamp)
	// Include key size (4 bytes)
	binary.LittleEndian.PutUint32(buf[4:8], entry.keySize)
	// Include value size (4 bytes)
	binary.LittleEndian.PutUint32(buf[8:12], entry.valueSize)
	// Include deleted flag (1 byte)
	if entry.deleted {
		buf[12] = 1
	} else {
		buf[12] = 0
	}

	crc.Write(buf[:])

	// Include key and value
	crc.Write(entry.key)
	if !entry.deleted && len(entry.value) > 0 {
		crc.Write(entry.value)
	}

	return crc.Sum32()
}

// Compact intelligently determines and performs the appropriate compaction.
// It checks all levels (L0 through L4) and compacts the level that needs it most.
// If no LSM compaction is needed, it performs garbage collection on segments with high dead ratios.
// Returns CompactionResult with details about what was compacted, or a result with Type="none" if no compaction was needed.
func (c *DiskCache[V]) Compact() (*CompactionResult, error) {
	if c == nil {
		return &CompactionResult{Type: "none"}, nil
	}

	c.compactionMutex.Lock()
	defer c.compactionMutex.Unlock()

	if c.isClosed() {
		return nil, ErrCacheClosed
	}

	// Get segments grouped by level
	byLevel, err := c.getSegmentsByLevel()
	if err != nil {
		return nil, fmt.Errorf("failed to get segments: %w", err)
	}

	// Priority 1: LSM compaction (level merging)
	// L0 first (most important), then L1, L2, L3, L4
	levelsToCheck := []uint8{0, 1, 2, 3, 4}

	for _, level := range levelsToCheck {
		segments := byLevel[level]

		// Determine if this level needs compaction
		var batchSize int
		var shouldCompact bool

		if level == 0 {
			// L0: Compact if we have 8+ segments (increased for high write volumes)
			batchSize = 8
			shouldCompact = len(segments) >= batchSize
		} else if level == 4 {
			// L4 (max level): Only compact if we have 2+ segments (merge into single L4)
			batchSize = len(segments)
			shouldCompact = len(segments) >= 2
		} else {
			// L1-L3: Compact if we have 3+ segments
			batchSize = 4
			shouldCompact = len(segments) >= batchSize
		}

		if shouldCompact {
			// For L4, we merge all segments into one (self-compaction)
			if level == 4 {
				return c.compactLevel4Locked(segments)
			}

			// For other levels, compact to next level (unlocked version to avoid deadlock)
			return c.compactLevelLSMLocked(level, batchSize, byLevel)
		}
	}

	// Priority 2: Garbage collection (if no LSM compaction needed)
	// Find and compact segments with high dead ratios
	return c.compactGarbageLocked(byLevel)
}

// compactGarbageLocked performs garbage collection on a single segment with high dead ratio
// This is called by Compact() when no LSM compaction is needed
// GC only runs on L4 segments to avoid creating churn at lower levels
func (c *DiskCache[V]) compactGarbageLocked(byLevel map[uint8][]*segmentInfo) (*CompactionResult, error) {
	// GC configuration for automatic compaction
	// Only runs on L4 (max level) to prevent churn from creating new L0 segments
	const (
		deadRatioThreshold = 0.4              // 40% dead data triggers GC
		minDeadBytes       = 50 * 1024 * 1024 // 50MB minimum dead space required
		minSegmentAge      = 1 * time.Second  // Very short age for manual compaction
		maxBytesPerSecond  = 10 * 1024 * 1024 // 10MB/s rate limit (faster than background)
	)

	// Only scan L4 segments - lower levels should use LSM compaction
	segments := byLevel[4]

	for _, seg := range segments {
		// Skip active segment
		if seg.id.generation == c.activeFileID {
			continue
		}

		// Check if segment is old enough
		fileInfo, err := os.Stat(seg.path)
		if err != nil {
			continue
		}
		if time.Since(fileInfo.ModTime()) < minSegmentAge {
			continue
		}

		// Scan segment to calculate dead ratio
		stats, err := c.scanSegmentForGC(seg.id.generation, seg.path)
		if err != nil {
			continue
		}

		// Calculate absolute dead space
		deadBytes := stats.TotalBytes - stats.LiveBytes

		// Only GC if both dead ratio AND absolute dead space are significant
		if stats.NeedsGC && stats.DeadRatio >= deadRatioThreshold && deadBytes >= minDeadBytes {
			// Perform GC on this segment by compacting it
			if err := c.compactSegment(seg.id.generation); err != nil {
				// Log error but continue - we did something
				_ = err
			}

			// We performed GC on one L4 segment, return result
			return &CompactionResult{
				Type:          "GC",
				Level:         4,
				InputSegments: []string{filepath.Base(seg.path)},
				LiveEntries:   int(stats.LiveEntries),
				BytesWritten:  stats.LiveBytes,
			}, nil
		}
	}

	// No GC needed
	return &CompactionResult{Type: "none"}, nil
}

// compactLevelLSMLocked is the internal version that assumes the lock is already held
func (c *DiskCache[V]) compactLevelLSMLocked(sourceLevel uint8, batchSize int, byLevel map[uint8][]*segmentInfo) (*CompactionResult, error) {
	if c.isClosed() {
		return nil, ErrCacheClosed
	}

	sourceSegments := byLevel[sourceLevel]
	if len(sourceSegments) < batchSize {
		return &CompactionResult{Type: "none"}, nil // Not enough segments to compact
	}

	// Take the oldest N segments (they're already sorted by generation)
	segmentsToCompact := sourceSegments[:batchSize]

	// Find the maximum generation from the segments being compacted
	var maxGen uint32
	for _, seg := range segmentsToCompact {
		if seg.id.generation > maxGen {
			maxGen = seg.id.generation
		}
	}

	// Target level is source level + 1
	targetLevel := sourceLevel + 1

	// Output segment uses the highest generation from inputs at the target level
	outputID := segmentID{
		generation: maxGen,
		level:      targetLevel,
	}

	result := &CompactionResult{
		Type:          "LSM",
		Level:         sourceLevel,
		OutputSegment: outputID.String(),
	}

	// Collect input segment names
	for _, seg := range segmentsToCompact {
		result.InputSegments = append(result.InputSegments, filepath.Base(seg.path))
	}

	// Compact the segments
	liveEntries, bytesWritten, err := c.compactSegmentsToLevel(segmentsToCompact, outputID)
	if err != nil {
		return nil, fmt.Errorf("failed to compact segments: %w", err)
	}

	result.LiveEntries = liveEntries
	result.BytesWritten = bytesWritten

	// Delete source segments
	for _, seg := range segmentsToCompact {
		// Remove from file cache first
		c.removeCachedFile(seg.id.generation)

		if err := os.Remove(seg.path); err == nil {
			result.DeletedSegments = append(result.DeletedSegments, filepath.Base(seg.path))
			// Unregister from in-memory tracking
			c.unregisterSegment(seg.id)
		}
	}

	// Register the new output segment
	outputPath := filepath.Join(c.dir, outputID.String())
	if stat, err := os.Stat(outputPath); err == nil {
		c.registerSegment(outputID, outputPath, stat.Size())
	}

	return result, nil
}

// compactLevel4Locked merges all L4 segments into a single L4 segment (garbage collection)
// This assumes the compaction lock is already held
func (c *DiskCache[V]) compactLevel4Locked(segments []*segmentInfo) (*CompactionResult, error) {
	if len(segments) < 2 {
		return &CompactionResult{Type: "none"}, nil
	}

	// Find the maximum generation
	var maxGen uint32
	for _, seg := range segments {
		if seg.id.generation > maxGen {
			maxGen = seg.id.generation
		}
	}

	// Output stays at L4 but with new generation
	outputID := segmentID{
		generation: maxGen,
		level:      4,
	}

	result := &CompactionResult{
		Type:          "LSM",
		Level:         4,
		OutputSegment: outputID.String(),
	}

	// Collect input segment names
	for _, seg := range segments {
		result.InputSegments = append(result.InputSegments, filepath.Base(seg.path))
	}

	// Compact all L4 segments into one
	liveEntries, bytesWritten, err := c.compactSegmentsToLevel(segments, outputID)
	if err != nil {
		return nil, fmt.Errorf("failed to compact L4 segments: %w", err)
	}

	result.LiveEntries = liveEntries
	result.BytesWritten = bytesWritten

	// Delete source segments
	for _, seg := range segments {
		c.removeCachedFile(seg.id.generation)
		if err := os.Remove(seg.path); err == nil {
			result.DeletedSegments = append(result.DeletedSegments, filepath.Base(seg.path))
			// Unregister from in-memory tracking
			c.unregisterSegment(seg.id)
		}
	}

	// Register the new output segment
	outputPath := filepath.Join(c.dir, outputID.String())
	if stat, err := os.Stat(outputPath); err == nil {
		c.registerSegment(outputID, outputPath, stat.Size())
	}

	return result, nil
}

// getSegments retrieves the list of segments (log files) to be compacted
func (c *DiskCache[V]) getSegments() ([]*keyEntry, error) {
	files, err := filepath.Glob(filepath.Join(c.dir, "*.log"))
	if err != nil {
		return nil, err
	}

	var segments []*keyEntry

	for _, filename := range files {
		fileID, err := c.getFileID(filename)
		if err != nil {
			return nil, err
		}

		// Exclude the active file from compaction
		if fileID == c.activeFileID {
			continue
		}

		segments = append(segments, &keyEntry{
			fileID: fileID,
		})
	}

	return segments, nil
}

// getFileID extracts the file ID from the log file name
func (c *DiskCache[V]) getFileID(filename string) (uint32, error) {
	return parseFileID(filename)
}

// parseFileID extracts the generation number from a filename
// Supports both legacy format (0000000000000000.log) and LSM format (00000000-00.log)
func parseFileID(filename string) (uint32, error) {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]

	// Try LSM format first: 00000000-00
	if strings.Contains(name, "-") {
		parts := strings.Split(name, "-")
		if len(parts) == 2 {
			gen, err := strconv.ParseUint(parts[0], 10, 32)
			if err == nil {
				return uint32(gen), nil
			}
		}
	}

	// Try legacy format: 0000000000000000 (16 digits)
	fileID, err := strconv.ParseUint(name, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid file name format: %s", filename)
	}
	return uint32(fileID), nil
}

// readHintEntry reads a hint entry from the hint data
func (c *DiskCache[V]) readHintEntry(reader *bufio.Reader) (*hintEntry, error) {
	// Read header
	header := make([]byte, hintHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}

	entry := &hintEntry{
		timestamp: binary.LittleEndian.Uint32(header[0:4]),
		keySize:   binary.LittleEndian.Uint32(header[4:8]),
		valueSize: binary.LittleEndian.Uint32(header[8:12]),
		offset:    int64(binary.LittleEndian.Uint64(header[12:20])),
	}

	// Read key
	entry.key = make([]byte, entry.keySize)
	if _, err := io.ReadFull(reader, entry.key); err != nil {
		return nil, err
	}

	return entry, nil
}

// writeHintEntry writes a hint entry to the hint data
func (c *DiskCache[V]) writeHintEntry(writer *bufio.Writer, entry *hintEntry) error {
	// Write header
	header := make([]byte, hintHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], entry.timestamp)
	binary.LittleEndian.PutUint32(header[4:8], entry.keySize)
	binary.LittleEndian.PutUint32(header[8:12], entry.valueSize)
	binary.LittleEndian.PutUint64(header[12:20], uint64(entry.offset))

	if _, err := writer.Write(header); err != nil {
		return err
	}

	// Write key
	if _, err := writer.Write(entry.key); err != nil {
		return err
	}

	return nil
}

// getKeyEntry retrieves a keyEntry from the key directory
func (c *DiskCache[V]) getKeyEntry(key string) (*keyEntry, bool) {
	tree := c.keydir.Load()
	if tree == nil {
		return nil, false
	}

	entry, ok := tree.Get(key)
	return entry, ok
}

// setKeyEntry sets a keyEntry in the key directory
func (c *DiskCache[V]) setKeyEntry(key string, entry *keyEntry) {
	c.keydirMu.Lock()
	defer c.keydirMu.Unlock()

	oldTree := c.keydir.Load()
	var newTree *btree.Map[string, *keyEntry]
	if oldTree == nil {
		// Create a new tree if none exists
		newTree = btree.NewMap[string, *keyEntry](64)
	} else {
		// Copy the tree for copy-on-write
		newTree = oldTree.Copy()
	}

	newTree.Set(key, entry)
	c.keydir.Store(newTree)
}
