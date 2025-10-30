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

	"github.com/tidwall/btree"
)

// cachedFile represents a cached file handle with its reader and mutex
type cachedFile struct {
	file   *os.File
	reader *bufio.Reader
	mutex  sync.RWMutex
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
	// Configuration
	maxSegmentSize int64
	// Marshaling
	marshaler Marshaler[V]
}

// DiskCacheConfig holds configuration options for DiskCache
type DiskCacheConfig struct {
	// MaxSegmentSize is the maximum size of a segment file before rotation
	// If 0, defaults to 16MB
	MaxSegmentSize int64
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

	cache := &DiskCache[V]{
		dir: dir,
		// Initialize the file cache
		fileCache:      make(map[uint32]*cachedFile),
		maxSegmentSize: maxSegSize,
		marshaler:      marshaler,
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
	value, err := c.marshaler.Unmarshal(data)
	if err != nil {
		return zero, fmt.Errorf("failed to unmarshal value: %w", err)
	}

	return value, nil
}

// Set stores a key-value pair in the cache
func (c *DiskCache[V]) Set(key []byte, value V) error {
	// Quick closed check without lock
	if c.isClosed() {
		return ErrCacheClosed
	}

	// Marshal the value to bytes
	data, err := c.marshaler.Marshal(value)
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed() {
		return nil
	}

	c.closed.Store(true)

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

	// Close all cached files
	c.fileCacheMutex.Lock()
	for fileID, cached := range c.fileCache {
		cached.mutex.Lock()
		cached.file.Close()
		cached.mutex.Unlock()
		delete(c.fileCache, fileID)
	}
	c.fileCacheMutex.Unlock()

	return nil
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

	// Count the number of segment files from the file cache plus the active file
	c.fileCacheMutex.RLock()
	stats.Segments = int64(len(c.fileCache))
	c.fileCacheMutex.RUnlock()

	// Add 1 for the active file if it exists
	if c.activeFile != nil {
		stats.Segments++
	}

	return stats
}

// Scan iterates through all keys with the given prefix and calls the function for each key
// The function should return true to stop iteration, false to continue
func (c *DiskCache[V]) Scan(prefix []byte, fn func(key []byte) bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tree := c.keydir.Load()
	if tree == nil {
		return nil
	}

	prefixStr := string(prefix)

	// If no prefix, iterate all keys
	if len(prefix) == 0 {
		var stop bool
		tree.Scan(func(k string, v *keyEntry) bool {
			// Skip deleted entries
			if v.deleted {
				return true // continue iteration
			}
			stop = fn([]byte(k))
			return !stop // return false to stop iteration
		})
		return nil
	}

	// Use prefix-based iteration
	var stop bool
	tree.Scan(func(k string, v *keyEntry) bool {
		// Check if key has the prefix
		if !strings.HasPrefix(k, prefixStr) {
			// If we've moved past the prefix range, stop
			if k > prefixStr {
				return false
			}
			return true // continue looking
		}

		// Skip deleted entries
		if v.deleted {
			return true // continue iteration
		}

		stop = fn([]byte(k))
		return !stop // return false to stop iteration
	})

	return nil
}

// loadFromDisk loads existing data files and rebuilds the keydir
func (c *DiskCache[V]) loadFromDisk() error {
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

	// Read file header to get hint offset
	header, err := c.readFileHeader(file)
	if err != nil {
		// If header is corrupted, try to scan from the beginning
		fmt.Printf("Warning: corrupted header in segment %d, attempting scan\n", fileID)
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to seek after header error: %w", err)
		}
		// Try scanning from offset 0 (no header)
		if err := c.scanSegmentFile(file, fileID); err != nil {
			return fmt.Errorf("failed to scan segment file with corrupted header: %w", err)
		}
		return nil
	}

	if header.hintOffset > 0 {
		// Hints are available, try to read them for keydir
		if err := c.loadHintsFromSegment(file, header.hintOffset, fileID); err != nil {
			// Hints failed, fall back to scanning the data section
			fmt.Printf("Warning: hints corrupted in segment %d, falling back to full scan\n", fileID)
			if err := c.scanSegmentFile(file, fileID); err != nil {
				return fmt.Errorf("failed to scan segment file after hint failure: %w", err)
			}
		}
	} else {
		// No hints available, scan the file
		if err := c.scanSegmentFile(file, fileID); err != nil {
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
func (c *DiskCache[V]) scanSegmentFile(file *os.File, fileID uint32) error {
	// Start after the file header
	if _, err := file.Seek(fileHeaderSize, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	offset := int64(fileHeaderSize)
	skippedRecords := 0

	for {
		entry, entrySize, err := c.readLogEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Handle corrupted records - skip and try to continue
			skippedRecords++

			// Try to skip ahead and find next valid record
			// Read byte by byte looking for potentially valid record headers
			if tryOffset := c.tryFindNextRecord(file, offset); tryOffset > offset {
				offset = tryOffset
				if _, err := file.Seek(offset, 0); err != nil {
					break // Can't seek, stop processing
				}
				reader = bufio.NewReader(file)
				continue
			}

			// Can't find next valid record, stop processing this segment
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

		offset += int64(entrySize)
	}

	if skippedRecords > 0 {
		fmt.Printf("Warning: skipped %d corrupted records in segment %d\n", skippedRecords, fileID)
	}

	return nil
}

// tryFindNextRecord attempts to find the next valid record after a corruption point
// Returns the offset of the next potential valid record, or the same offset if none found
func (c *DiskCache[V]) tryFindNextRecord(file *os.File, currentOffset int64) int64 {
	// Get file size to avoid reading beyond EOF
	stat, err := file.Stat()
	if err != nil {
		return currentOffset
	}
	fileSize := stat.Size()

	// Try to scan forward byte by byte initially, then in larger increments
	maxScanDistance := int64(4096) // Scan up to 4KB ahead

	// First pass: scan byte by byte for the first 256 bytes
	for skip := int64(1); skip < 256 && currentOffset+skip < fileSize-headerSize; skip++ {
		testOffset := currentOffset + skip

		if valid := c.validateRecordAtOffset(file, testOffset, fileSize); valid {
			return testOffset
		}
	}

	// Second pass: scan in 16-byte increments for the rest
	for skip := int64(256); skip < maxScanDistance && currentOffset+skip < fileSize-headerSize; skip += 16 {
		testOffset := currentOffset + skip

		if valid := c.validateRecordAtOffset(file, testOffset, fileSize); valid {
			return testOffset
		}
	}

	return currentOffset // No valid record found
}

// validateRecordAtOffset checks if there's a valid record at the given offset
func (c *DiskCache[V]) validateRecordAtOffset(file *os.File, offset int64, fileSize int64) bool {
	// Try to read a header at this position
	if _, err := file.Seek(offset, 0); err != nil {
		return false
	}

	header := make([]byte, headerSize)
	if n, err := file.Read(header); err != nil || n != headerSize {
		return false
	}

	// Parse header fields
	keySize := binary.LittleEndian.Uint32(header[8:12])
	valueSize := binary.LittleEndian.Uint32(header[12:16])

	// Sanity check: reasonable sizes
	// Key should be 1-65KB, value should be 0-100MB
	if keySize == 0 || keySize > 65536 || valueSize > 100*1024*1024 {
		return false
	}

	// Check if the entire record would fit in the file
	recordSize := int64(headerSize) + int64(keySize) + int64(valueSize)
	if offset+recordSize > fileSize {
		return false
	}

	// Try to actually read and validate the entry with CRC
	if _, err := file.Seek(offset, 0); err != nil {
		return false
	}

	reader := bufio.NewReader(file)
	if _, _, err := c.readLogEntry(reader); err == nil {
		// Found a valid record with correct CRC!
		return true
	}

	return false
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
			value, err := c.marshaler.Unmarshal(entry.value)
			if err != nil {
				// Skip corrupted entries
				offset += int64(entrySize)
				continue
			}

			c.compactionMutex.Unlock()
			err = c.Set(entry.key, value)
			c.compactionMutex.Lock()

			if err != nil {
				return fmt.Errorf("failed to rewrite entry during compaction: %w", err)
			}
		}

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

// readValueFromDisk reads a value from disk given a key entry using cached file handles
func (c *DiskCache[V]) readValueFromDisk(entry *keyEntry) ([]byte, error) {
	cachedFile, err := c.getCachedFile(entry.fileID)
	if err != nil {
		return nil, err
	}

	// Lock the cached file for reading
	cachedFile.mutex.RLock()
	defer cachedFile.mutex.RUnlock()

	// Seek to the entry position
	if _, err := cachedFile.file.Seek(entry.offset, 0); err != nil {
		return nil, err
	}

	// Create a new buffered reader for this read operation to avoid race conditions
	// with shared reader state
	reader := bufio.NewReader(cachedFile.file)

	logEntry, _, err := c.readLogEntry(reader)
	if err != nil {
		return nil, err
	}

	if logEntry.deleted {
		return nil, ErrKeyNotFound
	}

	return logEntry.value, nil
}

// getCachedFile returns a cached file handle for the given file ID
func (c *DiskCache[V]) getCachedFile(fileID uint32) (*cachedFile, error) {
	c.fileCacheMutex.RLock()
	cached, exists := c.fileCache[fileID]
	c.fileCacheMutex.RUnlock()

	if exists {
		return cached, nil
	}

	// File not in cache, need to open it
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

	cached = &cachedFile{
		file:   file,
		reader: bufio.NewReader(file),
		mutex:  sync.RWMutex{},
	}

	c.fileCache[fileID] = cached
	return cached, nil
}

// removeCachedFile removes a file from the cache and closes it
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

	// Close the file
	err := cached.file.Close()

	// Remove from cache
	delete(c.fileCache, fileID)

	return err
}

// readLogEntry reads a complete log entry from a reader
func (c *DiskCache[V]) readLogEntry(reader *bufio.Reader) (*logEntry, int, error) {
	// Read header
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, 0, err
	}

	entry := &logEntry{
		crc:       binary.LittleEndian.Uint32(header[0:4]),
		timestamp: binary.LittleEndian.Uint32(header[4:8]),
		keySize:   binary.LittleEndian.Uint32(header[8:12]),
		valueSize: binary.LittleEndian.Uint32(header[12:16]),
		deleted:   header[16] == 1,
	}

	// Read key
	entry.key = make([]byte, entry.keySize)
	if _, err := io.ReadFull(reader, entry.key); err != nil {
		return nil, 0, err
	}

	// Read value (if not deleted)
	if !entry.deleted && entry.valueSize > 0 {
		entry.value = make([]byte, entry.valueSize)
		if _, err := io.ReadFull(reader, entry.value); err != nil {
			return nil, 0, err
		}
	}

	// Verify CRC
	expectedCRC := c.calculateCRC(entry)
	if entry.crc != expectedCRC {
		return nil, 0, fmt.Errorf("CRC mismatch: expected %d, got %d", expectedCRC, entry.crc)
	}

	entrySize := headerSize + int(entry.keySize) + int(entry.valueSize)
	return entry, entrySize, nil
}

// calculateCRC calculates the CRC32 checksum for a log entry
func (c *DiskCache[V]) calculateCRC(entry *logEntry) uint32 {
	crc := crc32.NewIEEE()

	// Include timestamp, key size, value size, and deleted flag
	binary.Write(crc, binary.LittleEndian, entry.timestamp)
	binary.Write(crc, binary.LittleEndian, entry.keySize)
	binary.Write(crc, binary.LittleEndian, entry.valueSize)
	if entry.deleted {
		crc.Write([]byte{1})
	} else {
		crc.Write([]byte{0})
	}

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
		}
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
		}
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
