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
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-immutable-radix"
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
//     lookups. We store it in an immutable radix tree (github.com/hashicorp/go-immutable-radix)
//     which enables efficient prefix sharing, low memory overhead, and lock-free
//     reads by atomically swapping the tree root on updates.
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
//     radix tree pointer is updated atomically for mostly lock-free reads. File
//     handles for historical segments are cached with per-file RW locks to avoid
//     repeated open/close costs under read load.
//
// The structure implements the Cache interface (see cache.go) and is intended
// for workloads where the full key index fits in memory and fast point lookups
// are required, while values are stored on disk.
type DiskCache struct {
	mu              sync.RWMutex
	dir             string
	activeFile      *os.File
	activeWriter    *bufio.Writer
	activeFileID    uint32
	activeOffset    int64
	keydir          atomic.Pointer[iradix.Tree]
	stats           Stats
	closed          bool
	compactionMutex sync.Mutex
	lastCompaction  time.Time
	// File caching for efficient reads
	fileCache      map[uint32]*cachedFile
	fileCacheMutex sync.RWMutex
	// Configuration
	maxSegmentSize int64
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

// NewDiskCache creates a new bitcask cache in the specified directory
func NewDiskCache(dir string) (*DiskCache, error) {
	return NewDiskCacheWithConfig(dir, DiskCacheConfig{})
}

// NewDiskCacheWithConfig creates a new bitcask cache with custom configuration
func NewDiskCacheWithConfig(dir string, config DiskCacheConfig) (*DiskCache, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Set default segment size if not specified
	maxSegSize := config.MaxSegmentSize
	if maxSegSize <= 0 {
		maxSegSize = maxFileSize
	}

	cache := &DiskCache{
		dir: dir,
		// Initialize the file cache
		fileCache:      make(map[uint32]*cachedFile),
		maxSegmentSize: maxSegSize,
	}

	// Initialize the keydir with an empty iradix tree
	cache.keydir.Store(iradix.New())

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

// Get retrieves the value for the given key
func (c *DiskCache) Get(key []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return nil, ErrCacheClosed
	}

	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	if !exists || entry.deleted {
		return nil, ErrKeyNotFound
	}

	// If reading from the active file, check if the data might still be buffered
	// Only flush if the entry location + size extends beyond what's been written to disk
	needsFlush := false
	if entry.fileID == c.activeFileID && c.activeWriter != nil {
		entryEnd := entry.offset + int64(entry.size)
		bufferedBytes := int64(c.activeWriter.Buffered())

		// If the entry extends into the buffered region, we need to flush
		if entryEnd > c.activeOffset-bufferedBytes {
			needsFlush = true
		}
	}

	// Upgrade to write lock if we need to flush
	if needsFlush {
		c.mu.RUnlock()
		c.mu.Lock()
		// Flush the active writer
		if c.activeWriter != nil {
			if err := c.activeWriter.Flush(); err != nil {
				c.mu.Unlock()
				return nil, fmt.Errorf("failed to flush active writer: %w", err)
			}
		}
		c.mu.Unlock()
		c.mu.RLock()
	}

	atomic.AddInt64(&c.stats.Reads, 1)

	// Read the value from disk
	value, err := c.readValueFromDisk(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return value, nil
}

// Set stores a key-value pair in the cache
func (c *DiskCache) Set(key []byte, value []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrCacheClosed
	}

	entry := &logEntry{
		timestamp: uint32(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
		key:       key,
		value:     value,
		deleted:   false,
	}

	// Calculate CRC
	entry.crc = c.calculateCRC(entry)

	// Write to log file
	offset, err := c.writeLogEntry(entry)
	if err != nil {
		return fmt.Errorf("failed to write log entry: %w", err)
	}

	// Update keydir
	keyStr := string(key)
	oldEntry, _ := c.getKeyEntry(keyStr)

	c.setKeyEntry(keyStr, &keyEntry{
		fileID:    c.activeFileID,
		offset:    offset,
		size:      headerSize + entry.keySize + entry.valueSize,
		timestamp: entry.timestamp,
		deleted:   false,
	})

	// Update stats
	if oldEntry == nil || oldEntry.deleted {
		atomic.AddInt64(&c.stats.Keys, 1)
	}
	atomic.AddInt64(&c.stats.Writes, 1)
	atomic.AddInt64(&c.stats.DataSize, int64(headerSize+entry.keySize+entry.valueSize))

	return nil
}

// Delete removes a key from the cache
func (c *DiskCache) Delete(key []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrCacheClosed
	}

	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	if !exists || entry.deleted {
		return ErrKeyNotFound
	}

	// Write delete record
	logEntry := &logEntry{
		timestamp: uint32(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: 0,
		key:       key,
		value:     nil,
		deleted:   true,
	}
	logEntry.crc = c.calculateCRC(logEntry)

	offset, err := c.writeLogEntry(logEntry)
	if err != nil {
		return fmt.Errorf("failed to write delete record: %w", err)
	}

	// Update keydir
	c.setKeyEntry(keyStr, &keyEntry{
		fileID:    c.activeFileID,
		offset:    offset,
		size:      headerSize + logEntry.keySize,
		timestamp: logEntry.timestamp,
		deleted:   true,
	})

	// Update stats
	atomic.AddInt64(&c.stats.Keys, -1)
	atomic.AddInt64(&c.stats.Deletes, 1)
	atomic.AddInt64(&c.stats.DataSize, int64(headerSize+logEntry.keySize))

	return nil
}

// Has checks if a key exists in the cache
func (c *DiskCache) Has(key []byte) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		return false
	}

	keyStr := string(key)
	entry, exists := c.getKeyEntry(keyStr)
	return exists && !entry.deleted
}

// Close closes the cache and flushes any pending writes
func (c *DiskCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

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
func (c *DiskCache) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
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
func (c *DiskCache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stats := c.stats
	// Calculate index size by walking the iradix tree
	tree := c.keydir.Load()
	if tree != nil {
		count := 0
		tree.Root().Walk(func(k []byte, v interface{}) bool {
			count++
			return false
		})
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
func (c *DiskCache) Scan(prefix []byte, fn func(key []byte) bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	tree := c.keydir.Load()
	if tree == nil {
		return nil
	}

	// If no prefix, iterate all keys
	if len(prefix) == 0 {
		tree.Root().Walk(func(k []byte, v interface{}) bool {
			entry := v.(*keyEntry)
			// Skip deleted entries - continue iteration without calling fn
			if entry.deleted {
				return false // continue to next entry
			}
			return fn(k) // call fn and use its return value
		})
		return nil
	}

	// Use prefix-based iteration
	tree.Root().WalkPrefix(prefix, func(k []byte, v interface{}) bool {
		entry := v.(*keyEntry)
		// Skip deleted entries - continue iteration without calling fn
		if entry.deleted {
			return false // continue to next entry
		}
		return fn(k) // call fn and use its return value
	})

	return nil
}

// loadFromDisk loads existing data files and rebuilds the keydir
func (c *DiskCache) loadFromDisk() error {
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
func (c *DiskCache) loadFromSegmentFiles() error {
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
		// Extract file ID from filename
		base := filepath.Base(filename)
		var fileID uint32
		if _, err := fmt.Sscanf(base, "%016d.log", &fileID); err != nil {
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
func (c *DiskCache) loadSegmentFile(filename string, fileID uint32) error {
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
func (c *DiskCache) readFileHeader(file *os.File) (*fileHeader, error) {
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
func (c *DiskCache) writeFileHeader(file *os.File, header *fileHeader) error {
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
func (c *DiskCache) loadHintsFromSegment(file *os.File, hintOffset int64, fileID uint32) error {
	// Seek to hint section
	if _, err := file.Seek(hintOffset, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	hintsLoaded := 0
	hintErrors := 0

	for {
		entry, err := c.readHintEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			hintErrors++
			// If we get errors reading hints, return error to trigger fallback to scanning
			if hintsLoaded == 0 {
				// No hints loaded yet, hints section is likely corrupted from the start
				return fmt.Errorf("hints corrupted, falling back to scan: %w", err)
			}
			// We loaded some hints but then hit corruption, stop here
			fmt.Printf("Warning: hints partially corrupted in segment %d, loaded %d entries\n", fileID, hintsLoaded)
			break
		}

		keyStr := string(entry.key)
		oldEntry, existed := c.getKeyEntry(keyStr)

		// Hints don't contain deletion status, so we assume non-deleted
		// Deleted entries should be filtered out during compaction
		c.setKeyEntry(keyStr, &keyEntry{
			fileID:    fileID,
			offset:    entry.offset,
			size:      headerSize + entry.keySize + entry.valueSize,
			timestamp: entry.timestamp,
			deleted:   false,
		})

		// Only increment Keys counter for new keys, not overwrites
		if !existed || (oldEntry != nil && oldEntry.deleted) {
			atomic.AddInt64(&c.stats.Keys, 1)
		}
		atomic.AddInt64(&c.stats.DataSize, int64(entry.valueSize+headerSize))
		hintsLoaded++
	}

	return nil
}

// scanSegmentFile scans a segment file to build the keydir (fallback when no hints)
func (c *DiskCache) scanSegmentFile(file *os.File, fileID uint32) error {
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
func (c *DiskCache) tryFindNextRecord(file *os.File, currentOffset int64) int64 {
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
func (c *DiskCache) validateRecordAtOffset(file *os.File, offset int64, fileSize int64) bool {
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
func (c *DiskCache) openLogFile() error {
	if err := os.MkdirAll(c.dir, 0755); err != nil {
		return err
	}

	filename := filepath.Join(c.dir, fmt.Sprintf("%016d.log", c.activeFileID))

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
func (c *DiskCache) writeLogEntry(entry *logEntry) (int64, error) {
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
func (c *DiskCache) rotateLogFile() error {
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
func (c *DiskCache) writeHintsToActiveFile(fileID uint32, hintOffset int64) error {
	// Write all hints for this file to the active writer
	tree := c.keydir.Load()
	if tree != nil {
		tree.Root().Walk(func(k []byte, v interface{}) bool {
			entry := v.(*keyEntry)

			// Only write hints for this specific file, and skip deleted entries
			if entry.fileID != fileID {
				return false // continue walking
			}

			// Skip deleted entries - they shouldn't be in hints
			if entry.deleted {
				return false // continue walking
			}

			keyStr := string(k)
			hintEntry := &hintEntry{
				timestamp: entry.timestamp,
				keySize:   uint32(len(keyStr)),
				valueSize: uint32(entry.size - uint32(headerSize) - uint32(len(keyStr))),
				offset:    entry.offset,
				key:       []byte(keyStr),
			}

			if err := c.writeHintEntry(c.activeWriter, hintEntry); err != nil {
				return true // stop walking on error
			}
			return false // continue walking
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
func (c *DiskCache) loadLogFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Extract file ID from filename
	base := filepath.Base(filename)
	var fileID uint32
	if _, err := fmt.Sscanf(base, "%016d.log", &fileID); err != nil {
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
func (c *DiskCache) compactSegment(fileID uint32) error {
	filename := filepath.Join(c.dir, fmt.Sprintf("%016d.log", fileID))
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open segment file %s: %w", filename, err)
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
			// This is the latest version, write it to the active file using Set
			// We need to unlock the compaction mutex temporarily to avoid deadlock
			// since Set also needs to take the write lock
			c.compactionMutex.Unlock()
			err := c.Set(entry.key, entry.value)
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
func (c *DiskCache) readValueFromDisk(entry *keyEntry) ([]byte, error) {
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
func (c *DiskCache) getCachedFile(fileID uint32) (*cachedFile, error) {
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

	// Open the file
	filename := filepath.Join(c.dir, fmt.Sprintf("%016d.log", fileID))
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
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
func (c *DiskCache) removeCachedFile(fileID uint32) error {
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
func (c *DiskCache) readLogEntry(reader *bufio.Reader) (*logEntry, int, error) {
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
func (c *DiskCache) calculateCRC(entry *logEntry) uint32 {
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

// Compact compacts the cache by merging segments and removing deleted keys
func (c *DiskCache) Compact() error {
	return c.CompactN(0)
}

// CompactN compacts up to N oldest segments. If count is 0 or negative, all segments are compacted.
func (c *DiskCache) CompactN(count int) error {
	c.compactionMutex.Lock()
	defer c.compactionMutex.Unlock()

	if c.closed {
		return ErrCacheClosed
	}

	// Check if compaction is needed based on the time since last compaction
	if time.Since(c.lastCompaction) < minCompactionInterval {
		return nil
	}

	// Identify segments to compact (exclude active file)
	segments, err := c.getSegments()
	if err != nil {
		return fmt.Errorf("failed to get segments: %w", err)
	}

	// Skip compaction if there are no segments to compact
	if len(segments) == 0 {
		return nil
	}

	// Sort segments by file ID to process oldest first
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].fileID < segments[j].fileID
	})

	// Limit the number of segments to compact if count is specified
	if count > 0 && count < len(segments) {
		segments = segments[:count]
	}

	// Process each segment starting with the oldest
	for _, segment := range segments {
		if err := c.compactSegment(segment.fileID); err != nil {
			return fmt.Errorf("failed to compact segment %d: %w", segment.fileID, err)
		}
	}

	// Update last compaction time
	c.lastCompaction = time.Now()

	return nil
}

// getSegments retrieves the list of segments (log files) to be compacted
func (c *DiskCache) getSegments() ([]*keyEntry, error) {
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
func (c *DiskCache) getFileID(filename string) (uint32, error) {
	base := filepath.Base(filename)
	ext := filepath.Ext(base)
	name := base[0 : len(base)-len(ext)]

	fileID, err := strconv.ParseUint(name, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid file ID in log file name: %s", filename)
	}

	return uint32(fileID), nil
}

// readHintEntry reads a hint entry from the hint data
func (c *DiskCache) readHintEntry(reader *bufio.Reader) (*hintEntry, error) {
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
func (c *DiskCache) writeHintEntry(writer *bufio.Writer, entry *hintEntry) error {
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
func (c *DiskCache) getKeyEntry(key string) (*keyEntry, bool) {
	tree := c.keydir.Load()
	if tree == nil {
		return nil, false
	}

	node, ok := tree.Get([]byte(key))
	if !ok {
		return nil, false
	}

	entry, ok := node.(*keyEntry)
	if !ok {
		return nil, false
	}

	return entry, true
}

// setKeyEntry sets a keyEntry in the key directory
func (c *DiskCache) setKeyEntry(key string, entry *keyEntry) {
	for {
		tree := c.keydir.Load()
		if tree == nil {
			// Create a new tree if none exists
			newTree := iradix.New()
			newTree, _, _ = newTree.Insert([]byte(key), entry)
			if c.keydir.CompareAndSwap(tree, newTree) {
				return
			}
		} else {
			// Update existing tree
			newTree, _, _ := tree.Insert([]byte(key), entry)
			if c.keydir.CompareAndSwap(tree, newTree) {
				return
			}
		}
	}
}
