package bitcache

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// GCConfig holds configuration for garbage collection compaction
type GCConfig struct {
	// DeadRatioThreshold is the minimum ratio of dead data (0.0-1.0) to trigger GC
	// Default: 0.4 (40% dead data)
	DeadRatioThreshold float64

	// ScanInterval is how often to scan a segment
	// Default: 5 minutes
	ScanInterval time.Duration

	// MaxBytesPerSecond limits the rate of GC to avoid performance impact
	// Default: 5MB/s
	MaxBytesPerSecond int64

	// MinSegmentAge is the minimum age of a segment before considering it for GC
	// Default: 10 minutes (avoids GC on recent segments)
	MinSegmentAge time.Duration
}

// DefaultGCConfig returns the default GC configuration
func DefaultGCConfig() GCConfig {
	return GCConfig{
		DeadRatioThreshold: 0.4,
		ScanInterval:       5 * time.Minute,
		MaxBytesPerSecond:  5 * 1024 * 1024, // 5MB/s
		MinSegmentAge:      10 * time.Minute,
	}
}

// scanSegmentForGC scans a segment to calculate live vs dead data
func (c *DiskCache) scanSegmentForGC(fileID uint32, segmentPath string) (SegmentGCInfo, error) {
	file, err := os.Open(segmentPath)
	if err != nil {
		return SegmentGCInfo{}, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return SegmentGCInfo{}, err
	}

	stats := SegmentGCInfo{
		FileID:      fileID,
		TotalBytes:  fileInfo.Size(),
		LastScanned: time.Now(),
	}

	// Skip file header
	if _, err := file.Seek(fileHeaderSize, io.SeekStart); err != nil {
		return SegmentGCInfo{}, err
	}

	// Get current keydir for checking if entries are live
	keydir := c.keydir.Load()
	if keydir == nil {
		return stats, nil
	}

	// Reuse a single entry for all reads to avoid allocations
	entry := &logEntry{}

	// Scan all entries in the segment
	offset := int64(fileHeaderSize)
	for {
		var err error
		entry, entrySize, err := c.readLogEntryAt(entry, file, offset, true) // skipValue = true
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip corrupted entries
			break
		}

		// Check if this entry is still live in the keydir
		keyStr := string(entry.key)
		if keyEntry, exists := keydir.Get(keyStr); exists {
			// Entry is live if it points to this file and offset
			if keyEntry.fileID == fileID && keyEntry.offset == offset {
				stats.LiveBytes += entrySize
				stats.LiveEntries++
			} else {
				// Entry has been updated/moved elsewhere
				stats.DeadBytes += entrySize
				stats.DeadEntries++
			}
		} else {
			// Key not in keydir anymore (deleted)
			stats.DeadBytes += entrySize
			stats.DeadEntries++
		}

		offset += entrySize
	}

	// Calculate dead ratio
	if stats.TotalBytes > 0 {
		stats.DeadRatio = float64(stats.DeadBytes) / float64(stats.TotalBytes)
	}

	stats.NeedsGC = stats.DeadBytes > 0

	return stats, nil
}

// getAllSegmentFiles returns all segment file paths
func (c *DiskCache) getAllSegmentFiles() ([]string, error) {
	pattern := filepath.Join(c.dir, "*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return files, nil
}

// getSegmentPath returns the file path for a given file ID
func (c *DiskCache) getSegmentPath(fileID uint32) string {
	// Try LSM format first (check all levels)
	for level := uint8(0); level <= 4; level++ {
		path := filepath.Join(c.dir, fmt.Sprintf("%08d-%02d.log", fileID, level))
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	// Fall back to legacy format
	return filepath.Join(c.dir, fmt.Sprintf("%016d.log", fileID))
}

// parseFileIDFromPath extracts the file ID from a segment path
func parseFileIDFromPath(path string) (uint32, error) {
	base := filepath.Base(path)
	return parseFileID(base)
}

// readLogEntryAt reads a log entry at a specific offset and returns the entry and its size
// If entry is nil, a new one will be allocated. Otherwise, the provided entry is reused.
// If skipValue is true, the value field is not read (saves memory for scans that only need keys)
func (c *DiskCache) readLogEntryAt(entry *logEntry, file *os.File, offset int64, skipValue bool) (*logEntry, int64, error) {
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, err
	}

	// Read header
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, 0, err
	}

	// Allocate entry if not provided
	if entry == nil {
		entry = &logEntry{}
	}

	// Parse header
	entry.crc = binary.LittleEndian.Uint32(header[0:4])
	entry.timestamp = binary.LittleEndian.Uint32(header[4:8])
	entry.keySize = binary.LittleEndian.Uint32(header[8:12])
	entry.valueSize = binary.LittleEndian.Uint32(header[12:16])
	entry.deleted = header[16] == 1

	// Reuse or allocate key buffer
	if cap(entry.key) < int(entry.keySize) {
		entry.key = make([]byte, entry.keySize)
	} else {
		entry.key = entry.key[:entry.keySize]
	}

	// Read key
	if _, err := io.ReadFull(file, entry.key); err != nil {
		return nil, 0, err
	}

	// Read or skip value
	if skipValue {
		// Skip value bytes without reading into memory
		if entry.valueSize > 0 {
			if _, err := file.Seek(int64(entry.valueSize), io.SeekCurrent); err != nil {
				return nil, 0, err
			}
		}
		entry.value = nil
	} else {
		// Reuse or allocate value buffer
		if cap(entry.value) < int(entry.valueSize) {
			entry.value = make([]byte, entry.valueSize)
		} else {
			entry.value = entry.value[:entry.valueSize]
		}

		// Read value
		if _, err := io.ReadFull(file, entry.value); err != nil {
			return nil, 0, err
		}
	}

	entrySize := int64(headerSize + entry.keySize + entry.valueSize)
	return entry, entrySize, nil
}

// GetGCStats returns statistics about garbage collection
// Since background GC has been removed (GC is now handled via Compact),
// this returns the current segment statistics by scanning all segments
func (c *DiskCache) GetGCStats() GCStats {
	segments, err := c.scanAllSegments()
	if err != nil {
		return GCStats{}
	}

	return GCStats{
		Running:      false, // Background GC is no longer supported
		SegmentStats: segments,
	}
}

// scanAllSegments scans all segments and returns their GC statistics
// This is used internally by GetGCStats
func (c *DiskCache) scanAllSegments() ([]SegmentGCInfo, error) {
	if c.isClosed() {
		return nil, ErrCacheClosed
	}

	// Get all segment files
	files, err := c.getAllSegmentFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to get segment files: %w", err)
	}

	var results []SegmentGCInfo

	for _, segmentPath := range files {
		fileID, err := parseFileIDFromPath(segmentPath)
		if err != nil {
			continue // Skip files we can't parse
		}

		// Skip active segment
		if fileID == c.activeFileID {
			continue
		}

		// Scan this segment
		stats, err := c.scanSegmentForGC(fileID, segmentPath)
		if err != nil {
			// Skip segments we can't scan
			continue
		}

		results = append(results, stats)
	}

	return results, nil
}

// GCStats holds statistics about garbage collection
type GCStats struct {
	Running         bool
	LastScanTime    time.Time
	BytesProcessed  int64
	SegmentsScanned int
	SegmentsGCed    int
	SegmentStats    []SegmentGCInfo
}

// SegmentGCInfo holds information about a segment for GC
type SegmentGCInfo struct {
	FileID      uint32
	TotalBytes  int64
	LiveBytes   int64
	DeadBytes   int64
	LiveEntries int
	DeadEntries int
	DeadRatio   float64
	NeedsGC     bool
	LastScanned time.Time
}
