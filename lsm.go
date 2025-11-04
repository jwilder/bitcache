package bitcache

// This file contains LSM-style compaction implementation
// It implements generation-level based segment naming and hierarchical compaction

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// CompactionResult contains information about a completed compaction
type CompactionResult struct {
	// Type of compaction performed ("LSM", "GC", "pruning", or "none")
	Type string
	// Level that was compacted (for LSM)
	Level uint8
	// Input segments that were compacted
	InputSegments []string
	// Output segment created
	OutputSegment string
	// Number of live entries written to output
	LiveEntries int
	// Total bytes written to output
	BytesWritten int64
	// Segments deleted after compaction
	DeletedSegments []string
}

// segmentID represents a segment file identifier with generation and level
// Generation increments on each rotation, level indicates compaction depth (0-4)
type segmentID struct {
	generation uint32
	level      uint8 // 0 = original writes, 1-4 = compaction levels
}

// String returns the filename for this segment
func (s segmentID) String() string {
	return fmt.Sprintf("%08d-%02d.log", s.generation, s.level)
}

// Less returns true if this segment comes before another in sort order
func (s segmentID) Less(other segmentID) bool {
	if s.generation != other.generation {
		return s.generation < other.generation
	}
	return s.level < other.level
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

// segmentInfo tracks metadata about a segment for LSM compaction
type segmentInfo struct {
	id         segmentID
	path       string
	liveKeys   int64
	liveBytes  int64
	totalBytes int64
}

// LSMCompactionConfig holds LSM-style compaction configuration
type LSMCompactionConfig struct {
	MaxLevels           int     // Maximum compaction levels (default: 4)
	L0CompactionTrigger int     // Number of L0 segments before compaction (default: 4)
	L4GCFragmentation   float64 // L4 fragmentation threshold for GC (default: 0.40)
}

// DefaultLSMCompactionConfig returns default LSM compaction settings
func DefaultLSMCompactionConfig() LSMCompactionConfig {
	return LSMCompactionConfig{
		MaxLevels:           4,
		L0CompactionTrigger: 4,
		L4GCFragmentation:   0.40,
	}
}

// getSegmentsByLevel returns segments grouped by level, sorted by generation within each level
// This uses in-memory segment tracking for efficiency instead of listing files from disk
func (c *DiskCache[V]) getSegmentsByLevel() (map[uint8][]*segmentInfo, error) {
	// Get active file ID to skip it
	c.mu.RLock()
	activeID := c.activeFileID
	c.mu.RUnlock()

	// Read from in-memory segment tracking
	c.segmentsMutex.RLock()
	defer c.segmentsMutex.RUnlock()

	byLevel := make(map[uint8][]*segmentInfo)

	for id, info := range c.segments {
		// Skip active segment (compare generation to activeFileID)
		if id.generation == activeID {
			continue
		}

		// Create a copy of the segment info to avoid data races
		infoCopy := &segmentInfo{
			id:         info.id,
			path:       info.path,
			liveKeys:   info.liveKeys,
			liveBytes:  info.liveBytes,
			totalBytes: info.totalBytes,
		}

		byLevel[id.level] = append(byLevel[id.level], infoCopy)
	}

	// Sort segments within each level by generation (oldest first)
	for _, segments := range byLevel {
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].id.Less(segments[j].id)
		})
	}

	return byLevel, nil
}

// shouldCompactLSM determines if LSM-style compaction is needed
func (c *DiskCache[V]) shouldCompactLSM(config LSMCompactionConfig) (level uint8, reason string, ok bool) {
	byLevel, err := c.getSegmentsByLevel()
	if err != nil || len(byLevel) == 0 {
		return 0, "", false
	}

	// Level 0: trigger based on count (like LSM)
	l0Count := len(byLevel[0])
	if l0Count >= config.L0CompactionTrigger {
		return 0, fmt.Sprintf("%d L0 segments (trigger: %d)", l0Count, config.L0CompactionTrigger), true
	}

	// Higher levels: trigger based on segment count
	for level := uint8(1); level < uint8(config.MaxLevels); level++ {
		segments := byLevel[level]
		if len(segments) == 0 {
			continue
		}

		// Compact if we have too many segments at this level
		if len(segments) >= 4 {
			return level, fmt.Sprintf("L%d has %d segments", level, len(segments)), true
		}
	}

	// L4 garbage collection check
	l4Segments := byLevel[4]
	if len(l4Segments) >= 2 {
		return 4, fmt.Sprintf("L4 has %d segments (GC candidate)", len(l4Segments)), true
	}

	return 0, "", false
}

// compactEntry holds an entry during compaction
type compactEntry struct {
	key       []byte
	value     []byte
	timestamp uint32
	offset    int64  // offset in source file
	fileID    uint32 // source file ID
}

// compactSegmentsToLevel compacts multiple segments into a single output segment
// Returns the number of live entries and bytes written
func (c *DiskCache[V]) compactSegmentsToLevel(segments []*segmentInfo, outputID segmentID) (int, int64, error) {
	outputPath := filepath.Join(c.dir, outputID.String())
	tmpPath := outputPath + ".tmp"

	// Open temporary output file
	outputFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create temporary output file: %w", err)
	}
	defer func() {
		outputFile.Close()
		// Clean up temporary file if we're returning with an error
		if err != nil {
			os.Remove(tmpPath)
		}
	}()

	// Write file header
	header := &fileHeader{hintOffset: 0}
	if err := c.writeFileHeader(outputFile, header); err != nil {
		return 0, 0, fmt.Errorf("failed to write header: %w", err)
	}

	// Collect all live entries from source segments
	entries := make(map[string]*compactEntry)

	for _, seg := range segments {
		// Open source file
		sourceFile, err := os.Open(seg.path)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to open %s: %w", seg.path, err)
		}

		// Read all entries from this segment
		if err := c.readSegmentEntries(sourceFile, seg.id, entries); err != nil {
			sourceFile.Close()
			return 0, 0, fmt.Errorf("failed to read entries from %s: %w", seg.path, err)
		}
		sourceFile.Close()
	}

	// Write all entries to output file
	bytesWritten, err := c.writeCompactedEntries(outputFile, outputID, entries)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to write entries: %w", err)
	}

	// Fsync the temporary file to ensure it's durably written
	if err := outputFile.Sync(); err != nil {
		return 0, 0, fmt.Errorf("failed to fsync temporary output file: %w", err)
	}

	// Close the file before renaming
	if err := outputFile.Close(); err != nil {
		return 0, 0, fmt.Errorf("failed to close temporary output file: %w", err)
	}

	// Rename temporary file to final name (atomic operation)
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return 0, 0, fmt.Errorf("failed to rename temporary file to final output: %w", err)
	}

	return len(entries), bytesWritten, nil
}

// readSegmentEntries reads all live entries from a segment
func (c *DiskCache[V]) readSegmentEntries(file *os.File, segID segmentID, entries map[string]*compactEntry) error {
	// Read file header
	header, err := c.readFileHeader(file)
	if err != nil {
		return err
	}

	// Start after header
	if _, err := file.Seek(fileHeaderSize, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(file)
	offset := int64(fileHeaderSize)

	// Determine where to stop reading (before hints if they exist)
	stat, _ := file.Stat()
	endOffset := stat.Size()
	if header.hintOffset > 0 {
		endOffset = header.hintOffset
	}

	for offset < endOffset {
		entry, entrySize, err := c.readLogEntry(reader)
		if err != nil {
			break // EOF or error
		}

		if entry.deleted {
			c.releaseLogEntry(entry)
			offset += int64(entrySize)
			continue
		}

		keyStr := string(entry.key)

		// Check if this is still the current version in keydir
		currentEntry, exists := c.getKeyEntry(keyStr)
		if exists &&
			currentEntry.fileID == segID.generation &&
			currentEntry.offset == offset &&
			!currentEntry.deleted {
			// This is still the live version - make copies since we're releasing the entry
			keyCopy := make([]byte, len(entry.key))
			copy(keyCopy, entry.key)
			valueCopy := make([]byte, len(entry.value))
			copy(valueCopy, entry.value)

			entries[keyStr] = &compactEntry{
				key:       keyCopy,
				value:     valueCopy,
				timestamp: entry.timestamp,
				offset:    offset,
				fileID:    segID.generation,
			}
		}

		c.releaseLogEntry(entry)
		offset += int64(entrySize)
	}

	return nil
}

// writeCompactedEntries writes all entries to the output file and updates keydir
// Returns the total bytes written
func (c *DiskCache[V]) writeCompactedEntries(file *os.File, outputID segmentID, entries map[string]*compactEntry) (int64, error) {
	writer := bufio.NewWriter(file)
	offset := int64(fileHeaderSize)

	// Sort keys for deterministic output
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write each entry
	for _, key := range keys {
		entry := entries[key]

		logEntry := &logEntry{
			timestamp: entry.timestamp,
			keySize:   uint32(len(entry.key)),
			valueSize: uint32(len(entry.value)),
			key:       entry.key,
			value:     entry.value,
			deleted:   false,
		}
		logEntry.crc = c.calculateCRC(logEntry)

		// Write entry
		if err := c.writeLogEntryToFile(writer, logEntry); err != nil {
			return 0, err
		}

		entrySize := headerSize + int64(logEntry.keySize) + int64(logEntry.valueSize)

		// Update keydir to point to new location
		c.setKeyEntry(key, &keyEntry{
			fileID:    outputID.generation,
			offset:    offset,
			size:      uint32(entrySize),
			timestamp: entry.timestamp,
			deleted:   false,
		})

		offset += entrySize
	}

	// Flush writer
	if err := writer.Flush(); err != nil {
		return 0, err
	}

	// Write hints at the end
	hintOffset := offset
	for _, key := range keys {
		entry := entries[key]

		// Get the updated keydir entry (has new offset)
		kdEntry, _ := c.getKeyEntry(key)

		hint := &hintEntry{
			timestamp: entry.timestamp,
			keySize:   uint32(len(entry.key)),
			valueSize: uint32(len(entry.value)),
			offset:    kdEntry.offset,
			key:       entry.key,
		}

		if err := c.writeHintEntry(writer, hint); err != nil {
			return 0, err
		}
	}

	if err := writer.Flush(); err != nil {
		return 0, err
	}

	// Update header with hint offset
	header := &fileHeader{hintOffset: hintOffset}
	if err := c.writeFileHeader(file, header); err != nil {
		return 0, err
	}

	// Note: Sync is handled by the caller after writing is complete
	return offset, nil
}

// writeLogEntryToFile writes a log entry to a buffered writer
func (c *DiskCache[V]) writeLogEntryToFile(writer *bufio.Writer, entry *logEntry) error {
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

	if _, err := writer.Write(header); err != nil {
		return err
	}

	// Write key
	if _, err := writer.Write(entry.key); err != nil {
		return err
	}

	// Write value
	if !entry.deleted && entry.valueSize > 0 {
		if _, err := writer.Write(entry.value); err != nil {
			return err
		}
	}

	return nil
}

// SegmentInfo is the exported version of segmentInfo for external tools
type SegmentInfo struct {
	ID         SegmentID
	Path       string
	LiveKeys   int64
	LiveBytes  int64
	TotalBytes int64
}

// SegmentID is the exported version of segmentID for external tools
type SegmentID struct {
	Generation uint32
	Level      uint8
}

// String returns the filename for this segment
func (s SegmentID) String() string {
	return fmt.Sprintf("%08d-%02d.log", s.Generation, s.Level)
}

// GetSegmentsByLevel returns segments grouped by level (exported version)
func (c *DiskCache[V]) GetSegmentsByLevel() (map[uint8][]*SegmentInfo, error) {
	internal, err := c.getSegmentsByLevel()
	if err != nil {
		return nil, err
	}

	// Convert to exported type
	result := make(map[uint8][]*SegmentInfo)
	for level, segments := range internal {
		exported := make([]*SegmentInfo, len(segments))
		for i, seg := range segments {
			exported[i] = &SegmentInfo{
				ID: SegmentID{
					Generation: seg.id.generation,
					Level:      seg.id.level,
				},
				Path:       seg.path,
				LiveKeys:   seg.liveKeys,
				LiveBytes:  seg.liveBytes,
				TotalBytes: seg.totalBytes,
			}
		}
		result[level] = exported
	}

	return result, nil
}
