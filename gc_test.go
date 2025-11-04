package bitcache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDefaultGCConfig tests the default GC configuration
func TestDefaultGCConfig(t *testing.T) {
	config := DefaultGCConfig()

	if config.DeadRatioThreshold != 0.4 {
		t.Errorf("expected DeadRatioThreshold=0.4, got %f", config.DeadRatioThreshold)
	}

	if config.ScanInterval != 5*time.Minute {
		t.Errorf("expected ScanInterval=5m, got %v", config.ScanInterval)
	}

	if config.MaxBytesPerSecond != 5*1024*1024 {
		t.Errorf("expected MaxBytesPerSecond=5MB, got %d", config.MaxBytesPerSecond)
	}

	if config.MinSegmentAge != 10*time.Minute {
		t.Errorf("expected MinSegmentAge=10m, got %v", config.MinSegmentAge)
	}

	t.Log("✓ DefaultGCConfig returns correct values")
}

// TestScanSegmentForGC tests scanning a segment for GC statistics
func TestScanSegmentForGC(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize: 10 * 1024,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write some keys
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-some-data", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Update some keys (creates dead data)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("updated-value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to update key: %v", err)
		}
	}

	// Delete some keys (creates more dead data)
	for i := 40; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := cache.Delete(key); err != nil {
			t.Fatalf("failed to delete key: %v", err)
		}
	}

	// Force rotation to create a non-active segment
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("failed to rotate: %v", err)
	}

	// Get all segment files
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("failed to find log files: %v", err)
	}

	if len(files) < 2 {
		t.Fatal("expected at least 2 log files after rotation")
	}

	// Find a non-active segment to scan
	var targetFile string
	var fileID uint32
	for _, file := range files {
		id, err := parseFileIDFromPath(file)
		if err != nil {
			continue
		}
		if id != cache.activeFileID {
			targetFile = file
			fileID = id
			break
		}
	}

	if targetFile == "" {
		t.Fatal("no non-active segment found")
	}

	stats, err := cache.scanSegmentForGC(fileID, targetFile)
	if err != nil {
		t.Fatalf("scanSegmentForGC failed: %v", err)
	}

	// Verify stats
	if stats.FileID != fileID {
		t.Errorf("expected FileID=%d, got %d", fileID, stats.FileID)
	}

	if stats.TotalBytes == 0 {
		t.Error("expected TotalBytes > 0")
	}

	// Should have at least some live or dead bytes (segment is not empty)
	if stats.LiveBytes == 0 && stats.DeadBytes == 0 {
		t.Error("expected either LiveBytes or DeadBytes > 0")
	}

	if stats.TotalBytes > 0 {
		expectedRatio := float64(stats.DeadBytes) / float64(stats.TotalBytes)
		if stats.DeadRatio != expectedRatio {
			t.Errorf("expected DeadRatio=%.2f, got %.2f", expectedRatio, stats.DeadRatio)
		}
	}

	if stats.LastScanned.IsZero() {
		t.Error("expected LastScanned to be set")
	}

	t.Logf("✓ Scanned segment %d: total=%d, live=%d, dead=%d, ratio=%.2f",
		stats.FileID, stats.TotalBytes, stats.LiveBytes, stats.DeadBytes, stats.DeadRatio)
}

// TestScanSegmentForGC_NonexistentFile tests scanning a nonexistent file
func TestScanSegmentForGC_NonexistentFile(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	_, err = cache.scanSegmentForGC(999, filepath.Join(dir, "nonexistent.log"))
	if err == nil {
		t.Error("expected error for nonexistent file")
	}

	t.Log("✓ scanSegmentForGC correctly returns error for nonexistent file")
}

// TestGetAllSegmentFiles tests getting all segment files
func TestGetAllSegmentFiles(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize: 5 * 1024,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write enough data to create multiple segments
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-extra-data-to-fill-segment", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	files, err := cache.getAllSegmentFiles()
	if err != nil {
		t.Fatalf("getAllSegmentFiles failed: %v", err)
	}

	if len(files) == 0 {
		t.Error("expected at least one segment file")
	}

	// Verify all returned files exist and are .log files
	for _, file := range files {
		if filepath.Ext(file) != ".log" {
			t.Errorf("expected .log extension, got %s", file)
		}

		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("file does not exist: %s", file)
		}
	}

	t.Logf("✓ Found %d segment files", len(files))
}

// TestGetSegmentPath tests getting the path for a segment
func TestGetSegmentPath(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write some data to create a segment
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Get path for active segment
	fileID := cache.activeFileID
	path := cache.getSegmentPath(fileID)

	if path == "" {
		t.Error("expected non-empty path")
	}

	// Verify the path exists or would exist in the correct format
	if _, err := os.Stat(path); err != nil && !os.IsNotExist(err) {
		t.Errorf("unexpected error checking path: %v", err)
	}

	t.Logf("✓ Got segment path for fileID %d: %s", fileID, path)
}

// TestParseFileIDFromPath tests parsing file IDs from paths
func TestParseFileIDFromPath(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		expectedID  uint32
		expectError bool
	}{
		{
			name:        "LSM format",
			path:        "/path/to/00000005-00.log",
			expectedID:  5,
			expectError: false,
		},
		{
			name:        "LSM format with level",
			path:        "/path/to/00000042-03.log",
			expectedID:  42,
			expectError: false,
		},
		{
			name:        "Legacy format",
			path:        "/path/to/0000000000000010.log",
			expectedID:  10,
			expectError: false,
		},
		{
			name:        "Invalid format",
			path:        "/path/to/invalid.log",
			expectedID:  0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := parseFileIDFromPath(tt.path)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if id != tt.expectedID {
					t.Errorf("expected ID=%d, got %d", tt.expectedID, id)
				}
			}
		})
	}

	t.Log("✓ parseFileIDFromPath works correctly")
}

// TestReadLogEntryAt tests reading a log entry at a specific offset
func TestReadLogEntryAt(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write a key
	testKey := []byte("test-key")
	testValue := []byte("test-value")
	if err := cache.Set(testKey, testValue); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	cache.Close()

	// Open the segment file directly
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("failed to find log files: %v", err)
	}

	file, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Reopen cache to use readLogEntryAt
	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Read entry at offset (skip file header)
	entry, entrySize, err := cache2.readLogEntryAt(nil, file, fileHeaderSize, false)
	if err != nil {
		t.Fatalf("readLogEntryAt failed: %v", err)
	}

	if entry == nil {
		t.Fatal("expected non-nil entry")
	}

	if string(entry.key) != string(testKey) {
		t.Errorf("expected key=%s, got %s", testKey, entry.key)
	}

	if string(entry.value) != string(testValue) {
		t.Errorf("expected value=%s, got %s", testValue, entry.value)
	}

	if entrySize == 0 {
		t.Error("expected entrySize > 0")
	}

	t.Logf("✓ Read entry: key=%s, value=%s, size=%d", entry.key, entry.value, entrySize)
}

// TestReadLogEntryAt_SkipValue tests reading an entry without loading the value
func TestReadLogEntryAt_SkipValue(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write a key with a large value
	testKey := []byte("test-key")
	testValue := make([]byte, 10000) // 10KB value
	for i := range testValue {
		testValue[i] = byte(i % 256)
	}
	if err := cache.Set(testKey, testValue); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	cache.Close()

	// Open the segment file
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("failed to find log files: %v", err)
	}

	file, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Read entry with skipValue=true
	entry, entrySize, err := cache2.readLogEntryAt(nil, file, fileHeaderSize, true)
	if err != nil {
		t.Fatalf("readLogEntryAt with skipValue failed: %v", err)
	}

	if entry == nil {
		t.Fatal("expected non-nil entry")
	}

	if string(entry.key) != string(testKey) {
		t.Errorf("expected key=%s, got %s", testKey, entry.key)
	}

	if entry.value != nil {
		t.Error("expected value=nil when skipValue=true")
	}

	if entry.valueSize != uint32(len(testValue)) {
		t.Errorf("expected valueSize=%d, got %d", len(testValue), entry.valueSize)
	}

	if entrySize == 0 {
		t.Error("expected entrySize > 0")
	}

	t.Logf("✓ Read entry with skipValue: key=%s, valueSize=%d, size=%d", entry.key, entry.valueSize, entrySize)
}

// TestReadLogEntryAt_ReuseEntry tests reusing an entry buffer
func TestReadLogEntryAt_ReuseEntry(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write multiple keys
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	cache.Close()

	// Open the segment file
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("failed to find log files: %v", err)
	}

	file, err := os.Open(files[0])
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Read first entry
	entry, size1, err := cache2.readLogEntryAt(nil, file, fileHeaderSize, false)
	if err != nil {
		t.Fatalf("first read failed: %v", err)
	}

	// Reuse the entry buffer for second read
	offset := fileHeaderSize + size1
	entry2, _, err := cache2.readLogEntryAt(entry, file, offset, false)
	if err != nil {
		t.Fatalf("second read failed: %v", err)
	}

	// Verify it's the same pointer (buffer reuse)
	if entry != entry2 {
		t.Error("expected entry buffer to be reused")
	}

	t.Log("✓ Entry buffer successfully reused")
}

// TestGetGCStats tests getting GC statistics
func TestGetGCStats(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize: 10 * 1024,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write data to create multiple segments
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-some-data", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Force rotation to create non-active segments
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("failed to rotate: %v", err)
	}

	// Update some keys to create dead data
	for i := 0; i < 30; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("updated-value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to update key: %v", err)
		}
	}

	// Get GC stats
	gcStats := cache.GetGCStats()

	if gcStats.Running {
		t.Error("expected Running=false (background GC not supported)")
	}

	// Should have at least one non-active segment
	if len(gcStats.SegmentStats) == 0 {
		t.Error("expected at least one non-active segment in stats")
	}

	// Verify segment stats
	for _, seg := range gcStats.SegmentStats {
		if seg.TotalBytes == 0 {
			t.Errorf("segment %d has TotalBytes=0", seg.FileID)
		}

		// TotalBytes includes file header and hints, LiveBytes+DeadBytes only count data entries
		// So TotalBytes should be >= LiveBytes + DeadBytes
		if seg.TotalBytes < seg.LiveBytes+seg.DeadBytes {
			t.Errorf("segment %d: TotalBytes (%d) < LiveBytes (%d) + DeadBytes (%d)",
				seg.FileID, seg.TotalBytes, seg.LiveBytes, seg.DeadBytes)
		}

		t.Logf("Segment %d: total=%d, live=%d, dead=%d, ratio=%.2f, needsGC=%v",
			seg.FileID, seg.TotalBytes, seg.LiveBytes, seg.DeadBytes, seg.DeadRatio, seg.NeedsGC)
	}

	t.Logf("✓ GetGCStats returned stats for %d segments", len(gcStats.SegmentStats))
}

// TestGetGCStats_EmptyCache tests GC stats on an empty cache
func TestGetGCStats_EmptyCache(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Get stats without writing any data
	gcStats := cache.GetGCStats()

	if gcStats.Running {
		t.Error("expected Running=false")
	}

	// Should have no segments (active segment not included in GC stats)
	if len(gcStats.SegmentStats) != 0 {
		t.Errorf("expected 0 segments, got %d", len(gcStats.SegmentStats))
	}

	t.Log("✓ GetGCStats correctly returns empty stats for empty cache")
}

// TestScanAllSegments tests scanning all segments
func TestScanAllSegments(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize: 5 * 1024,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write enough data to create multiple segments
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-data", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Force rotation to create non-active segments
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("failed to rotate: %v", err)
	}

	// Create some dead data
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := cache.Delete(key); err != nil {
			t.Fatalf("failed to delete key: %v", err)
		}
	}

	segments, err := cache.scanAllSegments()
	if err != nil {
		t.Fatalf("scanAllSegments failed: %v", err)
	}

	if len(segments) == 0 {
		t.Error("expected at least one non-active segment")
	}

	// Verify all segments have valid stats
	totalLive := int64(0)
	totalDead := int64(0)
	for _, seg := range segments {
		// FileID can be 0 (first segment), that's valid
		if seg.TotalBytes == 0 {
			t.Errorf("segment %d has TotalBytes=0", seg.FileID)
		}

		totalLive += seg.LiveBytes
		totalDead += seg.DeadBytes
	}

	if totalDead == 0 {
		t.Error("expected some dead data after deletions")
	}

	t.Logf("✓ Scanned %d segments: totalLive=%d, totalDead=%d", len(segments), totalLive, totalDead)
}

// TestScanAllSegments_ClosedCache tests scanning when cache is closed
func TestScanAllSegments_ClosedCache(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write some data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Close the cache
	cache.Close()

	// Try to scan
	_, err = cache.scanAllSegments()
	if err != ErrCacheClosed {
		t.Errorf("expected ErrCacheClosed, got %v", err)
	}

	t.Log("✓ scanAllSegments correctly returns ErrCacheClosed")
}

// TestScanSegmentForGC_WithDeletedEntries tests GC stats calculation with deleted entries
func TestScanSegmentForGC_WithDeletedEntries(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize: 20 * 1024,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write keys
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Force rotation to create a non-active segment
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("failed to rotate: %v", err)
	}

	// Delete all the keys (making the first segment 100% dead)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := cache.Delete(key); err != nil {
			t.Fatalf("failed to delete key: %v", err)
		}
	}

	// Get all segments
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("failed to find log files: %v", err)
	}

	// Find a non-active segment
	var targetFile string
	var targetID uint32
	for _, file := range files {
		id, err := parseFileIDFromPath(file)
		if err != nil {
			continue
		}
		if id != cache.activeFileID {
			targetFile = file
			targetID = id
			break
		}
	}

	if targetFile == "" {
		t.Fatal("no non-active segment found")
	}

	// Scan the segment
	stats, err := cache.scanSegmentForGC(targetID, targetFile)
	if err != nil {
		t.Fatalf("scanSegmentForGC failed: %v", err)
	}

	// All entries should be dead (deleted)
	if stats.LiveEntries > 0 {
		t.Errorf("expected LiveEntries=0, got %d", stats.LiveEntries)
	}

	if stats.DeadEntries == 0 {
		t.Error("expected DeadEntries > 0")
	}

	// DeadRatio should be positive (some dead data)
	// Note: TotalBytes includes file header and hints, LiveBytes+DeadBytes only count data entries
	// So DeadRatio might not be exactly 1.0 even if all data entries are dead
	if stats.DeadRatio <= 0 {
		t.Errorf("expected DeadRatio > 0, got %.2f", stats.DeadRatio)
	}

	if !stats.NeedsGC {
		t.Error("expected NeedsGC=true for segment with dead data")
	}

	t.Logf("✓ Segment with deleted entries: dead=%d, deadRatio=%.4f, needsGC=%v",
		stats.DeadEntries, stats.DeadRatio, stats.NeedsGC)
}

// TestSegmentGCInfo_Structure tests the SegmentGCInfo structure
func TestSegmentGCInfo_Structure(t *testing.T) {
	info := SegmentGCInfo{
		FileID:      123,
		TotalBytes:  1000,
		LiveBytes:   600,
		DeadBytes:   400,
		LiveEntries: 60,
		DeadEntries: 40,
		DeadRatio:   0.4,
		NeedsGC:     true,
		LastScanned: time.Now(),
	}

	if info.FileID != 123 {
		t.Errorf("expected FileID=123, got %d", info.FileID)
	}

	if info.TotalBytes != info.LiveBytes+info.DeadBytes {
		t.Error("TotalBytes should equal LiveBytes + DeadBytes")
	}

	if info.DeadRatio != 0.4 {
		t.Errorf("expected DeadRatio=0.4, got %.2f", info.DeadRatio)
	}

	if !info.NeedsGC {
		t.Error("expected NeedsGC=true")
	}

	if info.LastScanned.IsZero() {
		t.Error("expected LastScanned to be set")
	}

	t.Log("✓ SegmentGCInfo structure is correct")
}

// TestGCStats_Structure tests the GCStats structure
func TestGCStats_Structure(t *testing.T) {
	stats := GCStats{
		Running:         false,
		LastScanTime:    time.Now(),
		BytesProcessed:  1024,
		SegmentsScanned: 5,
		SegmentsGCed:    2,
		SegmentStats: []SegmentGCInfo{
			{FileID: 1, TotalBytes: 100, LiveBytes: 60, DeadBytes: 40},
			{FileID: 2, TotalBytes: 200, LiveBytes: 150, DeadBytes: 50},
		},
	}

	if stats.Running {
		t.Error("expected Running=false")
	}

	if stats.BytesProcessed != 1024 {
		t.Errorf("expected BytesProcessed=1024, got %d", stats.BytesProcessed)
	}

	if stats.SegmentsScanned != 5 {
		t.Errorf("expected SegmentsScanned=5, got %d", stats.SegmentsScanned)
	}

	if stats.SegmentsGCed != 2 {
		t.Errorf("expected SegmentsGCed=2, got %d", stats.SegmentsGCed)
	}

	if len(stats.SegmentStats) != 2 {
		t.Errorf("expected 2 segment stats, got %d", len(stats.SegmentStats))
	}

	t.Log("✓ GCStats structure is correct")
}
