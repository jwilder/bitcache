package bitcache

import (
	"bytes"
	"os"
	"testing"
	"time"
)

// TestDiskCache_WriteOnce verifies that Set is write-once only
func TestDiskCache_WriteOnce(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcache_writeonce_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	key := []byte("test_key")
	value1 := []byte("first_value")
	value2 := []byte("second_value")

	// First write should succeed
	err = cache.Set(key, value1)
	if err != nil {
		t.Fatalf("First Set failed: %v", err)
	}

	// Verify first value is stored
	retrieved, err := cache.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(retrieved, value1) {
		t.Fatalf("Expected %s, got %s", value1, retrieved)
	}

	// Second write should be a no-op
	err = cache.Set(key, value2)
	if err != nil {
		t.Fatalf("Second Set failed: %v", err)
	}

	// Verify original value is still there (not overwritten)
	retrieved, err = cache.Get(key)
	if err != nil {
		t.Fatalf("Get after second Set failed: %v", err)
	}
	if !bytes.Equal(retrieved, value1) {
		t.Fatalf("Value was overwritten! Expected %s, got %s", value1, retrieved)
	}

	t.Log("✓ Write-once semantics verified")
}

// TestDiskCache_SegmentAccessTracking verifies access tracking
func TestDiskCache_SegmentAccessTracking(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcache_access_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write some keys
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 10)}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Rotate to create a non-active segment
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("Failed to rotate: %v", err)
	}

	// Get the segment info
	cache.segmentsMutex.RLock()
	var segInfo *segmentInfo
	for _, info := range cache.segments {
		segInfo = info
		break
	}
	cache.segmentsMutex.RUnlock()

	if segInfo == nil {
		t.Fatal("No segment found")
	}

	// Check initial access stats
	count1, lastAccess1 := segInfo.getAccessStats()
	t.Logf("Initial access: count=%d, time=%v", count1, lastAccess1)

	// Wait a bit then access a key from the segment
	time.Sleep(10 * time.Millisecond)

	_, err = cache.Get([]byte{0})
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	// Check that access was tracked
	count2, lastAccess2 := segInfo.getAccessStats()
	t.Logf("After read: count=%d, time=%v", count2, lastAccess2)

	if count2 <= count1 {
		t.Errorf("Access count should have increased: %d -> %d", count1, count2)
	}

	if !lastAccess2.After(lastAccess1) {
		t.Errorf("Last access time should have been updated: %v -> %v", lastAccess1, lastAccess2)
	}

	t.Log("✓ Access tracking verified")
}

// TestDiskCache_PruneSegments verifies segment pruning
func TestDiskCache_PruneSegments(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcache_prune_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write some keys to first segment
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 10)}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Rotate to create a segment
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("Failed to rotate: %v", err)
	}

	// Write more keys to a new segment
	for i := 5; i < 10; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 10)}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Check initial stats
	stats := cache.Stats()
	initialKeys := stats.Keys
	t.Logf("Initial keys: %d", initialKeys)

	// Manually set an old last access time on the first segment
	cache.segmentsMutex.RLock()
	var oldSegment *segmentInfo
	for _, info := range cache.segments {
		if info.id.generation == 0 {
			oldSegment = info
			break
		}
	}
	cache.segmentsMutex.RUnlock()

	if oldSegment != nil {
		oldSegment.mu.Lock()
		oldSegment.createdAt = time.Now().Add(-2 * time.Hour)
		oldSegment.mu.Unlock()
		t.Log("Set old segment creation time to 2 hours ago")
	}

	// Compact segments older than 1 hour (age-based pruning)
	result, err := cache.Compact(1 * time.Hour)
	if err != nil {
		t.Fatalf("Failed to compact segments: %v", err)
	}

	pruned := len(result.DeletedSegments)
	t.Logf("Pruned %d segments", pruned)

	if pruned == 0 {
		t.Error("Expected at least one segment to be pruned")
	}

	// Check that keys from pruned segment are gone
	stats = cache.Stats()
	t.Logf("Keys after pruning: %d", stats.Keys)

	if stats.Keys >= initialKeys {
		t.Errorf("Expected fewer keys after pruning: %d -> %d", initialKeys, stats.Keys)
	}

	// Verify that keys from the old segment are no longer accessible
	for i := 0; i < 5; i++ {
		key := []byte{byte(i)}
		_, err := cache.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Key %d should have been pruned, but got: %v", i, err)
		}
	}

	// Verify that keys from the newer segment are still accessible
	for i := 5; i < 10; i++ {
		key := []byte{byte(i)}
		value, err := cache.Get(key)
		if err != nil {
			t.Errorf("Key %d should still exist: %v", i, err)
		} else {
			expected := byte(i * 10)
			if value[0] != expected {
				t.Errorf("Key %d has wrong value: expected %d, got %d", i, expected, value[0])
			}
		}
	}

	t.Log("✓ Segment pruning verified")
}
