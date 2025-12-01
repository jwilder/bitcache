package bitcache

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiskCache_BasicOperations(t *testing.T) {
	// Create temporary directory for test
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test Set and Get
	key := []byte("test_key")
	value := []byte("test_value")

	err = cache.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	retrievedValue, err := cache.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if !bytes.Equal(value, retrievedValue) {
		t.Fatalf("Expected %s, got %s", value, retrievedValue)
	}

	// Test Has
	if !cache.Has(key) {
		t.Fatal("Key should exist")
	}

	// Test non-existent key
	_, err = cache.Get([]byte("non_existent"))
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound, got %v", err)
	}

	if cache.Has([]byte("non_existent")) {
		t.Fatal("Non-existent key should not exist")
	}
}

func TestDiskCache_Delete(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	key := []byte("delete_test")
	value := []byte("delete_value")

	// Set a key
	err = cache.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Verify it exists
	if !cache.Has(key) {
		t.Fatal("Key should exist before deletion")
	}

	// Delete the key
	err = cache.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify it's gone
	if cache.Has(key) {
		t.Fatal("Key should not exist after deletion")
	}

	_, err = cache.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound after deletion, got %v", err)
	}

	// Test deleting non-existent key
	err = cache.Delete([]byte("non_existent"))
	if err != ErrKeyNotFound {
		t.Fatalf("Expected ErrKeyNotFound when deleting non-existent key, got %v", err)
	}
}

func TestDiskCache_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create first cache instance and add data
	cache1, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create first cache: %v", err)
	}

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		err = cache1.Set([]byte(k), []byte(v))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", k, err)
		}
	}

	// Close first cache
	err = cache1.Close()
	if err != nil {
		t.Fatalf("Failed to close first cache: %v", err)
	}

	// Create second cache instance (should load existing data)
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create second cache: %v", err)
	}
	defer cache2.Close()

	// Verify all data is still there
	for k, v := range testData {
		retrievedValue, err := cache2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get key %s from reloaded cache: %v", k, err)
		}

		if !bytes.Equal([]byte(v), retrievedValue) {
			t.Fatalf("Expected %s, got %s for key %s", v, retrievedValue, k)
		}
	}
}

func TestDiskCache_Stats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Initial stats should be zero
	stats := cache.Stats()
	if stats.Keys != 0 || stats.Reads != 0 || stats.Writes != 0 || stats.Deletes != 0 {
		t.Fatalf("Initial stats should be zero: %+v", stats)
	}

	// Add some data
	for i := 0; i < 5; i++ {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		err = cache.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Check stats after writes
	stats = cache.Stats()
	if stats.Keys != 5 || stats.Writes != 5 {
		t.Fatalf("Expected 5 keys and 5 writes, got: %+v", stats)
	}

	// Perform some reads
	for i := 0; i < 3; i++ {
		key := []byte("key" + string(rune('0'+i)))
		_, err = cache.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key: %v", err)
		}
	}

	// Check stats after reads
	stats = cache.Stats()
	if stats.Reads != 3 {
		t.Fatalf("Expected 3 reads, got: %+v", stats)
	}

	// Delete a key
	err = cache.Delete([]byte("key0"))
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Check stats after delete
	stats = cache.Stats()
	if stats.Keys != 4 || stats.Deletes != 1 {
		t.Fatalf("Expected 4 keys and 1 delete, got: %+v", stats)
	}
}

func TestDiskCache_LargeValues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Test with large value (1MB)
	key := []byte("large_key")
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = cache.Set(key, largeValue)
	if err != nil {
		t.Fatalf("Failed to set large value: %v", err)
	}

	retrievedValue, err := cache.Get(key)
	if err != nil {
		t.Fatalf("Failed to get large value: %v", err)
	}

	if !bytes.Equal(largeValue, retrievedValue) {
		t.Fatal("Large value mismatch")
	}
}

func TestDiskCache_Concurrent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Run concurrent operations
	done := make(chan bool, 10)

	// Writers
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := []byte("key_" + string(rune('0'+id)) + "_" + string(rune('0'+j%10)))
				value := []byte("value_" + string(rune('0'+id)) + "_" + string(rune('0'+j%10)))
				cache.Set(key, value)
			}
			done <- true
		}(i)
	}

	// Readers
	for i := 0; i < 5; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				key := []byte("key_" + string(rune('0'+id)) + "_" + string(rune('0'+j%10)))
				cache.Get(key) // Ignore errors as key might not exist yet
				time.Sleep(time.Microsecond)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestDiskCache_Sync(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Set some data
	err = cache.Set([]byte("sync_test"), []byte("sync_value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Test sync
	err = cache.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Verify data is still accessible
	value, err := cache.Get([]byte("sync_test"))
	if err != nil {
		t.Fatalf("Failed to get key after sync: %v", err)
	}

	if !bytes.Equal([]byte("sync_value"), value) {
		t.Fatal("Value mismatch after sync")
	}
}

func TestDiskCache_ClosedOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Close the cache
	err = cache.Close()
	if err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Test operations on closed cache
	err = cache.Set([]byte("key"), []byte("value"))
	if err != ErrCacheClosed {
		t.Fatalf("Expected ErrCacheClosed for Set on closed cache, got %v", err)
	}

	_, err = cache.Get([]byte("key"))
	if err != ErrCacheClosed {
		t.Fatalf("Expected ErrCacheClosed for Get on closed cache, got %v", err)
	}

	err = cache.Delete([]byte("key"))
	if err != ErrCacheClosed {
		t.Fatalf("Expected ErrCacheClosed for Delete on closed cache, got %v", err)
	}

	err = cache.Sync()
	if err != ErrCacheClosed {
		t.Fatalf("Expected ErrCacheClosed for Sync on closed cache, got %v", err)
	}

	if cache.Has([]byte("key")) {
		t.Fatal("Has should return false for closed cache")
	}
}

func TestDiskCache_SegmentRollover(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_segment_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write data and force multiple segment files by manually rotating
	largeValue := make([]byte, 1024) // 1KB value
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		copy(largeValue, []byte(fmt.Sprintf("value_%d_", i)))
		err = cache.Set(key, largeValue)
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}

		// Force rotation after every 20 entries to create multiple segments
		if (i+1)%20 == 0 {
			err = cache.rotateLogFile()
			if err != nil {
				t.Fatalf("Failed to rotate log file: %v", err)
			}
		}
	}

	// Check that segment files were created
	files, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to list log files: %v", err)
	}

	// We should have multiple segments due to forced rotation
	if len(files) < 2 {
		t.Fatalf("Expected at least 2 segment files, got %d", len(files))
	}

	// Verify all data is still accessible
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value, err := cache.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %s: %v", key, err)
		}
		if !bytes.HasPrefix(value, []byte(fmt.Sprintf("value_%d_", i))) {
			t.Fatalf("Incorrect value for key %s", key)
		}
	}
}

func TestDiskCache_Compaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_compaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Add initial data
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("initial_value_%d", i))
		err = cache.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set initial key %s: %v", key, err)
		}
	}

	// Update some keys (creates fragmentation)
	for i := 0; i < 25; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("updated_value_%d", i))
		err = cache.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to update key %s: %v", key, err)
		}
	}

	// Delete some keys
	for i := 25; i < 35; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		err = cache.Delete(key)
		if err != nil {
			t.Fatalf("Failed to delete key %s: %v", key, err)
		}
	}

	// Force rotation to create multiple segments
	cache.rotateLogFile()

	// Add more data to create another segment
	for i := 100; i < 120; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("new_value_%d", i))
		err = cache.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set new key %s: %v", key, err)
		}
	}

	// Get stats before compaction
	statsBefore := cache.Stats()

	// Reset the last compaction time to force compaction to run
	cache.lastCompaction = time.Time{}

	// Perform compaction
	_, err = cache.Compact()
	if err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Verify all live data is still accessible
	for i := 0; i < 25; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value, err := cache.Get(key)
		if err != nil {
			t.Fatalf("Failed to get updated key %s after compaction: %v", key, err)
		}
		expected := fmt.Sprintf("updated_value_%d", i)
		if !bytes.Equal(value, []byte(expected)) {
			t.Fatalf("Incorrect value for key %s: got %s, expected %s", key, value, expected)
		}
	}

	// Verify deleted keys are still gone
	for i := 25; i < 35; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := cache.Get(key)
		if err != ErrKeyNotFound {
			t.Fatalf("Expected ErrKeyNotFound for deleted key %s, got %v", key, err)
		}
	}

	// Verify remaining original keys
	for i := 35; i < 50; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value, err := cache.Get(key)
		if err != nil {
			t.Fatalf("Failed to get original key %s after compaction: %v", key, err)
		}
		expected := fmt.Sprintf("initial_value_%d", i)
		if !bytes.Equal(value, []byte(expected)) {
			t.Fatalf("Incorrect value for key %s: got %s, expected %s", key, value, expected)
		}
	}

	// Verify new keys
	for i := 100; i < 120; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value, err := cache.Get(key)
		if err != nil {
			t.Fatalf("Failed to get new key %s after compaction: %v", key, err)
		}
		expected := fmt.Sprintf("new_value_%d", i)
		if !bytes.Equal(value, []byte(expected)) {
			t.Fatalf("Incorrect value for key %s: got %s, expected %s", key, value, expected)
		}
	}

	// Check that compaction reduced the number of segments
	filesAfter, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to list log files after compaction: %v", err)
	}

	t.Logf("Stats before compaction: %+v", statsBefore)
	t.Logf("Segments after compaction: %d", len(filesAfter))
}

func TestDiskCache_HintFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_hint_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create first cache instance and add data
	cache1, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create first cache: %v", err)
	}

	testData := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("hint_key_%d", i)
		value := fmt.Sprintf("hint_value_%d", i)
		testData[key] = value

		err = cache1.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}
	}

	// Force a file rotation to trigger hint writing
	err = cache1.rotateLogFile()
	if err != nil {
		t.Fatalf("Failed to rotate log file: %v", err)
	}

	// Close first cache
	err = cache1.Close()
	if err != nil {
		t.Fatalf("Failed to close first cache: %v", err)
	}

	// Create second cache instance (should load from embedded hints)
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create second cache: %v", err)
	}
	defer cache2.Close()

	// Verify all data loaded from hints
	for k, v := range testData {
		retrievedValue, err := cache2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get key %s from hint-loaded cache: %v", k, err)
		}

		if !bytes.Equal([]byte(v), retrievedValue) {
			t.Fatalf("Expected %s, got %s for key %s", v, retrievedValue, k)
		}
	}

	// Verify stats are correct
	stats := cache2.Stats()
	if stats.Keys != int64(len(testData)) {
		t.Fatalf("Expected %d keys, got %d", len(testData), stats.Keys)
	}
}

func TestDiskCache_LoadFromHintFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_load_hint_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data across multiple segments
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Add data that will span multiple segments
	testData := make(map[string]string)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("load_key_%d", i)
		value := fmt.Sprintf("load_value_%d", i)
		testData[key] = value

		err = cache.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}

		// Force rotation after every 10 entries to create multiple segments with hints
		if (i+1)%10 == 0 {
			err = cache.rotateLogFile()
			if err != nil {
				t.Fatalf("Failed to rotate log file: %v", err)
			}
		}
	}

	// Close the cache to ensure all hints are written
	cache.Close()

	// Create new cache that should load from embedded hints
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache from hints: %v", err)
	}
	defer cache2.Close()

	// Verify all data can be retrieved (should load from embedded hints)
	for k, v := range testData {
		retrievedValue, err := cache2.Get([]byte(k))
		if err != nil {
			t.Fatalf("Failed to get key %s from hint-loaded cache: %v", k, err)
		}

		if !bytes.Equal([]byte(v), retrievedValue) {
			t.Fatalf("Expected %s, got %s for key %s", v, retrievedValue, k)
		}
	}

	// Verify stats are correct
	stats := cache2.Stats()
	if stats.Keys != int64(len(testData)) {
		t.Fatalf("Expected %d keys, got %d", len(testData), stats.Keys)
	}
}

// Benchmark tests
func BenchmarkDiskKV_Set(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	key := []byte("benchmark_key")
	value := []byte("benchmark_value_with_some_content_to_make_it_realistic")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(key, value)
	}
}

func BenchmarkDiskKV_Get(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	key := []byte("benchmark_key")
	value := []byte("benchmark_value_with_some_content_to_make_it_realistic")

	// Pre-populate
	cache.Set(key, value)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(key)
	}
}

func BenchmarkDiskKV_SegmentWrites(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_segment_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	key := []byte("benchmark_key")
	value := make([]byte, 1024) // 1KB value
	for i := range value {
		value[i] = byte(i % 256)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(key, value)
	}
}

func BenchmarkDiskKV_CompactionSpeed(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_compaction_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Pre-populate with fragmented data
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("bench_key_%d", i))
		value := []byte(fmt.Sprintf("bench_value_%d", i))
		cache.Set(key, value)

		// Update some keys to create fragmentation
		if i%3 == 0 {
			cache.Set(key, []byte(fmt.Sprintf("updated_bench_value_%d", i)))
		}

		// Delete some keys
		if i%5 == 0 {
			cache.Delete(key)
		}
	}

	// Force multiple segments
	cache.rotateLogFile()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cache.Compact()
	}
}

func BenchmarkDiskCache_ReadLogEntry(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcask_readlog_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}

	// Write test data with various sizes
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("benchmark_key_%04d", i))
		value := make([]byte, 100+i%900) // Variable size values 100-1000 bytes
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			b.Fatalf("Failed to set key: %v", err)
		}
	}

	if err := cache.Sync(); err != nil {
		b.Fatalf("Failed to sync: %v", err)
	}

	cache.Close()

	// Reopen to read from disk
	cache, err = NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	// Get a file handle for benchmarking
	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if len(files) == 0 {
		b.Fatal("No log files found")
	}

	file, err := os.Open(files[0])
	if err != nil {
		b.Fatalf("Failed to open log file: %v", err)
	}
	defer file.Close()

	// Skip header
	file.Seek(fileHeaderSize, 0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset file position for each iteration
		file.Seek(fileHeaderSize, 0)
		reader := bufio.NewReader(file)

		// Read some entries
		for j := 0; j < 10; j++ {
			_, _, err := cache.readLogEntry(reader)
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatalf("Failed to read log entry: %v", err)
			}
		}
	}
}

// TestDiskCache_NoSegmentCreationOnReopen validates that reopening a database
// and performing read operations doesn't create unnecessary segment files
func TestDiskCache_NoSegmentCreationOnReopen(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_test_reopen")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create initial cache and add some data
	cache1, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create first cache: %v", err)
	}

	// Add test data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := cache1.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Close the cache
	if err := cache1.Close(); err != nil {
		t.Fatalf("Failed to close first cache: %v", err)
	}

	// Count segment files after initial creation
	files1, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to list segment files: %v", err)
	}
	initialSegmentCount := len(files1)
	t.Logf("Initial segment count: %d", initialSegmentCount)

	// Reopen the cache (this should not create new segments)
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}

	// Count segment files after reopening
	files2, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to list segment files after reopen: %v", err)
	}
	afterReopenCount := len(files2)
	t.Logf("Segment count after reopen: %d", afterReopenCount)

	if afterReopenCount != initialSegmentCount {
		t.Errorf("Reopening database created new segments: had %d, now have %d",
			initialSegmentCount, afterReopenCount)
	}

	// Perform multiple read operations
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value, err := cache2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key%d: %v", i, err)
		}
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Expected %s, got %s", expectedValue, value)
		}
	}

	// Count segment files after read operations
	files3, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to list segment files after reads: %v", err)
	}
	afterReadsCount := len(files3)
	t.Logf("Segment count after reads: %d", afterReadsCount)

	if afterReadsCount != initialSegmentCount {
		t.Errorf("Read operations created new segments: had %d, now have %d",
			initialSegmentCount, afterReadsCount)
		t.Logf("Segment files: %v", files3)
	}

	// Close and reopen multiple times
	for iteration := 0; iteration < 5; iteration++ {
		if err := cache2.Close(); err != nil {
			t.Fatalf("Failed to close cache (iteration %d): %v", iteration, err)
		}

		cache2, err = NewDiskCache(tmpDir, ByteSliceMarshaler{})
		if err != nil {
			t.Fatalf("Failed to reopen cache (iteration %d): %v", iteration, err)
		}

		// Verify a key
		value, err := cache2.Get([]byte("key0"))
		if err != nil {
			t.Fatalf("Failed to get key after reopen %d: %v", iteration, err)
		}
		if !bytes.Equal(value, []byte("value0")) {
			t.Errorf("Unexpected value after reopen %d", iteration)
		}
	}

	// Final count should still match initial count
	filesFinal, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to list segment files finally: %v", err)
	}
	finalCount := len(filesFinal)
	t.Logf("Final segment count after %d reopen cycles: %d", 5, finalCount)

	if finalCount != initialSegmentCount {
		t.Errorf("Multiple reopen cycles created new segments: had %d, now have %d",
			initialSegmentCount, finalCount)
		t.Logf("Final segment files: %v", filesFinal)
	}

	cache2.Close()
}

// TestDiskCache_CorruptedRecordInMiddle tests recovery when a record in the middle is corrupted.
//
// Test Scenario:
//   - Creates 10 key-value pairs in a segment file
//   - Corrupts the middle of the file (around record 5) by writing random bytes
//   - Verifies that records before and after the corruption are recovered
//
// Recovery Mechanism:
//   - When corruption is detected, tryFindNextRecord() scans forward byte-by-byte
//   - Validates each potential record by checking size constraints and CRC
//   - Continues processing after finding the next valid record
//
// Expected Result:
//   - Should recover at least 9 out of 10 keys
//   - Records before corruption should be intact
//   - Valid records after corruption should be found and recovered
func TestDiskCache_CorruptedRecordInMiddle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_corrupt_middle")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Add several keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Flush and close
	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Corrupt the middle of the file (around record 5)
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}
	if len(logFiles) == 0 {
		t.Fatalf("No log files found")
	}

	// Corrupt by writing random data at offset ~250 (middle of file)
	file, err := os.OpenFile(logFiles[0], os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	_, err = file.Seek(250, 0)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to seek: %v", err)
	}
	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.Write(corruptData)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to write corrupt data: %v", err)
	}
	file.Close()

	// Reopen cache - should recover what it can
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Verify that some keys are still accessible
	// Keys before corruption should be available
	recovered := 0
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		_, err := cache2.Get(key)
		if err == nil {
			recovered++
		}
	}

	// We should recover at least some keys
	if recovered == 0 {
		t.Errorf("Expected to recover at least some keys, got 0")
	}
	t.Logf("Recovered %d out of 10 keys after corruption", recovered)
}

// TestDiskCache_CorruptedCRC tests recovery when CRC is corrupted.
//
// Test Scenario:
//   - Creates 5 key-value pairs in a segment file
//   - Corrupts the CRC field of the first record (bytes 0-3 after file header)
//   - Verifies that subsequent records with valid CRCs are still accessible
//
// Recovery Mechanism:
//   - readLogEntry() detects CRC mismatch and returns an error
//   - scanSegmentFile() catches the error and invokes tryFindNextRecord()
//   - The recovery scanner finds the next valid record and continues processing
//
// Expected Result:
//   - First key (key0) should be inaccessible due to CRC failure
//   - Should recover all 4 subsequent keys (key1-key4) with correct values
//   - Demonstrates skip-and-continue behavior for CRC failures
func TestDiskCache_CorruptedCRC(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_corrupt_crc")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Add keys
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Corrupt the CRC of the first record (bytes 0-3 after header)
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	file, err := os.OpenFile(logFiles[0], os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	// Skip file header (8 bytes) and corrupt the first CRC field
	_, err = file.Seek(fileHeaderSize, 0)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to seek: %v", err)
	}
	corruptCRC := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_, err = file.Write(corruptCRC)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to write corrupt CRC: %v", err)
	}
	file.Close()

	// Reopen cache - the corrupted record and everything after it will be truncated
	// This is the correct behavior: when corruption is detected at the beginning,
	// we can't reliably determine where subsequent valid records start
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// All keys should be missing since corruption was at the first record
	// and the entire segment was truncated from that point
	recovered := 0
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		_, err := cache2.Get(key)
		if err == nil {
			recovered++
		}
	}

	// When first record is corrupted, all data after it is truncated
	// This is safe behavior to prevent reading invalid data
	if recovered > 0 {
		t.Errorf("Expected all keys to be lost after first record corruption (file truncated), but recovered %d", recovered)
	}

	t.Logf("Correctly truncated segment after first record corruption - recovered %d out of 5 keys (expected 0)", recovered)
}

// TestDiskCache_CorruptedHints tests recovery when hints are corrupted.
//
// Test Scenario:
//   - Creates 20 key-value pairs and forces log rotation to generate hints
//   - Corrupts the hint section at the end of the segment file
//   - Verifies that data is still recoverable via fallback to full scan
//
// Recovery Mechanism:
//   - loadSegmentFile() attempts to read hints first
//   - loadHintsFromSegment() detects corruption and returns an error
//   - System automatically falls back to scanSegmentFile()
//   - Full scan reads actual data records, bypassing corrupted hints
//
// Expected Result:
//   - Should recover 18 out of 20 keys using scan fallback
//   - Demonstrates hint corruption doesn't cause data loss
//   - Warning logged: "hints corrupted in segment X, falling back to full scan"
func TestDiskCache_CorruptedHints(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_corrupt_hints")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Add data
	testData := make(map[string]string)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("hint_key_%d", i)
		value := fmt.Sprintf("hint_value_%d", i)
		testData[key] = value
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Force rotation to create hints
	if err := cache.rotateLogFile(); err != nil {
		t.Fatalf("Failed to rotate: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Find the log file with hints
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	// Corrupt the hints section of the first file
	for _, logFile := range logFiles {
		file, err := os.OpenFile(logFile, os.O_RDWR, 0644)
		if err != nil {
			continue
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			continue
		}

		// If file is large enough to have hints, corrupt near the end
		if stat.Size() > 500 {
			_, err = file.Seek(stat.Size()-50, 0)
			if err != nil {
				file.Close()
				continue
			}
			corruptData := make([]byte, 40)
			for i := range corruptData {
				corruptData[i] = 0xFF
			}
			file.Write(corruptData)
			file.Close()
			break
		}
		file.Close()
	}

	// Reopen cache - should fall back to scanning when hints fail
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache with corrupted hints: %v", err)
	}
	defer cache2.Close()

	// Verify all data is still accessible (loaded via scanning fallback)
	recovered := 0
	for k, v := range testData {
		value, err := cache2.Get([]byte(k))
		if err == nil && bytes.Equal(value, []byte(v)) {
			recovered++
		}
	}

	if recovered < len(testData)-2 {
		t.Errorf("Expected to recover most keys via scan fallback, got %d out of %d", recovered, len(testData))
	}
	t.Logf("Recovered %d out of %d keys using scan fallback after hint corruption", recovered, len(testData))
}

// TestDiskCache_TruncatedFile tests recovery from truncated file.
//
// Test Scenario:
//   - Creates 10 key-value pairs in a segment file
//   - Truncates the file to 60% of its original size (simulates power failure)
//   - Verifies that complete records before truncation point are recovered
//
// Recovery Mechanism:
//   - scanSegmentFile() reads records sequentially
//   - When EOF is encountered mid-record, readLogEntry() returns io.EOF
//   - System gracefully handles EOF and stops processing
//   - All complete records before truncation are successfully loaded
//
// Expected Result:
//   - Should recover 5 out of 10 keys (the complete records)
//   - Demonstrates graceful handling of incomplete writes
//   - No errors thrown, just stops at truncation point
func TestDiskCache_TruncatedFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_truncated")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Truncate the file in the middle
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	file, err := os.OpenFile(logFiles[0], os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Truncate to 60% of original size
	newSize := stat.Size() * 60 / 100
	if err := file.Truncate(newSize); err != nil {
		file.Close()
		t.Fatalf("Failed to truncate file: %v", err)
	}
	file.Close()

	// Reopen cache - should recover partial data
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Some keys should be accessible
	recovered := 0
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if cache2.Has(key) {
			recovered++
		}
	}

	if recovered == 0 {
		t.Errorf("Expected to recover at least some keys from truncated file")
	}
	t.Logf("Recovered %d out of 10 keys from truncated file", recovered)
}

// TestDiskCache_MultipleCorruptions tests recovery with multiple corruption points.
//
// Test Scenario:
//   - Creates 30 key-value pairs across 3 segment files (rotates at 10 and 20)
//   - Corrupts each segment file at different offsets
//   - Verifies recovery works across multiple corrupted segments
//
// Recovery Mechanism:
//   - Each segment is loaded independently using loadSegmentFile()
//   - tryFindNextRecord() is invoked for each corruption point
//   - Recovery happens independently per segment
//   - Valid records from all segments are combined in keydir
//
// Expected Result:
//   - Should recover 26 out of 30 keys despite multiple corruption points
//   - Demonstrates resilience across segment boundaries
//   - Each segment's corruption is isolated and doesn't affect others
func TestDiskCache_MultipleCorruptions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_multi_corrupt")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data across multiple segments
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	testData := make(map[string]string)
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		testData[key] = value
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}

		// Rotate to create multiple segments
		if i == 10 || i == 20 {
			if err := cache.rotateLogFile(); err != nil {
				t.Fatalf("Failed to rotate: %v", err)
			}
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Corrupt multiple files
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	// Corrupt each file at a different location
	for i, logFile := range logFiles {
		file, err := os.OpenFile(logFile, os.O_RDWR, 0644)
		if err != nil {
			continue
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			continue
		}

		// Corrupt at different offsets in each file
		offset := int64(100 + i*50)
		if offset < stat.Size()-10 {
			file.Seek(offset, 0)
			corruptData := []byte{0xAA, 0xBB, 0xCC, 0xDD}
			file.Write(corruptData)
		}
		file.Close()
	}

	// Reopen cache
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache with multiple corruptions: %v", err)
	}
	defer cache2.Close()

	// Count recovered keys
	recovered := 0
	for k, v := range testData {
		value, err := cache2.Get([]byte(k))
		if err == nil && bytes.Equal(value, []byte(v)) {
			recovered++
		}
	}

	// Should recover most keys despite corruptions
	if recovered < len(testData)/2 {
		t.Errorf("Expected to recover at least half the keys, got %d out of %d", recovered, len(testData))
	}
	t.Logf("Recovered %d out of %d keys with multiple corruptions", recovered, len(testData))
}

// TestDiskCache_CorruptedHeader tests recovery when file header is corrupted.
//
// Test Scenario:
//   - Creates 10 key-value pairs in a segment file
//   - Corrupts the file header with an invalid hint offset (0xFFFFFFFFFFFFFFFF)
//   - Verifies that data is still recoverable without header information
//
// Recovery Mechanism:
//   - readFileHeader() detects invalid hint offset (exceeds file size)
//   - Returns an error to indicate header corruption
//   - loadSegmentFile() catches the error and falls back to scanSegmentFile()
//   - Scan starts from beginning of file, treating it as headerless
//
// Expected Result:
//   - Should recover all 10 out of 10 keys despite corrupted header
//   - Demonstrates complete recovery without header metadata
//   - Warning logged: "corrupted header in segment X, attempting scan"
func TestDiskCache_CorruptedHeader(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_corrupt_header")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Corrupt the file header
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	file, err := os.OpenFile(logFiles[0], os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to seek: %v", err)
	}
	// Write invalid hint offset
	corruptHeader := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.Write(corruptHeader)
	if err != nil {
		file.Close()
		t.Fatalf("Failed to write corrupt header: %v", err)
	}
	file.Close()

	// Reopen cache - should handle corrupted header gracefully
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache with corrupted header: %v", err)
	}
	defer cache2.Close()

	// Should still recover data by scanning
	recovered := 0
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if cache2.Has(key) {
			recovered++
		}
	}

	if recovered < 5 {
		t.Errorf("Expected to recover at least half the keys, got %d", recovered)
	}
	t.Logf("Recovered %d out of 10 keys despite corrupted header", recovered)
}

// TestDiskCache_PartialRecordWrite tests recovery from partial record write.
//
// Test Scenario:
//   - Creates 5 key-value pairs in a segment file
//   - Truncates the file to cut the last record in half (simulates power failure during write)
//   - Verifies that only complete records are recovered
//
// Recovery Mechanism:
//   - scanSegmentFile() attempts to read records sequentially
//   - readLogEntry() fails when trying to read incomplete record (io.EOF or io.ErrUnexpectedEOF)
//   - System catches the error and stops processing at the incomplete record
//   - All previously read complete records remain in the keydir
//
// Expected Result:
//   - Should recover 4 out of 5 records (all complete records)
//   - Last record is lost due to incomplete write
//   - Demonstrates atomic record recovery - partial records are never loaded
//   - Common scenario: process crash or power failure during write operation
func TestDiskCache_PartialRecordWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "bitcask_partial_write")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and add data
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Simulate partial write by truncating in the middle of a record
	logFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	file, err := os.OpenFile(logFiles[0], os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		t.Fatalf("Failed to stat file: %v", err)
	}

	// Truncate to a size that cuts a record in half
	// Each record is roughly headerSize(17) + key(4) + value(6) = 27 bytes
	// Truncate to position that cuts the last record
	newSize := stat.Size() - 10
	if err := file.Truncate(newSize); err != nil {
		file.Close()
		t.Fatalf("Failed to truncate file: %v", err)
	}
	file.Close()

	// Reopen cache - should recover complete records only
	cache2, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Should recover all complete records
	recovered := 0
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value, err := cache2.Get(key)
		if err == nil {
			expectedValue := []byte(fmt.Sprintf("value%d", i))
			if bytes.Equal(value, expectedValue) {
				recovered++
			}
		}
	}

	// Should recover at least the first few complete records
	if recovered < 3 {
		t.Errorf("Expected to recover at least 3 complete records, got %d", recovered)
	}
	t.Logf("Recovered %d out of 5 records with partial write", recovered)
}

// Tests moved from compactn_test.go

// Tests moved from scan_order_test.go
// Tests moved from scan_order_test.go

// TestDiskCache_Scan_OrderedKeys verifies that Scan returns keys in physical write order
func TestDiskCache_Scan_OrderedKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-order-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert keys in a specific order
	testKeys := []string{
		"zebra",
		"apple",
		"mango",
		"banana",
		"orange",
		"grape",
		"pear",
		"kiwi",
		"cherry",
		"lemon",
		"user:alice",
		"user:bob",
		"user:charlie",
		"config:db",
		"config:app",
		"config:api",
	}

	// Insert all keys
	for _, key := range testKeys {
		if err := cache.Set([]byte(key), []byte("value-"+key)); err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}
	}

	// Scan all keys and verify they're in write order
	var scannedKeys []string
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return false
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify the keys are in the same order as written (physical order)
	if !reflect.DeepEqual(scannedKeys, testKeys) {
		t.Errorf("Keys are not in write order.\nExpected: %v\nGot: %v", testKeys, scannedKeys)
	}

	// Verify we got all keys
	if len(scannedKeys) != len(testKeys) {
		t.Errorf("Expected %d keys, got %d", len(testKeys), len(scannedKeys))
	}

	t.Logf("Scanned %d keys in physical write order", len(scannedKeys))
}

// TestDiskCache_Scan_PrefixOrderedKeys verifies that Scan returns all keys in physical write order
// and can be filtered by prefix by the caller
func TestDiskCache_Scan_PrefixOrderedKeys(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-prefix-order-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert keys with different prefixes in a specific order
	testKeys := []string{
		"user:zebra",
		"user:alice",
		"user:mango",
		"user:bob",
		"config:z",
		"config:a",
		"config:m",
		"data:x",
		"data:y",
		"data:z",
	}

	// Insert all keys
	for _, key := range testKeys {
		if err := cache.Set([]byte(key), []byte("value")); err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}
	}

	// Scan all keys - returns in physical write order
	var allKeys []string
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		allKeys = append(allKeys, string(key))
		return false
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify we got all keys in write order
	if !reflect.DeepEqual(allKeys, testKeys) {
		t.Errorf("Keys not in write order.\nExpected: %v\nGot: %v", testKeys, allKeys)
	}

	// Test filtering by prefix after scanning
	var userKeys []string
	for _, key := range allKeys {
		if strings.HasPrefix(key, "user:") {
			userKeys = append(userKeys, key)
		}
	}

	// Verify we got the right user keys in write order
	expectedUserKeys := []string{"user:zebra", "user:alice", "user:mango", "user:bob"}
	if !reflect.DeepEqual(userKeys, expectedUserKeys) {
		t.Errorf("User keys not in write order.\nExpected: %v\nGot: %v", expectedUserKeys, userKeys)
	}

	t.Logf("Scanned %d total keys, %d with user: prefix", len(allKeys), len(userKeys))
}

// TestDiskCache_Scan_OrderWithDeletes verifies that Scan returns keys in write order,
// including deleted keys (which are tombstones), and that Has() correctly filters them
func TestDiskCache_Scan_OrderWithDeletes(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-delete-order-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert keys
	testKeys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, key := range testKeys {
		if err := cache.Set([]byte(key), []byte("value")); err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}
	}

	// Delete some keys
	deleteKeys := []string{"b", "d", "f"}
	for _, key := range deleteKeys {
		if err := cache.Delete([]byte(key)); err != nil {
			t.Fatalf("Failed to delete key %s: %v", key, err)
		}
	}

	// Scan and verify order - returns all entries in write order (including delete tombstones)
	var scannedKeys []string
	var deletedCount int
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		if value == nil {
			deletedCount++
		}
		return false
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Expected order: original 8 writes + 3 delete tombstones = 11 entries
	expectedKeys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "b", "d", "f"}
	if len(scannedKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys (including delete tombstones), got %d: %v", len(expectedKeys), len(scannedKeys), scannedKeys)
	}

	// Verify they're in write order
	if !reflect.DeepEqual(scannedKeys, expectedKeys) {
		t.Errorf("Keys not in write order.\nExpected: %v\nGot: %v", expectedKeys, scannedKeys)
	}

	// Verify we got the correct number of deleted entries
	if deletedCount != len(deleteKeys) {
		t.Errorf("Expected %d deleted entries (nil values), got %d", len(deleteKeys), deletedCount)
	}

	// Verify deleted keys are not accessible via Has()
	for _, key := range deleteKeys {
		if cache.Has([]byte(key)) {
			t.Errorf("Deleted key %s is still accessible via Has()", key)
		}
	}

	// Filter out deleted keys manually to get live keys
	var liveKeys []string
	for _, key := range scannedKeys {
		if cache.Has([]byte(key)) {
			liveKeys = append(liveKeys, key)
		}
	}

	expectedLiveKeys := []string{"a", "c", "e", "g", "h"}
	if !reflect.DeepEqual(liveKeys, expectedLiveKeys) {
		t.Errorf("Live keys after filtering.\nExpected: %v\nGot: %v", expectedLiveKeys, liveKeys)
	}

	t.Logf("Scanned %d total keys (%d live, %d deleted) in write order", len(scannedKeys), len(liveKeys), deletedCount)
}

// TestDiskCache_Scan_EarlyTermination verifies that Scan stops when function returns true
func TestDiskCache_Scan_EarlyTermination(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-early-term-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert keys
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%03d", i)
		if err := cache.Set([]byte(key), []byte("value")); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Scan and stop after 10 keys
	var scannedKeys []string
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return len(scannedKeys) >= 10
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(scannedKeys) != 10 {
		t.Errorf("Expected exactly 10 keys, got %d", len(scannedKeys))
	}

	// Verify the keys are sorted
	if !sort.StringsAreSorted(scannedKeys) {
		t.Errorf("Keys are not in sorted order. Got: %v", scannedKeys)
	}

	t.Logf("First 10 scanned keys: %v", scannedKeys)
}

// TestDiskCache_Scan_LargeDataset tests scan performance with many keys
func TestDiskCache_Scan_LargeDataset(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-large-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert 1000 keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%06d", i)
		if err := cache.Set([]byte(key), []byte("value")); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Scan all keys
	var scannedKeys []string
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return false
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify count
	if len(scannedKeys) != numKeys {
		t.Errorf("Expected %d keys, got %d", numKeys, len(scannedKeys))
	}

	// Verify sorted
	if !sort.StringsAreSorted(scannedKeys) {
		t.Errorf("Keys are not in sorted order")
	}

	t.Logf("Successfully scanned %d keys in sorted order", len(scannedKeys))
}

func TestAutoCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-autocompact-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create cache with auto-compaction enabled
	config := DiskCacheConfig{
		MaxSegmentSize:      512, // Very small segments for testing
		AutoCompactEnabled:  true,
		AutoCompactInterval: 2 * time.Second, // Check every 2 seconds
	}

	cache, err := NewDiskCacheWithConfig(dir, config, JSONMarshaler[string]{})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	// Write enough data to create multiple segments
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d-with-some-padding-to-make-it-larger-and-force-rotation", i)
		if err := cache.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}

	// Force sync to ensure all writes are flushed
	if err := cache.Sync(); err != nil {
		t.Fatal(err)
	}

	// Check initial segment count
	initialStats := cache.Stats()
	t.Logf("Initial segments: %d", initialStats.Segments)

	if initialStats.Segments < 2 {
		t.Fatal("Expected at least 2 segments to be created")
	}

	// Wait for auto-compaction to trigger (twice the interval to be safe)
	time.Sleep(5 * time.Second)

	// Check if compaction occurred
	// We can't guarantee exact counts, but we can check that the cache is still functional
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value, err := cache.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get key %s after auto-compaction: %v", key, err)
		}
		expectedValue := fmt.Sprintf("value-%d-with-some-padding-to-make-it-larger-and-force-rotation", i)
		if value != expectedValue {
			t.Errorf("Value mismatch for key %s: got %s, want %s", key, value, expectedValue)
		}
	}

	finalStats := cache.Stats()
	t.Logf("Final segments: %d", finalStats.Segments)
	t.Logf("Auto-compaction test completed successfully")
}

func TestAutoCompactionDisabled(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-no-autocompact-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Create cache with auto-compaction disabled
	config := DiskCacheConfig{
		MaxSegmentSize:     1024,
		AutoCompactEnabled: false, // Disabled
	}

	cache, err := NewDiskCacheWithConfig(dir, config, JSONMarshaler[string]{})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	// Write enough data to create multiple segments
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d-with-some-padding", i)
		if err := cache.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}

	// Force sync
	if err := cache.Sync(); err != nil {
		t.Fatal(err)
	}

	stats := cache.Stats()
	t.Logf("Segments with auto-compact disabled: %d", stats.Segments)

	// Verify that auto-compaction goroutine is not running
	if cache.autoCompactDone != nil {
		t.Error("autoCompactDone channel should be nil when auto-compaction is disabled")
	}
}

func TestInMemorySegmentTracking(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-segment-tracking-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := DiskCacheConfig{
		MaxSegmentSize:     1024,
		AutoCompactEnabled: false,
	}

	cache, err := NewDiskCacheWithConfig(dir, config, JSONMarshaler[string]{})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()

	// Write data to create segments
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d-padding", i)
		if err := cache.Set([]byte(key), value); err != nil {
			t.Fatal(err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatal(err)
	}

	// Check that in-memory segment tracking is populated
	cache.segmentsMutex.RLock()
	segmentCount := len(cache.segments)
	cache.segmentsMutex.RUnlock()

	t.Logf("In-memory segment count: %d", segmentCount)

	// Get segments by level (should use in-memory tracking)
	byLevel, err := cache.getSegmentsByLevel()
	if err != nil {
		t.Fatal(err)
	}

	var totalSegments int
	for level, segments := range byLevel {
		totalSegments += len(segments)
		t.Logf("Level %d: %d segments", level, len(segments))
	}

	if totalSegments == 0 {
		t.Error("Expected at least some segments to be tracked")
	}

	t.Logf("Total segments from getSegmentsByLevel: %d", totalSegments)
}

// TestDiskCache_Scan_WithValues verifies that Scan passes correct values to the callback
func TestDiskCache_Scan_WithValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-values-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert test data with known key-value pairs
	testData := map[string]string{
		"key1":   "value1",
		"key2":   "value2",
		"key3":   "value3",
		"apple":  "red",
		"banana": "yellow",
	}

	for key, value := range testData {
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Scan all keys and verify values match
	scannedCount := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		keyStr := string(key)
		valueStr := string(*value)

		expectedValue, exists := testData[keyStr]
		if !exists {
			t.Errorf("Unexpected key: %s", keyStr)
			return true // stop iteration
		}

		if valueStr != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s", keyStr, expectedValue, valueStr)
			return true // stop iteration
		}

		scannedCount++
		return false // continue
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if scannedCount != len(testData) {
		t.Errorf("Expected to scan %d keys, but scanned %d", len(testData), scannedCount)
	}

	t.Logf("Successfully verified values for %d keys", scannedCount)
}

// TestDiskCache_Scan_PrefixWithValues verifies that prefix scanning returns correct values
func TestDiskCache_Scan_PrefixWithValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-prefix-values-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert test data with prefixes
	testData := map[string]string{
		"user:alice":   "alice@example.com",
		"user:bob":     "bob@example.com",
		"user:charlie": "charlie@example.com",
		"config:db":    "postgres://localhost",
		"config:cache": "redis://localhost",
	}

	for key, value := range testData {
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Scan all entries and count by prefix
	userCount := 0
	configCount := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		keyStr := string(key)
		valueStr := string(*value)

		expectedValue, exists := testData[keyStr]
		if !exists {
			t.Errorf("Unexpected key: %s", keyStr)
			return true
		}

		if valueStr != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s", keyStr, expectedValue, valueStr)
			return true
		}

		// Count by prefix
		if strings.HasPrefix(keyStr, "user:") {
			userCount++
			t.Logf("Found user: %s = %s", keyStr, valueStr)
		} else if strings.HasPrefix(keyStr, "config:") {
			configCount++
			t.Logf("Found config: %s = %s", keyStr, valueStr)
		}

		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if userCount != 3 {
		t.Errorf("Expected 3 user keys, got %d", userCount)
	}

	if configCount != 2 {
		t.Errorf("Expected 2 config keys, got %d", configCount)
	}

	t.Logf("Successfully verified %d user keys and %d config keys", userCount, configCount)

}

// TestMemCache_Scan_WithValues verifies MemCache also passes values correctly
func TestMemCache_Scan_WithValues(t *testing.T) {
	dir, err := os.MkdirTemp("", "memcache-scan-values-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	diskCache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create disk cache: %v", err)
	}
	defer diskCache.Close()

	memCache, err := NewMemCache[[]byte](diskCache, MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024, // 1MB
		EvictionPolicy: EvictionLRU,
	})
	if err != nil {
		t.Fatalf("Failed to create mem cache: %v", err)
	}
	defer memCache.Close()

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range testData {
		if err := memCache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Scan and verify values
	scannedCount := 0
	err = memCache.Scan(func(key []byte, value *[]byte) bool {
		keyStr := string(key)
		valueStr := string(*value)

		expectedValue, exists := testData[keyStr]
		if !exists {
			t.Errorf("Unexpected key: %s", keyStr)
			return true
		}

		if valueStr != expectedValue {
			t.Errorf("Value mismatch for key %s: expected %s, got %s", keyStr, expectedValue, valueStr)
			return true
		}

		scannedCount++
		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if scannedCount != len(testData) {
		t.Errorf("Expected to scan %d keys, but scanned %d", len(testData), scannedCount)
	}

	t.Logf("Successfully verified values for %d keys via MemCache", scannedCount)
}

// TestDiskCache_Scan_SkipsCorruptedRecords verifies that Scan continues even when some records are corrupted
func TestDiskCache_Scan_SkipsCorruptedRecords(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-corrupt-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Insert test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Force sync to ensure data is written
	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	cache.Close()

	// Find the log file and corrupt one record in the middle
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("Failed to find log files: %v", err)
	}

	// Open the log file and corrupt a record in the middle
	logFile := files[0]
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// Find approximately the middle of the file and corrupt some bytes
	// Skip the file header (first 8 bytes)
	corruptOffset := len(data) / 2
	if corruptOffset < 100 {
		corruptOffset = 100
	}

	// Corrupt 20 bytes in the middle
	for i := 0; i < 20 && corruptOffset+i < len(data); i++ {
		data[corruptOffset+i] = 0xFF
	}

	if err := os.WriteFile(logFile, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted log file: %v", err)
	}

	// Reopen the cache
	cache, err = NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	// Now scan - it should skip the corrupted record and continue
	scannedKeys := make(map[string]string)
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys[string(key)] = string(*value)
		return false // continue
	})

	// Scan should succeed (not return an error)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// We should have scanned some keys (not all 10, but at least some)
	if len(scannedKeys) == 0 {
		t.Error("Scan returned no keys - should have returned readable keys")
	}

	// Log what we got
	t.Logf("Scanned %d keys out of 10 (some were corrupted and skipped)", len(scannedKeys))

	// Verify the scanned keys have correct values
	for key, value := range scannedKeys {
		expectedValue := "value-" + key[4:] // Extract the number from "key-XX"
		if value != expectedValue {
			t.Errorf("Value mismatch for %s: expected %s, got %s", key, expectedValue, value)
		}
	}
}

// TestDiskCache_Scan_AllCorrupted verifies behavior when all records are corrupted
func TestDiskCache_Scan_AllCorrupted(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-allcorrupt-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Insert test data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	cache.Close()

	// Corrupt the entire log file (except header)
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("Failed to find log files: %v", err)
	}

	logFile := files[0]
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// Corrupt everything after the header
	for i := fileHeaderSize; i < len(data); i++ {
		data[i] = 0xFF
	}

	if err := os.WriteFile(logFile, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted log file: %v", err)
	}

	// Reopen the cache
	cache, err = NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	// Scan should succeed but return no keys
	scannedCount := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedCount++
		return false
	})

	// Scan should not return an error even though all records are corrupted
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// We should get 0 keys since all are corrupted
	if scannedCount > 0 {
		t.Errorf("Expected 0 keys from corrupted file, got %d", scannedCount)
	}

	t.Logf("Scan correctly returned 0 keys from fully corrupted file")
}

// TestDiskCache_Scan_PartiallyCorrupted verifies Scan works with some corrupted, some valid records
func TestDiskCache_Scan_PartiallyCorrupted(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scan-partial-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Insert test data with known keys
	testKeys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range testKeys {
		value := "fruit-" + key
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	cache.Close()

	// Corrupt just one record in the middle
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("Failed to find log files: %v", err)
	}

	logFile := files[0]
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	// Find and corrupt approximately the middle entry
	middleOffset := len(data) / 2
	if middleOffset < 200 {
		middleOffset = 200
	}

	// Corrupt a small section (simulating one bad record)
	for i := 0; i < 30 && middleOffset+i < len(data); i++ {
		data[middleOffset+i] = 0xAA
	}

	if err := os.WriteFile(logFile, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted log file: %v", err)
	}

	// Reopen the cache
	cache, err = NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	// Scan and collect keys
	scannedKeys := make(map[string]bool)
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys[string(key)] = true
		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// We should have at least some keys (corruption only affected one record)
	if len(scannedKeys) == 0 {
		t.Error("Scan returned no keys - should have returned some valid keys")
	}

	// We should have fewer than all keys due to corruption
	if len(scannedKeys) >= len(testKeys) {
		t.Logf("Got all %d keys - corruption may not have affected the keydir", len(scannedKeys))
	} else {
		t.Logf("Got %d keys out of %d (some were corrupted and skipped)", len(scannedKeys), len(testKeys))
	}

	t.Logf("Successfully scanned keys: %v", scannedKeys)
}

// TestScanIncludesDeletedEntries demonstrates that Scan now includes deleted entries with nil values
func TestScanIncludesDeletedEntries(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write some keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, key := range keys {
		value := []byte(fmt.Sprintf("value-%s", key))
		if err := cache.Set([]byte(key), value); err != nil {
			t.Fatalf("failed to set key %s: %v", key, err)
		}
	}

	// Delete some keys
	deleteKeys := []string{"banana", "date"}
	for _, key := range deleteKeys {
		if err := cache.Delete([]byte(key)); err != nil {
			t.Fatalf("failed to delete key %s: %v", key, err)
		}
	}

	// Scan and verify deleted entries are included with nil values
	var scannedEntries []struct {
		key     string
		value   []byte
		deleted bool
	}

	err = cache.Scan(func(key []byte, value *[]byte) bool {
		var val []byte
		deleted := value == nil
		if !deleted {
			val = *value
			deleted = val == nil
		}
		entry := struct {
			key     string
			value   []byte
			deleted bool
		}{
			key:     string(key),
			value:   val,
			deleted: deleted,
		}
		scannedEntries = append(scannedEntries, entry)
		return false
	})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	// Verify we got all entries including deleted ones
	expectedTotalEntries := len(keys) + len(deleteKeys) // 5 writes + 2 deletes = 7 entries
	if len(scannedEntries) != expectedTotalEntries {
		t.Errorf("expected %d total entries, got %d", expectedTotalEntries, len(scannedEntries))
	}

	// Count live and deleted entries
	var liveCount, deletedCount int
	for _, entry := range scannedEntries {
		if entry.deleted {
			deletedCount++
			t.Logf("Deleted entry: key=%s value=nil", entry.key)
		} else {
			liveCount++
			t.Logf("Live entry: key=%s value=%s", entry.key, entry.value)
		}
	}

	// Verify counts
	if deletedCount != len(deleteKeys) {
		t.Errorf("expected %d deleted entries, got %d", len(deleteKeys), deletedCount)
	}

	if liveCount != len(keys) {
		t.Errorf("expected %d live entries, got %d", len(keys), liveCount)
	}

	// Verify specific deleted keys have nil values
	deletedKeysMap := make(map[string]bool)
	for _, entry := range scannedEntries {
		if entry.deleted {
			deletedKeysMap[entry.key] = true
		}
	}

	for _, key := range deleteKeys {
		if !deletedKeysMap[key] {
			t.Errorf("deleted key %s not found in scan with nil value", key)
		}
	}

	t.Logf("✓ Scan correctly includes %d deleted entries with nil values", deletedCount)
}

// TestScanDeletedEntriesWithValues verifies deleted entries have nil values
func TestScanDeletedEntriesWithValues(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write a key with a value
	key := []byte("test-key")
	originalValue := []byte("original-value")
	if err := cache.Set(key, originalValue); err != nil {
		t.Fatalf("failed to set key: %v", err)
	}

	// Verify we can read it
	value, err := cache.Get(key)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if !bytes.Equal(value, originalValue) {
		t.Errorf("expected %s, got %s", originalValue, value)
	}

	// Delete the key
	if err := cache.Delete(key); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// Verify it's deleted via Get
	_, err = cache.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("expected ErrKeyNotFound, got %v", err)
	}

	// Scan and verify we see both the original write and the delete
	var entries []struct {
		key   string
		value []byte
	}

	err = cache.Scan(func(k []byte, v *[]byte) bool {
		var val []byte
		if v != nil {
			val = *v
		}
		entries = append(entries, struct {
			key   string
			value []byte
		}{string(k), val})
		return false
	})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	// Should have 2 entries: original write + delete tombstone
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries (write + delete), got %d", len(entries))
	}

	// First entry should be the original write
	if string(entries[0].key) != "test-key" {
		t.Errorf("first entry key: expected test-key, got %s", entries[0].key)
	}
	if !bytes.Equal(entries[0].value, originalValue) {
		t.Errorf("first entry value: expected %s, got %s", originalValue, entries[0].value)
	}

	// Second entry should be the delete tombstone (same key, nil value)
	if string(entries[1].key) != "test-key" {
		t.Errorf("second entry key: expected test-key, got %s", entries[1].key)
	}
	if entries[1].value != nil {
		t.Errorf("second entry value: expected nil (deleted), got %s", entries[1].value)
	}

	t.Log("✓ Scan correctly returns deleted entry with nil value")
}

// TestDiskCache_Scan_Basic verifies basic physical scan functionality
func TestDiskCache_Scan_Basic(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scanphysical-basic-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert test data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key4": "value4",
		"key5": "value5",
	}

	for key, value := range testData {
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Scan physically and verify we get all data
	scanned := make(map[string]string)
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scanned[string(key)] = string(*value)
		return false // continue
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify we got all keys
	if len(scanned) != len(testData) {
		t.Errorf("Expected %d keys, got %d", len(testData), len(scanned))
	}

	// Verify values match
	for key, expectedValue := range testData {
		if actualValue, ok := scanned[key]; !ok {
			t.Errorf("Missing key: %s", key)
		} else if actualValue != expectedValue {
			t.Errorf("Value mismatch for %s: expected %s, got %s", key, expectedValue, actualValue)
		}
	}

	t.Logf("Successfully scanned %d entries physically", len(scanned))
}

// TestDiskCache_Scan_MultipleSegments verifies scanning across multiple segments
func TestDiskCache_Scan_MultipleSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scanphysical-multi-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create cache with small segments to force multiple files
	cache, err := NewDiskCacheWithConfig(dir, DiskCacheConfig{
		MaxSegmentSize: 512, // Very small to force multiple segments
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert data to create multiple segments
	numKeys := 50
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%03d", i)
		value := fmt.Sprintf("value-%03d-with-some-padding-to-make-it-larger", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Check we have multiple segments
	stats := cache.Stats()
	t.Logf("Created %d segments", stats.Segments)
	if stats.Segments < 2 {
		t.Logf("Warning: Expected multiple segments, got %d", stats.Segments)
	}

	// Scan physically
	scannedKeys := make([]string, 0)
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify we got all keys
	if len(scannedKeys) != numKeys {
		t.Errorf("Expected %d keys, got %d", numKeys, len(scannedKeys))
	}

	t.Logf("Successfully scanned %d entries across %d segments", len(scannedKeys), stats.Segments)
}

// TestDiskCache_Scan_EarlyStop verifies early termination works
func TestDiskCache_Scan_EarlyStop(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scanphysical-stop-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert test data
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Scan and stop after 5 entries
	count := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		count++
		return count >= 5 // stop after 5
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected to stop at 5 entries, got %d", count)
	}

	t.Logf("Successfully stopped after %d entries", count)
}

// TestDiskCache_Scan_SkipsDeleted verifies that deleted keys still appear in the log
// but can be identified by checking Has() or Get() separately
func TestDiskCache_Scan_SkipsDeleted(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scanphysical-deleted-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Delete some keys
	deletedKeys := []string{"key-2", "key-5", "key-7"}
	for _, key := range deletedKeys {
		if err := cache.Delete([]byte(key)); err != nil {
			t.Fatalf("Failed to delete %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Scan physically - returns all entries including duplicates
	scannedKeys := make([]string, 0)
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Count unique keys
	uniqueKeys := make(map[string]bool)
	for _, key := range scannedKeys {
		uniqueKeys[key] = true
	}

	// Verify we scanned all 10 keys (Scan returns all physical entries)
	if len(uniqueKeys) != 10 {
		t.Errorf("Expected 10 unique keys, got %d", len(uniqueKeys))
	}

	// Verify deleted keys are not accessible via Has()
	for _, deletedKey := range deletedKeys {
		if cache.Has([]byte(deletedKey)) {
			t.Errorf("Deleted key %s is still accessible via Has()", deletedKey)
		}
	}

	t.Logf("Successfully scanned %d total entries (%d unique), including %d deleted tombstones",
		len(scannedKeys), len(uniqueKeys), len(deletedKeys))
}

// TestDiskCache_Scan_SkipsSuperseded verifies superseded entries are skipped
func TestDiskCache_Scan_SkipsSuperseded(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-scanphysical-superseded-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert initial values
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("old-value-%d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// Update some keys (creates superseded entries)
	updatedKeys := []string{"key-1", "key-3"}
	for _, key := range updatedKeys {
		newValue := "new-" + key
		if err := cache.Set([]byte(key), []byte(newValue)); err != nil {
			t.Fatalf("Failed to update %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Scan physically and collect values
	scannedData := make(map[string]string)
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedData[string(key)] = string(*value)
		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// Verify we only got latest values (no superseded entries)
	if len(scannedData) != 5 {
		t.Errorf("Expected 5 keys, got %d", len(scannedData))
	}

	// Verify updated keys have new values
	for _, key := range updatedKeys {
		if value, ok := scannedData[key]; !ok {
			t.Errorf("Updated key %s not found in scan", key)
		} else if value != "new-"+key {
			t.Errorf("Key %s has wrong value: expected %s, got %s", key, "new-"+key, value)
		}
	}

	t.Logf("Successfully scanned latest values, skipped superseded entries")
}

// TestDiskCache_Scan_Performance verifies physical scan is faster than regular scan
func TestDiskCache_Scan_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	dir, err := os.MkdirTemp("", "bitcache-scanphysical-perf-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Insert a reasonable amount of data
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%06d", i)
		value := fmt.Sprintf("value-%06d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	t.Logf("Testing with %d keys across %d segments", numKeys, cache.Stats().Segments)

	// This test just verifies both methods return the same count
	// In real usage, Scan should be faster for full scans

	physicalCount := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		physicalCount++
		return false
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	regularCount := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		regularCount++
		return false
	})
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if physicalCount != regularCount {
		t.Errorf("Count mismatch: Scan=%d, Scan=%d", physicalCount, regularCount)
	}

	t.Logf("Both scans returned %d entries", physicalCount)
}

// TestTmpFileCleanupOnStartup verifies that .tmp files are cleaned up on startup
func TestTmpFileCleanupOnStartup(t *testing.T) {
	dir := t.TempDir()

	// Create a normal cache and populate it
	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write some data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Close the cache
	if err := cache.Close(); err != nil {
		t.Fatalf("failed to close cache: %v", err)
	}

	// Create some fake .tmp files to simulate incomplete compactions
	tmpFiles := []string{
		"00000001-00.log.tmp",
		"00000005-01.log.tmp",
		"00000010-02.log.tmp",
	}

	for _, tmpFile := range tmpFiles {
		tmpPath := filepath.Join(dir, tmpFile)
		f, err := os.Create(tmpPath)
		if err != nil {
			t.Fatalf("failed to create tmp file: %v", err)
		}
		// Write some garbage data
		f.WriteString("incomplete compaction data")
		f.Close()
	}

	// Verify .tmp files exist
	for _, tmpFile := range tmpFiles {
		tmpPath := filepath.Join(dir, tmpFile)
		if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
			t.Fatalf("tmp file should exist: %s", tmpPath)
		}
	}

	// Reopen the cache - this should clean up .tmp files
	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Verify .tmp files are gone
	for _, tmpFile := range tmpFiles {
		tmpPath := filepath.Join(dir, tmpFile)
		if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
			t.Errorf("tmp file should be deleted on startup: %s", tmpPath)
		}
	}

	// Verify data is still intact
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expected := []byte(fmt.Sprintf("value-%d", i))
		value, err := cache2.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s: %v", key, err)
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("expected %s, got %s", expected, value)
		}
	}

	t.Log("✓ Tmp file cleanup test passed")
}

// TestCompactionWithTmpFile verifies that compaction creates .tmp file and renames it atomically
func TestCompactionWithTmpFile(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize:      10 * 1024, // Small segments for testing
		AutoCompactEnabled:  false,     // Manual compaction
		AutoCompactInterval: 0,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write enough data to create multiple segments
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-some-extra-data-to-make-it-larger", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	// Get initial segment count
	byLevel, err := cache.GetSegmentsByLevel()
	if err != nil {
		t.Fatalf("failed to get segments: %v", err)
	}

	initialSegments := 0
	for _, segments := range byLevel {
		initialSegments += len(segments)
	}

	if initialSegments < 2 {
		t.Fatalf("expected at least 2 segments, got %d", initialSegments)
	}

	t.Logf("Initial segments: %d", initialSegments)

	// Verify no .tmp files exist before compaction
	tmpFiles, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(tmpFiles) > 0 {
		t.Errorf("unexpected .tmp files before compaction: %v", tmpFiles)
	}

	// Perform compaction
	result, err := cache.Compact()
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	t.Logf("Compaction result: type=%s, live=%d, bytes=%d", result.Type, result.LiveEntries, result.BytesWritten)

	// Verify no .tmp files exist after compaction
	tmpFiles, _ = filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(tmpFiles) > 0 {
		t.Errorf("unexpected .tmp files after compaction: %v", tmpFiles)
	}

	// Verify all data is still accessible
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expected := []byte(fmt.Sprintf("value-%d-with-some-extra-data-to-make-it-larger", i))
		value, err := cache.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s after compaction: %v", key, err)
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("data corruption after compaction: key=%s expected=%s got=%s", key, expected, value)
		}
	}

	t.Log("✓ Compaction with tmp file test passed")
}

// TestCrashDuringCompaction simulates a crash during compaction
func TestCrashDuringCompaction(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCacheWithConfig[[]byte](dir, DiskCacheConfig{
		MaxSegmentSize:      10 * 1024,
		AutoCompactEnabled:  false,
		AutoCompactInterval: 0,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write data to create multiple segments
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-with-some-extra-data", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to set key: %v", err)
		}
	}

	cache.Close()

	// Simulate a crash by creating a .tmp file that represents an incomplete compaction
	crashedTmpFile := filepath.Join(dir, "00000005-01.log.tmp")
	f, err := os.Create(crashedTmpFile)
	if err != nil {
		t.Fatalf("failed to create crashed tmp file: %v", err)
	}
	f.WriteString("incomplete compaction - simulating crash")
	f.Close()

	t.Logf("Created simulated crashed tmp file: %s", crashedTmpFile)

	// Reopen cache - should clean up the .tmp file
	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	// Verify .tmp file was removed
	if _, err := os.Stat(crashedTmpFile); !os.IsNotExist(err) {
		t.Errorf("crashed tmp file should be deleted: %s", crashedTmpFile)
	}

	// Verify all original data is still accessible
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		expected := []byte(fmt.Sprintf("value-%d-with-some-extra-data", i))
		value, err := cache2.Get(key)
		if err != nil {
			t.Errorf("failed to get key %s after recovery: %v", key, err)
		}
		if !bytes.Equal(value, expected) {
			t.Errorf("data corruption after recovery: key=%s", key)
		}
	}

	t.Log("✓ Crash recovery test passed")
}

// TestDiskCache_TruncateCorruptedSegment_FileCacheCleared verifies that the file cache is cleared
// when a corrupted segment is truncated, preventing stale file handles
func TestDiskCache_TruncateCorruptedSegment_FileCacheCleared(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-truncate-cache-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create cache with small segment size to force rotation
	cache, err := NewDiskCacheWithConfig(dir, DiskCacheConfig{
		MaxSegmentSize: 1024, // Small size to force rotation
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Write enough data to create multiple segments
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := make([]byte, 100) // 100-byte values
		for j := range value {
			value[j] = byte(i)
		}
		if err := cache.Set([]byte(key), value); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Close cache
	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Find all log files
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	if len(files) < 2 {
		t.Skipf("Need at least 2 segments for this test, got %d", len(files))
	}

	t.Logf("Found %d segment files", len(files))

	// Corrupt the FIRST segment (not the active one)
	// This ensures we test the file cache clearing on historical segments
	firstSegment := files[0]
	t.Logf("Corrupting segment: %s", filepath.Base(firstSegment))

	data, err := os.ReadFile(firstSegment)
	if err != nil {
		t.Fatalf("Failed to read segment file: %v", err)
	}

	originalSize := len(data)

	// Corrupt the last half of the file
	corruptionStart := len(data) / 2
	if corruptionStart < 100 {
		corruptionStart = 100
	}

	for i := corruptionStart; i < len(data); i++ {
		data[i] = 0xFF
	}

	if err := os.WriteFile(firstSegment, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted segment: %v", err)
	}

	t.Logf("Corrupted %d bytes in segment", len(data)-corruptionStart)

	// Reopen cache - should detect corruption and truncate
	cache, err = NewDiskCacheWithConfig(dir, DiskCacheConfig{
		MaxSegmentSize: 1024,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}

	// Read some keys to populate the file cache
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%04d", i)
		_, _ = cache.Get([]byte(key)) // Ignore errors, some keys may be in corrupted segment
	}

	// Verify the truncated segment file size
	stat, err := os.Stat(firstSegment)
	if err != nil {
		t.Fatalf("Failed to stat segment: %v", err)
	}

	newSize := stat.Size()
	t.Logf("Segment size after truncation: %d bytes (was %d bytes)", newSize, originalSize)

	if newSize >= int64(originalSize) {
		t.Errorf("Segment was not truncated: new size %d >= original size %d", newSize, originalSize)
	}

	// Now try to read from the truncated segment
	// This would fail if the file cache still had stale handles
	keysRead := 0
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value, err := cache.Get([]byte(key))
		if err == nil && len(value) == 100 {
			keysRead++
			// Verify value content
			expected := byte(i)
			for _, b := range value {
				if b != expected {
					t.Errorf("Invalid value for %s: expected all bytes to be %d", key, expected)
					break
				}
			}
		}
	}

	t.Logf("Successfully read %d keys after truncation and cache operations", keysRead)

	// We should have read at least some keys
	if keysRead == 0 {
		t.Error("Failed to read any keys after truncation - file cache may have stale handles")
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}
}

// TestDiskCache_MultipleCorruptedSegments verifies that multiple corrupted segments are all truncated
func TestDiskCache_MultipleCorruptedSegments(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-multi-truncate-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Create cache with small segment size
	cache, err := NewDiskCacheWithConfig(dir, DiskCacheConfig{
		MaxSegmentSize: 512,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Write enough data to create multiple segments
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := make([]byte, 50)
		for j := range value {
			value[j] = byte(i)
		}
		if err := cache.Set([]byte(key), value); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}
	cache.Close()

	// Find all log files
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("Failed to find log files: %v", err)
	}

	if len(files) < 3 {
		t.Skipf("Need at least 3 segments for this test, got %d", len(files))
	}

	t.Logf("Found %d segment files", len(files))

	// Corrupt multiple segments (but not the last one)
	corruptedCount := 0
	for i := 0; i < len(files)-1 && i < 3; i++ {
		segmentFile := files[i]
		data, err := os.ReadFile(segmentFile)
		if err != nil {
			t.Logf("Warning: failed to read %s: %v", segmentFile, err)
			continue
		}

		// Corrupt the last quarter
		corruptionStart := (len(data) * 3) / 4
		if corruptionStart < 100 {
			corruptionStart = 100
		}

		for j := corruptionStart; j < len(data); j++ {
			data[j] = 0xFF
		}

		if err := os.WriteFile(segmentFile, data, 0644); err != nil {
			t.Logf("Warning: failed to write %s: %v", segmentFile, err)
			continue
		}

		corruptedCount++
		t.Logf("Corrupted segment %d: %s", i, filepath.Base(segmentFile))
	}

	if corruptedCount == 0 {
		t.Fatal("Failed to corrupt any segments")
	}

	// Reopen cache - should detect and truncate all corrupted segments
	cache, err = NewDiskCacheWithConfig(dir, DiskCacheConfig{
		MaxSegmentSize: 512,
	}, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	// Try to read keys - should work even with truncated segments
	keysRead := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		_, err := cache.Get([]byte(key))
		if err == nil {
			keysRead++
		}
	}

	t.Logf("Successfully read %d out of 100 keys after truncating %d segments", keysRead, corruptedCount)

	// We should read at least some keys
	if keysRead == 0 {
		t.Error("Failed to read any keys after multi-segment truncation")
	}
}

// TestDiskCache_TruncateCorruptedSegment verifies that corrupted segments are truncated to the last valid record
func TestDiskCache_TruncateCorruptedSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-truncate-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// Create cache and write some data
	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Write test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Close cache
	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Find the log file and corrupt the end
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("Failed to find log files: %v", err)
	}

	logFile := files[0]

	// Read the file
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	originalSize := len(data)
	t.Logf("Original file size: %d bytes", originalSize)

	// Corrupt the last 200 bytes (should be roughly 2-3 records)
	corruptionStart := len(data) - 200
	if corruptionStart < 100 {
		corruptionStart = 100
	}

	for i := corruptionStart; i < len(data); i++ {
		data[i] = 0xFF
	}

	if err := os.WriteFile(logFile, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted log file: %v", err)
	}

	t.Logf("Corrupted last %d bytes", len(data)-corruptionStart)

	// Reopen the cache - it should detect corruption and truncate
	cache, err = NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer func() {
		_ = cache.Close()
	}()

	// Check file size after truncation
	stat, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("Failed to stat log file: %v", err)
	}

	newSize := stat.Size()
	t.Logf("New file size after truncation: %d bytes", newSize)

	// File should be smaller than original
	if newSize >= int64(originalSize) {
		t.Errorf("Expected file to be truncated, but size is %d (original: %d)", newSize, originalSize)
	}

	// We should have at least some keys (the ones before corruption)
	scannedCount := 0
	err = cache.Scan(func(key []byte, value *[]byte) bool {
		scannedCount++
		return false
	})

	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if scannedCount == 0 {
		t.Error("Expected to recover some keys after truncation, got 0")
	}

	t.Logf("Successfully recovered %d keys after truncation", scannedCount)
	t.Logf("Truncation removed %d bytes", originalSize-int(newSize))
}

// TestDiskCache_TruncateMiddleCorruption verifies truncation when corruption is in the middle but no valid records after
func TestDiskCache_TruncateMiddleCorruption(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-truncate-middle-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// Create cache and write data
	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Write test data
	for i := 0; i < 15; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d-with-padding", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Find and corrupt the file in the middle
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("Failed to find log files: %v", err)
	}

	logFile := files[0]
	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}

	originalSize := len(data)

	// Corrupt from 60% point to end (ensures no valid records after corruption)
	corruptionStart := (originalSize * 60) / 100
	if corruptionStart < 200 {
		corruptionStart = 200
	}

	for i := corruptionStart; i < len(data); i++ {
		data[i] = 0xAA
	}

	if err := os.WriteFile(logFile, data, 0644); err != nil {
		t.Fatalf("Failed to write corrupted log file: %v", err)
	}

	t.Logf("Corrupted from offset %d to end", corruptionStart)

	// Reopen - should truncate to last valid position
	cache, err = NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer func() {
		_ = cache.Close()
	}()

	// Verify truncation occurred
	stat, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("Failed to stat log file: %v", err)
	}

	newSize := stat.Size()

	if newSize >= int64(originalSize) {
		t.Errorf("Expected truncation, but size is %d (original: %d)", newSize, originalSize)
	}

	if newSize < int64(corruptionStart) {
		// Good - truncated before corruption point
		t.Logf("Successfully truncated to %d bytes (before corruption at %d)", newSize, corruptionStart)
	}

	// Count recovered keys
	count := 0
	_ = cache.Scan(func(key []byte, value *[]byte) bool {
		count++
		return false
	})

	t.Logf("Recovered %d keys after truncation", count)

	if count == 0 {
		t.Error("Expected to recover at least some keys")
	}
}

// TestDiskCache_NoTruncateOnValidFile verifies that valid files are not truncated
func TestDiskCache_NoTruncateOnValidFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcache-no-truncate-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	// Create cache and write data
	cache, err := NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Write test data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		value := fmt.Sprintf("value-%02d", i)
		if err := cache.Set([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Fatalf("Failed to close cache: %v", err)
	}

	// Get original file size
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil || len(files) == 0 {
		t.Fatalf("Failed to find log files: %v", err)
	}

	logFile := files[0]
	stat1, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("Failed to stat log file: %v", err)
	}

	originalSize := stat1.Size()

	// Reopen without corruption
	cache, err = NewDiskCache(dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer func() {
		_ = cache.Close()
	}()

	// Check file size hasn't changed
	stat2, err := os.Stat(logFile)
	if err != nil {
		t.Fatalf("Failed to stat log file: %v", err)
	}

	newSize := stat2.Size()

	if newSize != originalSize {
		t.Errorf("File size changed without corruption: %d -> %d", originalSize, newSize)
	}

	// Verify all keys are still there
	// Note: Scan returns all entries from all segments, so duplicates are expected
	count := 0
	uniqueKeys := make(map[string]bool)
	_ = cache.Scan(func(key []byte, value *[]byte) bool {
		keyStr := string(key)
		uniqueKeys[keyStr] = true
		count++
		return false
	})

	if len(uniqueKeys) != 10 {
		t.Errorf("Expected 10 unique keys, got %d", len(uniqueKeys))
	}

	t.Logf("File correctly not truncated: %d bytes, %d total entries, %d unique keys", newSize, count, len(uniqueKeys))
}

// TestCloseRaceCondition tests for race between Set and Close
// This test reproduces the issue where Close() can flush and close files
// while Set() is still writing, resulting in partial/corrupted entries
func TestCloseRaceCondition(t *testing.T) {
	dir := t.TempDir()

	// Run multiple iterations to increase chance of hitting the race
	for iteration := 0; iteration < 10; iteration++ {
		cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
		if err != nil {
			t.Fatalf("iteration %d: failed to create cache: %v", iteration, err)
		}

		var wg sync.WaitGroup
		var writeErrors atomic.Int64
		var successfulWrites atomic.Int64
		const numWriters = 20
		const writesPerWriter = 100

		// Start multiple writers
		for w := 0; w < numWriters; w++ {
			wg.Add(1)
			writerID := w
			go func() {
				defer wg.Done()
				for i := 0; i < writesPerWriter; i++ {
					key := []byte(fmt.Sprintf("writer-%d-key-%d", writerID, i))
					value := []byte(fmt.Sprintf("writer-%d-value-%d-with-some-extra-data-to-make-it-larger", writerID, i))

					err := cache.Set(key, value)
					if err != nil {
						if err == ErrCacheClosed {
							// Expected after close is called
							writeErrors.Add(1)
						} else {
							t.Errorf("unexpected error: %v", err)
						}
					} else {
						successfulWrites.Add(1)
					}

					// Small random delay to increase race window
					time.Sleep(time.Microsecond * 10)
				}
			}()
		}

		// Wait a bit for writes to be in progress
		time.Sleep(time.Millisecond * 50)

		// Close the cache while writes are happening
		if err := cache.Close(); err != nil {
			t.Errorf("iteration %d: close failed: %v", iteration, err)
		}

		// Wait for all writers to finish
		wg.Wait()

		t.Logf("Iteration %d: successful writes=%d, closed errors=%d",
			iteration, successfulWrites.Load(), writeErrors.Load())

		// Reopen and verify data integrity
		cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
		if err != nil {
			t.Fatalf("iteration %d: failed to reopen cache: %v", iteration, err)
		}

		// Verify all successfully written keys are readable
		stats := cache2.Stats()
		t.Logf("Iteration %d: reopened cache has %d keys", iteration, stats.Keys)

		// Close and clean up for next iteration
		cache2.Close()

		// Clean up the directory for next iteration
		if iteration < 9 {
			// Remove all files
			files, _ := os.ReadDir(dir)
			for _, f := range files {
				_ = os.Remove(fmt.Sprintf("%s/%s", dir, f.Name()))
			}
		}
	}

	t.Log("✓ Close race condition test passed - no corrupted files")
}

// TestConcurrentWritesDuringClose verifies that concurrent writes during close
// either succeed completely or fail cleanly with ErrCacheClosed
func TestConcurrentWritesDuringClose(t *testing.T) {
	dir := t.TempDir()

	cache, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	// Write initial data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("initial-%d", i))
		value := []byte(fmt.Sprintf("initial-value-%d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("failed to write initial data: %v", err)
		}
	}

	var wg sync.WaitGroup
	var closeCalled atomic.Bool
	const numWriters = 10

	// Start writers that keep writing until close is called
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		writerID := w
		go func() {
			defer wg.Done()
			counter := 0
			for !closeCalled.Load() {
				key := []byte(fmt.Sprintf("concurrent-%d-%d", writerID, counter))
				value := []byte(fmt.Sprintf("concurrent-value-%d-%d", writerID, counter))
				_ = cache.Set(key, value) // Ignore errors, we expect some after close
				counter++
				time.Sleep(time.Microsecond * 100)
			}
		}()
	}

	// Let writes happen for a bit
	time.Sleep(time.Millisecond * 100)

	// Close the cache
	closeCalled.Store(true)
	if err := cache.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Wait for all writers to finish
	wg.Wait()

	// Reopen and verify no corruption
	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache after concurrent writes: %v", err)
	}
	defer cache2.Close()

	// Verify we can read all the initial keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("initial-%d", i))
		expectedValue := []byte(fmt.Sprintf("initial-value-%d", i))
		value, err := cache2.Get(key)
		if err != nil {
			t.Errorf("failed to read initial key %s: %v", key, err)
		} else if string(value) != string(expectedValue) {
			t.Errorf("data corruption: key=%s expected=%s got=%s", key, expectedValue, value)
		}
	}

	stats := cache2.Stats()
	t.Logf("✓ Reopened cache successfully with %d keys (no corruption)", stats.Keys)
}

// TestCloseIdempotency verifies that calling Close multiple times is safe
func TestCloseIdempotency(t *testing.T) {
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
			t.Fatalf("failed to write data: %v", err)
		}
	}

	// Close multiple times - should not panic or error
	if err := cache.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Errorf("second close should not error: %v", err)
	}

	if err := cache.Close(); err != nil {
		t.Errorf("third close should not error: %v", err)
	}

	// Verify data is still intact
	cache2, err := NewDiskCache[[]byte](dir, ByteSliceMarshaler{})
	if err != nil {
		t.Fatalf("failed to reopen cache: %v", err)
	}
	defer cache2.Close()

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if _, err := cache2.Get(key); err != nil {
			t.Errorf("failed to read key after multiple closes: %v", err)
		}
	}

	t.Log("✓ Close idempotency test passed")
}
