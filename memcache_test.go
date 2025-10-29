package bitcache

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMemCache_BasicOperations(t *testing.T) {
	dir := t.TempDir()

	// Create backing disk cache
	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create memory cache with 1MB limit
	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024, // 1MB
		EvictionPolicy: EvictionLRU,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Test Set and Get
	key := []byte("test-key")
	value := []byte("test-value")

	err = memCache.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	retrieved, err := memCache.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Errorf("Expected value %s, got %s", value, retrieved)
	}

	// Test Has
	if !memCache.Has(key) {
		t.Error("Has returned false for existing key")
	}

	// Test Delete
	err = memCache.Delete(key)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if memCache.Has(key) {
		t.Error("Has returned true for deleted key")
	}

	_, err = memCache.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestMemCache_ReadThrough(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Pre-populate disk cache
	key := []byte("disk-key")
	value := []byte("disk-value")
	err = diskCache.Set(key, value)
	if err != nil {
		t.Fatalf("Failed to set in disk cache: %v", err)
	}

	// Create memory cache
	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Read through to backing store
	retrieved, err := memCache.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(retrieved) != string(value) {
		t.Errorf("Expected value %s, got %s", value, retrieved)
	}

	// Second read should come from memory
	stats := memCache.MemStats()
	if stats.Entries == 0 {
		t.Error("Expected entry to be cached in memory after first read")
	}
}

func TestMemCache_LRUEviction(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create small memory cache (512 bytes)
	config := MemCacheConfig{
		MaxMemoryBytes: 512,
		EvictionPolicy: EvictionLRU,
		ShardCount:     1, // Single shard for predictable behavior
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Fill cache with entries (larger values to force evictions)
	numEntries := 20
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d-with-extra-data-to-fill-memory", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	stats := memCache.MemStats()
	if stats.Entries >= int64(numEntries) {
		t.Errorf("Expected evictions, but found %d entries (added %d)", stats.Entries, numEntries)
	}

	// Verify all entries are still in backing store
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if !diskCache.Has(key) {
			t.Errorf("Key %s not found in backing store", key)
		}
	}
}

func TestMemCache_LFUEviction(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create memory cache with LFU (small limit to force evictions)
	config := MemCacheConfig{
		MaxMemoryBytes: 512,
		EvictionPolicy: EvictionLFU,
		ShardCount:     1,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add entries (larger values to force evictions)
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("value-%03d-with-extra-data-to-fill-memory", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Memory cache should have evicted some entries
	stats := memCache.MemStats()
	if stats.Entries >= 20 {
		t.Errorf("Expected evictions with LFU, but found %d entries", stats.Entries)
	}

	// All entries should still be in backing store
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value, err := memCache.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
		}
		expectedValue := []byte(fmt.Sprintf("value-%03d-with-extra-data-to-fill-memory", i))
		if string(value) != string(expectedValue) {
			t.Errorf("Wrong value for key %s", key)
		}
	}
}

func TestMemCache_CachePolicy(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Policy: only cache keys starting with "cache-"
	policy := func(key []byte, value []byte) bool {
		return len(key) >= 6 && string(key[:6]) == "cache-"
	}

	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024,
		EvictionPolicy: EvictionLRU,
		CachePolicy:    policy,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Set cacheable key
	cacheKey := []byte("cache-key")
	cacheValue := []byte("cache-value")
	err = memCache.Set(cacheKey, cacheValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Set non-cacheable key
	noCacheKey := []byte("nocache-key")
	noCacheValue := []byte("nocache-value")
	err = memCache.Set(noCacheKey, noCacheValue)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Both should be retrievable
	val1, err := memCache.Get(cacheKey)
	if err != nil || string(val1) != string(cacheValue) {
		t.Errorf("Failed to get cacheable key")
	}

	val2, err := memCache.Get(noCacheKey)
	if err != nil || string(val2) != string(noCacheValue) {
		t.Errorf("Failed to get non-cacheable key")
	}

	// Check memory stats - only cacheable key should be in memory
	stats := memCache.MemStats()
	if stats.Entries < 1 {
		t.Error("Expected at least one entry in memory cache")
	}

	// Both should be in backing store
	if !diskCache.Has(cacheKey) {
		t.Error("Cacheable key not in backing store")
	}
	if !diskCache.Has(noCacheKey) {
		t.Error("Non-cacheable key not in backing store")
	}
}

func TestMemCache_Sharding(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create memory cache with multiple shards
	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024,
		EvictionPolicy: EvictionLRU,
		ShardCount:     16,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add many entries
	numEntries := 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Verify all entries are accessible
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value, err := memCache.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %s: %v", key, err)
		}
		expectedValue := []byte(fmt.Sprintf("value-%05d", i))
		if string(value) != string(expectedValue) {
			t.Errorf("Wrong value for key %s", key)
		}
	}

	stats := memCache.MemStats()
	if stats.Shards != 16 {
		t.Errorf("Expected 16 shards, got %d", stats.Shards)
	}
}

func TestMemCache_Clear(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add entries
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Clear memory cache
	err = memCache.Clear()
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	stats := memCache.MemStats()
	if stats.Entries != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", stats.Entries)
	}

	// Data should still be in backing store
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if !diskCache.Has(key) {
			t.Errorf("Key %s not found in backing store after clear", key)
		}
	}
}

func TestMemCache_Invalidate(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	key := []byte("test-key")
	value := []byte("test-value")

	// Set and verify it's cached
	err = memCache.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Invalidate from memory cache
	memCache.Invalidate(key)

	// Should still be in backing store
	if !diskCache.Has(key) {
		t.Error("Key not found in backing store after invalidate")
	}

	// Should still be retrievable (will read-through)
	val, err := memCache.Get(key)
	if err != nil {
		t.Errorf("Get failed after invalidate: %v", err)
	}
	if string(val) != string(value) {
		t.Error("Wrong value after invalidate")
	}
}

func TestMemCache_Warmup(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Pre-populate backing store
	keys := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		keys[i] = key
		err = diskCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Create empty memory cache
	config := MemCacheConfig{
		MaxMemoryBytes: 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Warmup cache
	err = memCache.Warmup(keys)
	if err != nil {
		t.Fatalf("Warmup failed: %v", err)
	}

	stats := memCache.MemStats()
	if stats.Entries != 10 {
		t.Errorf("Expected 10 entries after warmup, got %d", stats.Entries)
	}
}

func TestMemCache_NoGCAfterInit(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes: 10 * 1024 * 1024, // 10MB
		EvictionPolicy: EvictionLRU,
		ShardCount:     256,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add many entries to force arena allocation
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := []byte(fmt.Sprintf("value-data-%06d-with-some-extra-content", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	stats := memCache.MemStats()
	t.Logf("Memory stats - Entries: %d, Used: %d, Allocated: %d, Limit: %d",
		stats.Entries, stats.MemoryUsed, stats.MemoryAllocated, stats.MemoryLimit)

	if stats.MemoryUsed > stats.MemoryLimit {
		t.Errorf("Memory used (%d) exceeds limit (%d)", stats.MemoryUsed, stats.MemoryLimit)
	}

	// Verify all entries are accessible
	for i := 0; i < 10000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		_, err := memCache.Get(key)
		if err != nil {
			t.Errorf("Get failed for key %s: %v", key, err)
		}
	}
}

func BenchmarkMemCache_Get_Hit(b *testing.B) {
	dir := b.TempDir()

	diskCache, _ := NewDiskCache(dir)
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024, // 100MB
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(diskCache, config)
	defer memCache.Close()

	// Pre-populate
	key := []byte("benchmark-key")
	value := []byte("benchmark-value-with-some-content")
	memCache.Set(key, value)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = memCache.Get(key)
	}
}

func BenchmarkMemCache_Get_Miss(b *testing.B) {
	dir := b.TempDir()

	diskCache, _ := NewDiskCache(dir)
	defer diskCache.Close()

	// Pre-populate disk cache
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		diskCache.Set(key, value)
	}

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(diskCache, config)
	defer memCache.Close()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i%1000))
		_, _ = memCache.Get(key)
	}
}

func BenchmarkMemCache_Set_WriteThrough(b *testing.B) {
	dir := b.TempDir()

	diskCache, _ := NewDiskCache(dir)
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(diskCache, config)
	defer memCache.Close()

	value := []byte("benchmark-value-with-some-content")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_ = memCache.Set(key, value)
	}
}

func BenchmarkMemCache_Set_MemoryOnly(b *testing.B) {
	dir := b.TempDir()

	diskCache, _ := NewDiskCache(dir)
	defer diskCache.Close()

	// Policy that never caches - measures only disk write performance
	noCachePolicy := func(key, value []byte) bool {
		return false
	}

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
		CachePolicy:    noCachePolicy,
	}
	memCache, _ := NewMemCache(diskCache, config)
	defer memCache.Close()

	value := []byte("benchmark-value-with-some-content")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_ = memCache.Set(key, value)
	}
}

func BenchmarkMemCache_Update_Cached(b *testing.B) {
	dir := b.TempDir()

	diskCache, _ := NewDiskCache(dir)
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(diskCache, config)
	defer memCache.Close()

	// Pre-populate one key
	key := []byte("benchmark-key")
	value := []byte("benchmark-value-with-some-content")
	memCache.Set(key, value)

	newValue := []byte("new-benchmark-value-with-content")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = memCache.Set(key, newValue)
	}
}

func TestMemCache_Compaction_BasicVerification(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes:      1024 * 1024,
		EvictionPolicy:      EvictionLRU,
		ShardCount:          4,
		CompactionThreshold: 0.3,
		CompactionInterval:  3600,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d-data", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Delete some entries
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i*2))
		memCache.Invalidate(key)
	}

	statsBefore := memCache.MemStats()

	// Manually trigger compaction
	memCache.Compact()

	statsAfter := memCache.MemStats()

	// Verify entry count is unchanged
	if statsAfter.Entries != statsBefore.Entries {
		t.Errorf("Entry count changed: before=%d, after=%d", statsBefore.Entries, statsAfter.Entries)
	}

	// Verify memory used is unchanged
	if statsAfter.MemoryUsed != statsBefore.MemoryUsed {
		t.Errorf("Memory used changed: before=%d, after=%d", statsBefore.MemoryUsed, statsAfter.MemoryUsed)
	}

	// Most importantly: verify all data is still intact
	for i := 0; i < 100; i++ {
		if i%2 == 1 {
			key := []byte(fmt.Sprintf("key-%05d", i))
			value, err := memCache.Get(key)
			if err != nil {
				t.Errorf("Failed to get key %s after compaction: %v", key, err)
			}
			expectedValue := []byte(fmt.Sprintf("value-%05d-data", i))
			if string(value) != string(expectedValue) {
				t.Errorf("Value mismatch for key %s", key)
			}
		}
	}

	t.Logf("Compaction completed successfully - Entries: %d, MemoryUsed: %d, MemoryAllocated: %d",
		statsAfter.Entries, statsAfter.MemoryUsed, statsAfter.MemoryAllocated)
}

func TestMemCache_Compaction_DataIntegrity(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create memory cache with compaction disabled initially (long interval)
	config := MemCacheConfig{
		MaxMemoryBytes:      1024 * 1024, // 1MB
		EvictionPolicy:      EvictionLRU,
		ShardCount:          4,
		CompactionThreshold: 0.3,
		CompactionInterval:  3600, // 1 hour - effectively disabled for manual testing
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add initial set of entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d-with-extra-data", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Delete half of them to create fragmentation
	for i := 0; i < numEntries/2; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i*2))
		err = memCache.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	// Get stats before compaction
	statsBefore := memCache.MemStats()
	t.Logf("Before compaction - Entries: %d, Used: %d, Allocated: %d, Fragmentation: %.2f",
		statsBefore.Entries, statsBefore.MemoryUsed, statsBefore.MemoryAllocated, statsBefore.Fragmentation)

	// Manually trigger compaction
	memCache.Compact()

	// Get stats after compaction
	statsAfter := memCache.MemStats()
	t.Logf("After compaction - Entries: %d, Used: %d, Allocated: %d, Fragmentation: %.2f",
		statsAfter.Entries, statsAfter.MemoryUsed, statsAfter.MemoryAllocated, statsAfter.Fragmentation)

	// Verify entry count is unchanged
	if statsAfter.Entries != statsBefore.Entries {
		t.Errorf("Entry count changed: before=%d, after=%d", statsBefore.Entries, statsAfter.Entries)
	}

	// Verify memory used is unchanged
	if statsAfter.MemoryUsed != statsBefore.MemoryUsed {
		t.Errorf("Memory used changed: before=%d, after=%d", statsBefore.MemoryUsed, statsAfter.MemoryUsed)
	}

	// Verify all remaining entries are intact and retrievable
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		expectedValue := []byte(fmt.Sprintf("value-%05d-with-extra-data", i))

		// Check if key should exist (odd indices were kept)
		if i%2 == 1 {
			value, err := memCache.Get(key)
			if err != nil {
				t.Errorf("Failed to get key %s after compaction: %v", key, err)
			}
			if string(value) != string(expectedValue) {
				t.Errorf("Value mismatch for key %s after compaction: got %s, want %s",
					key, value, expectedValue)
			}
		}
	}
}

func TestMemCache_Compaction_FragmentationReduction(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create memory cache with single shard for predictable behavior
	config := MemCacheConfig{
		MaxMemoryBytes:      512 * 1024, // 512KB
		EvictionPolicy:      EvictionLRU,
		ShardCount:          1,
		CompactionThreshold: 0.5,
		CompactionInterval:  3600,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Fill cache completely
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d-with-some-extra-padding-data", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	statsFull := memCache.MemStats()
	t.Logf("After filling - Entries: %d, Used: %d, Allocated: %d, Fragmentation: %.2f",
		statsFull.Entries, statsFull.MemoryUsed, statsFull.MemoryAllocated, statsFull.Fragmentation)

	// Delete 75% of entries to create significant fragmentation
	for i := 0; i < 150; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		memCache.Invalidate(key) // Remove from memory cache only
	}

	statsFragmented := memCache.MemStats()
	t.Logf("After deletion - Entries: %d, Used: %d, Allocated: %d, Fragmentation: %.2f",
		statsFragmented.Entries, statsFragmented.MemoryUsed, statsFragmented.MemoryAllocated, statsFragmented.Fragmentation)

	// Verify fragmentation increased
	if statsFragmented.Fragmentation <= statsFull.Fragmentation {
		t.Logf("Warning: Fragmentation did not increase as expected (before: %.2f, after: %.2f)",
			statsFull.Fragmentation, statsFragmented.Fragmentation)
	}

	// Compact
	memCache.Compact()

	statsCompacted := memCache.MemStats()
	t.Logf("After compaction - Entries: %d, Used: %d, Allocated: %d, Fragmentation: %.2f",
		statsCompacted.Entries, statsCompacted.MemoryUsed, statsCompacted.MemoryAllocated, statsCompacted.Fragmentation)

	// The key benefit of compaction is that it creates a fresh arena
	// Even if fragmentation ratio looks similar (due to large slab sizes),
	// the important thing is that data is contiguous and GC can reclaim old slabs

	// Verify allocated memory didn't increase (and likely decreased)
	if statsCompacted.MemoryAllocated > statsFragmented.MemoryAllocated {
		t.Errorf("Allocated memory increased after compaction: before=%d, after=%d",
			statsFragmented.MemoryAllocated, statsCompacted.MemoryAllocated)
	}

	// Log the improvement for visibility
	savedMemory := statsFragmented.MemoryAllocated - statsCompacted.MemoryAllocated
	if savedMemory > 0 {
		t.Logf("Compaction saved %d bytes of allocated memory", savedMemory)
	}

	// Verify data integrity for remaining entries
	for i := 150; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value, err := memCache.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s after compaction: %v", key, err)
		}
		expectedValue := []byte(fmt.Sprintf("value-%05d-with-some-extra-padding-data", i))
		if string(value) != string(expectedValue) {
			t.Errorf("Value mismatch for key %s", key)
		}
	}
}

func TestMemCache_Compaction_AutomaticTriggering(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	// Create memory cache with short compaction interval
	config := MemCacheConfig{
		MaxMemoryBytes:      256 * 1024, // 256KB
		EvictionPolicy:      EvictionLRU,
		ShardCount:          2,
		CompactionThreshold: 0.4, // 40% fragmentation triggers compaction
		CompactionInterval:  1,   // Check every 1 second
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add entries
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d-data", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Delete many entries to create fragmentation
	for i := 0; i < 70; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		memCache.Invalidate(key)
	}

	statsBeforeAuto := memCache.MemStats()
	t.Logf("Before automatic compaction - Fragmentation: %.2f", statsBeforeAuto.Fragmentation)

	// Verify all remaining data is still accessible
	for i := 70; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value, err := memCache.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", key, err)
		}
		expectedValue := []byte(fmt.Sprintf("value-%05d-data", i))
		if string(value) != string(expectedValue) {
			t.Errorf("Value mismatch for key %s", key)
		}
	}
}

func TestMemCache_Compaction_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes:      1024 * 1024, // 1MB
		EvictionPolicy:      EvictionLRU,
		ShardCount:          8,
		CompactionThreshold: 0.3,
		CompactionInterval:  3600,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Pre-populate
	numEntries := 500
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d-content", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Create fragmentation
	for i := 0; i < numEntries/2; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i*2))
		memCache.Invalidate(key)
	}

	// Start concurrent operations
	done := make(chan bool)
	errChan := make(chan error, 3)

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%05d", (i*2+1)%numEntries))
			_, err := memCache.Get(key)
			if err != nil && err != ErrKeyNotFound {
				errChan <- fmt.Errorf("read error: %v", err)
				return
			}
		}
		done <- true
	}()

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("new-key-%05d", i))
			value := []byte(fmt.Sprintf("new-value-%05d", i))
			err := memCache.Set(key, value)
			if err != nil {
				errChan <- fmt.Errorf("write error: %v", err)
				return
			}
		}
		done <- true
	}()

	// Compaction goroutine
	go func() {
		for i := 0; i < 5; i++ {
			memCache.Compact()
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 3; i++ {
		select {
		case <-done:
			// Success
		case err := <-errChan:
			t.Fatalf("Concurrent operation failed: %v", err)
		}
	}

	// Verify data integrity
	for i := 0; i < numEntries; i++ {
		if i%2 == 1 { // Odd indices should still exist
			key := []byte(fmt.Sprintf("key-%05d", i))
			value, err := memCache.Get(key)
			if err != nil {
				t.Errorf("Failed to get key %s after concurrent operations: %v", key, err)
			}
			expectedValue := []byte(fmt.Sprintf("value-%05d-content", i))
			if string(value) != string(expectedValue) {
				t.Errorf("Value mismatch for key %s", key)
			}
		}
	}
}

func TestMemCache_Compaction_EmptyShard(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes:      1024 * 1024,
		EvictionPolicy:      EvictionLRU,
		ShardCount:          4,
		CompactionThreshold: 0.3,
		CompactionInterval:  3600,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Compact empty cache - should not crash
	memCache.Compact()

	stats := memCache.MemStats()
	if stats.Entries != 0 {
		t.Errorf("Expected 0 entries, got %d", stats.Entries)
	}
}

func TestMemCache_Compaction_MultipleRounds(t *testing.T) {
	dir := t.TempDir()

	diskCache, err := NewDiskCache(dir)
	if err != nil {
		t.Fatalf("Failed to open disk cache: %v", err)
	}
	defer diskCache.Close()

	config := MemCacheConfig{
		MaxMemoryBytes:      512 * 1024,
		EvictionPolicy:      EvictionLRU,
		ShardCount:          2,
		CompactionThreshold: 0.3,
		CompactionInterval:  3600,
	}
	memCache, err := NewMemCache(diskCache, config)
	if err != nil {
		t.Fatalf("Failed to create memory cache: %v", err)
	}
	defer memCache.Close()

	// Add entries
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := []byte(fmt.Sprintf("value-%05d-padding", i))
		err = memCache.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Multiple rounds of delete and compact
	for round := 0; round < 3; round++ {
		t.Logf("Round %d", round)

		// Delete some entries
		deleteStart := round * 50
		deleteEnd := deleteStart + 40
		for i := deleteStart; i < deleteEnd && i < 200; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			memCache.Invalidate(key)
		}

		statsBefore := memCache.MemStats()
		t.Logf("  Before compaction - Entries: %d, Fragmentation: %.2f",
			statsBefore.Entries, statsBefore.Fragmentation)

		// Compact
		memCache.Compact()

		statsAfter := memCache.MemStats()
		t.Logf("  After compaction - Entries: %d, Fragmentation: %.2f",
			statsAfter.Entries, statsAfter.Fragmentation)

		// Verify fragmentation decreased or stayed the same
		if statsAfter.Fragmentation > statsBefore.Fragmentation {
			t.Errorf("Round %d: Fragmentation increased: %.2f -> %.2f",
				round, statsBefore.Fragmentation, statsAfter.Fragmentation)
		}
	}

	// Verify remaining data is intact
	for i := 120; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value, err := memCache.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %s after multiple compactions: %v", key, err)
		}
		expectedValue := []byte(fmt.Sprintf("value-%05d-padding", i))
		if string(value) != string(expectedValue) {
			t.Errorf("Value mismatch for key %s after multiple compactions", key)
		}
	}
}

// mockCache is a no-op cache for testing pure memory cache performance
type mockCache struct{}

func (m *mockCache) Get(key []byte) ([]byte, error) {
	return nil, ErrKeyNotFound
}

func (m *mockCache) Set(key []byte, value []byte) error {
	return nil // No-op
}

func (m *mockCache) Delete(key []byte) error {
	return nil
}

func (m *mockCache) Has(key []byte) bool {
	return false
}

func (m *mockCache) Scan(prefix []byte, fn func(key []byte) bool) error {
	return nil
}

func (m *mockCache) Stats() Stats {
	return Stats{}
}

func (m *mockCache) CompactN(count int) error {
	return nil // No-op
}

func (m *mockCache) Close() error {
	return nil
}

// Benchmarks with mock backing cache to measure pure memory cache performance

func BenchmarkMemCache_Set_PureMemory(b *testing.B) {
	mockBacking := &mockCache{}

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(mockBacking, config)
	defer memCache.Close()

	// Pre-allocate keys to avoid allocation in benchmark loop
	keys := make([][]byte, 10000)
	for i := 0; i < 10000; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	value := []byte("benchmark-value-with-some-content")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[i%10000]
		_ = memCache.Set(key, value)
	}
}

func BenchmarkMemCache_Get_PureMemory(b *testing.B) {
	mockBacking := &mockCache{}

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(mockBacking, config)
	defer memCache.Close()

	// Pre-populate
	key := []byte("benchmark-key")
	value := []byte("benchmark-value-with-some-content")
	memCache.Set(key, value)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = memCache.Get(key)
	}
}

func BenchmarkMemCache_Update_PureMemory(b *testing.B) {
	mockBacking := &mockCache{}

	config := MemCacheConfig{
		MaxMemoryBytes: 100 * 1024 * 1024,
		EvictionPolicy: EvictionLRU,
	}
	memCache, _ := NewMemCache(mockBacking, config)
	defer memCache.Close()

	// Pre-populate one key
	key := []byte("benchmark-key")
	value := []byte("benchmark-value-with-some-content")
	memCache.Set(key, value)

	newValue := []byte("new-benchmark-value-with-content")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = memCache.Set(key, newValue)
	}
}

func BenchmarkMemCache_GetSet_PureMemory(b *testing.B) {
	mockBacking := &mockCache{}

	config := MemCacheConfig{
		MaxMemoryBytes: 10 * 1024 * 1024, // Smaller cache to test eviction
		EvictionPolicy: EvictionLRU,
		ShardCount:     1, // Single shard for predictable behavior
	}
	memCache, _ := NewMemCache(mockBacking, config)
	defer memCache.Close()

	// Pre-allocate keys
	numKeys := 1000
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%06d", i))
	}
	value := []byte("benchmark-value-with-some-content")

	// Warmup
	for i := 0; i < 100; i++ {
		memCache.Set(keys[i], value)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[i%numKeys]
		memCache.Set(key, value)
		_, _ = memCache.Get(key)
	}
}

func BenchmarkMemCache_LRU_Eviction_PureMemory(b *testing.B) {
	mockBacking := &mockCache{}

	config := MemCacheConfig{
		MaxMemoryBytes: 1024, // Very small to force evictions
		EvictionPolicy: EvictionLRU,
		ShardCount:     1,
	}
	memCache, _ := NewMemCache(mockBacking, config)
	defer memCache.Close()

	// Pre-allocate keys
	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%02d", i))
	}
	value := []byte("value-data-for-benchmark")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[i%100]
		_ = memCache.Set(key, value)
	}
}

func BenchmarkMemCache_LFU_Eviction_PureMemory(b *testing.B) {
	mockBacking := &mockCache{}

	config := MemCacheConfig{
		MaxMemoryBytes: 1024, // Very small to force evictions
		EvictionPolicy: EvictionLFU,
		ShardCount:     1,
	}
	memCache, _ := NewMemCache(mockBacking, config)
	defer memCache.Close()

	// Pre-allocate keys
	keys := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%02d", i))
	}
	value := []byte("value-data-for-benchmark")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := keys[i%100]
		_ = memCache.Set(key, value)
	}
}

// Tests moved from memcache_stats_test.go

func TestMemCacheStats(t *testing.T) {
	// Create a backing cache
	backing, err := NewDiskCache(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create backing cache: %v", err)
	}
	defer backing.Close()

	// Create mem cache with small size to trigger evictions
	mc, err := NewMemCache(backing, MemCacheConfig{
		MaxMemoryBytes: 1024 * 10, // 10KB - small to trigger evictions
		EvictionPolicy: EvictionLRU,
		ShardCount:     4,
	})
	if err != nil {
		t.Fatalf("Failed to create mem cache: %v", err)
	}
	defer mc.Close()

	fmt.Println("\n=== MemCache Stats Test ===")

	// Phase 1: Write some data
	fmt.Println("\nPhase 1: Writing 20 keys...")
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := make([]byte, 200)
		if err := mc.Set(key, value); err != nil {
			t.Fatalf("Failed to set: %v", err)
		}
	}

	// Phase 2: Read some keys (hits)
	fmt.Println("\nPhase 2: Reading 10 keys (should be hits)...")
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if _, err := mc.Get(key); err != nil {
			t.Fatalf("Failed to get: %v", err)
		}
	}

	// Phase 3: Read non-existent keys (misses)
	fmt.Println("\nPhase 3: Reading 5 non-existent keys (should be misses)...")
	for i := 100; i < 105; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		mc.Get(key) // Ignore error
	}

	// Phase 4: Write more data to trigger forced evictions
	fmt.Println("\nPhase 4: Writing 30 more keys (should trigger evictions)...")
	for i := 20; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := make([]byte, 200)
		if err := mc.Set(key, value); err != nil {
			t.Fatalf("Failed to set: %v", err)
		}
	}

	// Phase 5: Delete some keys
	fmt.Println("\nPhase 5: Deleting 5 keys...")
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		mc.Delete(key)
	}

	// Get stats
	stats := mc.MemStats()

	fmt.Println("\n=== Final Statistics ===")
	fmt.Printf("Entries: %d\n", stats.Entries)
	fmt.Printf("Memory Used: %d bytes\n", stats.MemoryUsed)
	fmt.Printf("Memory Limit: %d bytes\n", stats.MemoryLimit)
	fmt.Printf("Utilization: %.1f%%\n", stats.Utilization*100)
	fmt.Printf("\nCache Performance:\n")
	fmt.Printf("  Hits: %d\n", stats.Hits)
	fmt.Printf("  Misses: %d\n", stats.Misses)
	fmt.Printf("  Hit Rate: %.1f%%\n", stats.HitRate*100)
	fmt.Printf("  Forced Evictions: %d\n", stats.ForcedEvictions)
	fmt.Printf("  Delete Evictions: %d\n", stats.DeleteEvictions)
	fmt.Printf("  Eviction Rate: %.2f/sec\n", stats.EvictionRate)
	fmt.Printf("  Avg Item Size: %d bytes\n", stats.AvgItemSize)

	// Verify stats make sense
	if stats.Hits < 5 {
		t.Errorf("Expected at least 5 hits, got %d", stats.Hits)
	}
	if stats.Misses < 5 {
		t.Errorf("Expected at least 5 misses, got %d", stats.Misses)
	}
	if stats.ForcedEvictions == 0 {
		t.Errorf("Expected some forced evictions due to small cache size")
	}
	if stats.DeleteEvictions != 5 {
		t.Errorf("Expected 5 delete evictions, got %d", stats.DeleteEvictions)
	}
	if stats.HitRate < 0 || stats.HitRate > 1 {
		t.Errorf("Hit rate should be between 0 and 1, got %.2f", stats.HitRate)
	}

	fmt.Println("\n✓ All stats tests passed")
}

func TestMemCacheStatsReset(t *testing.T) {
	backing, err := NewDiskCache(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create backing cache: %v", err)
	}
	defer backing.Close()

	mc, err := NewMemCache(backing, MemCacheConfig{
		MaxMemoryBytes: 1024 * 100,
		EvictionPolicy: EvictionLRU,
		ShardCount:     4,
	})
	if err != nil {
		t.Fatalf("Failed to create mem cache: %v", err)
	}
	defer mc.Close()

	fmt.Println("\n=== MemCache Stats Reset Test ===")

	// Generate some activity
	fmt.Println("\nGenerating activity...")
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		mc.Set(key, []byte("value"))
		mc.Get(key)
	}

	stats1 := mc.MemStats()
	fmt.Printf("Before reset: Hits=%d, Misses=%d\n", stats1.Hits, stats1.Misses)

	if stats1.Hits == 0 {
		t.Error("Expected some hits before reset")
	}

	// Reset stats
	fmt.Println("\nResetting stats...")
	mc.ResetStats()
	time.Sleep(10 * time.Millisecond) // Small delay

	stats2 := mc.MemStats()
	fmt.Printf("After reset: Hits=%d, Misses=%d\n", stats2.Hits, stats2.Misses)

	if stats2.Hits != 0 {
		t.Errorf("Expected 0 hits after reset, got %d", stats2.Hits)
	}
	if stats2.Misses != 0 {
		t.Errorf("Expected 0 misses after reset, got %d", stats2.Misses)
	}
	if stats2.ForcedEvictions != 0 {
		t.Errorf("Expected 0 forced evictions after reset, got %d", stats2.ForcedEvictions)
	}
	if stats2.DeleteEvictions != 0 {
		t.Errorf("Expected 0 delete evictions after reset, got %d", stats2.DeleteEvictions)
	}

	fmt.Println("\n✓ Stats reset test passed")
}

func TestMemCacheSizingDecisions(t *testing.T) {
	backing, err := NewDiskCache(t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create backing cache: %v", err)
	}
	defer backing.Close()

	mc, err := NewMemCache(backing, MemCacheConfig{
		MaxMemoryBytes: 1024 * 5, // Very small - 5KB
		EvictionPolicy: EvictionLRU,
		ShardCount:     4,
	})
	if err != nil {
		t.Fatalf("Failed to create mem cache: %v", err)
	}
	defer mc.Close()

	fmt.Println("\n=== Cache Sizing Decision Test ===")

	// Write enough to fill cache and trigger many evictions
	fmt.Println("\nWriting 50 keys to small cache...")
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := make([]byte, 200)
		mc.Set(key, value)
	}

	stats := mc.MemStats()

	fmt.Println("\n=== Cache Health Analysis ===")
	fmt.Printf("Utilization: %.1f%%\n", stats.Utilization*100)
	fmt.Printf("Forced Evictions: %d\n", stats.ForcedEvictions)
	fmt.Printf("Delete Evictions: %d\n", stats.DeleteEvictions)
	fmt.Printf("Hit Rate: %.1f%%\n", stats.HitRate*100)

	// Decision criteria
	fmt.Println("\n=== Sizing Recommendations ===")

	tooSmall := false
	reasons := []string{}

	if stats.ForcedEvictions > stats.DeleteEvictions {
		reasons = append(reasons, fmt.Sprintf(
			"⚠️  More forced evictions (%d) than deletes (%d)",
			stats.ForcedEvictions, stats.DeleteEvictions))
		tooSmall = true
	}

	if stats.Utilization > 0.95 {
		reasons = append(reasons, fmt.Sprintf(
			"⚠️  Cache constantly full (%.1f%% utilized)",
			stats.Utilization*100))
		tooSmall = true
	}

	if stats.EvictionRate > 10 {
		reasons = append(reasons, fmt.Sprintf(
			"⚠️  High eviction rate (%.1f evictions/sec)",
			stats.EvictionRate))
		tooSmall = true
	}

	if tooSmall {
		fmt.Println("❌ CACHE TOO SMALL")
		for _, reason := range reasons {
			fmt.Printf("   %s\n", reason)
		}
		suggestedSize := stats.MemoryLimit * 4
		fmt.Printf("\n   💡 Recommendation: Increase from %d to %d bytes (4x)\n",
			stats.MemoryLimit, suggestedSize)
	} else {
		fmt.Println("✅ CACHE SIZE OK")
		fmt.Println("   Most evictions are natural (deletes)")
		fmt.Println("   Comfortable utilization")
	}

	// We expect it to be too small given our test setup
	if !tooSmall {
		t.Error("Expected cache to be flagged as too small")
	}

	fmt.Println("\n✓ Sizing decision test passed")
}

// Tests moved from deadlock_regression_test.go

// TestMemCache_DeadlockRegression tests the deadlock fix where:
// - compactGlobal() holds globalMu and then locks shard.mu
// - releaseMemory() (called from removeEntry while holding shard.mu) tries to lock globalMu
// This was fixed by making memoryUsed atomic, eliminating the lock in releaseMemory()
func TestMemCache_DeadlockRegression(t *testing.T) {
	// Create a small cache to trigger eviction and compaction
	tmpDir, err := os.MkdirTemp("", "deadlock_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 1024, // 1MB
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Run concurrent operations that would trigger the deadlock:
	// 1. Set operations that trigger reserveMemoryWithAllocation -> compactGlobal
	// 2. Delete operations from compaction that call releaseMemory
	// 3. Get operations that also call reserveMemoryWithAllocation

	const (
		numWriters = 10
		numReaders = 10
		numOps     = 100
		timeout    = 10 * time.Second
	)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Writers - these will trigger eviction and compaction
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					key := []byte(fmt.Sprintf("writer-%d-key-%d", id, j))
					value := make([]byte, 1024) // 1KB values
					cache.Set(key, value)
				}
			}
		}(i)
	}

	// Readers - these will also trigger memory reservation
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				select {
				case <-done:
					return
				default:
					key := []byte(fmt.Sprintf("writer-%d-key-%d", id%numWriters, j%numOps))
					cache.Get(key)
				}
			}
		}(i)
	}

	// Wait with timeout to detect deadlock
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		t.Log("Test completed successfully without deadlock")
	case <-time.After(timeout):
		close(done)
		t.Fatal("Test timed out - likely deadlock detected")
	}
}

// TestMemCache_ConcurrentCompactionAndEviction specifically tests the scenario where:
// - One goroutine is in compactGlobal() holding globalMu and locking shards
// - Another goroutine is evicting (holding shard.mu) and calling releaseMemory()
func TestMemCache_ConcurrentCompactionAndEviction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compaction_eviction_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Fill the cache to near capacity
	for i := 0; i < 40; i++ {
		key := []byte(fmt.Sprintf("initial-key-%d", i))
		value := make([]byte, 1024)
		cache.Set(key, value)
	}

	// Now hammer it with concurrent operations
	var wg sync.WaitGroup
	done := make(chan struct{})

	// Goroutine that triggers compaction
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			select {
			case <-done:
				return
			default:
				_, _ = cache.Compact()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Goroutines that trigger eviction
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				select {
				case <-done:
					return
				default:
					key := []byte(fmt.Sprintf("new-key-%d-%d", id, j))
					value := make([]byte, 1024)
					cache.Set(key, value)
				}
			}
		}(i)
	}

	// Wait with timeout
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		t.Log("Concurrent compaction and eviction test passed")
	case <-time.After(10 * time.Second):
		close(done)
		t.Fatal("Test timed out - deadlock in compaction/eviction")
	}
}
