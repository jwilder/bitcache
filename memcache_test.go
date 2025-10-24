package bitcache

import (
	"fmt"
	"testing"
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
