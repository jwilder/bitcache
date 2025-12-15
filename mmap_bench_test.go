package bitcache

import (
	"fmt"
	"os"
	"testing"
)

// BenchmarkDiskCache_MmapReads benchmarks read performance with mmap
func BenchmarkDiskCache_MmapReads(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcache_mmap_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create cache and populate it
	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}

	// Write 10,000 keys
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		value := []byte(fmt.Sprintf("value_%06d_with_some_data_to_make_it_realistic", i))
		if err := cache.Set(key, value); err != nil {
			b.Fatalf("Failed to set key: %v", err)
		}
	}

	// Force rotation to ensure we're reading from closed segments (mmap)
	if err := cache.rotateLogFile(); err != nil {
		b.Fatalf("Failed to rotate: %v", err)
	}

	// Close and reopen to ensure all files are closed and will be mmap'd
	if err := cache.Close(); err != nil {
		b.Fatalf("Failed to close cache: %v", err)
	}

	cache, err = NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark random reads
	for i := 0; i < b.N; i++ {
		keyIdx := i % numKeys
		key := []byte(fmt.Sprintf("key_%06d", keyIdx))
		_, err := cache.Get(key)
		if err != nil {
			b.Fatalf("Failed to get key: %v", err)
		}
	}
}

// BenchmarkDiskCache_MmapSequentialReads benchmarks sequential read performance
func BenchmarkDiskCache_MmapSequentialReads(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "bitcache_mmap_seq_bench")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to create cache: %v", err)
	}

	// Write 1,000 keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		value := []byte(fmt.Sprintf("value_%06d_with_some_data", i))
		if err := cache.Set(key, value); err != nil {
			b.Fatalf("Failed to set key: %v", err)
		}
	}

	// Force rotation
	if err := cache.rotateLogFile(); err != nil {
		b.Fatalf("Failed to rotate: %v", err)
	}

	if err := cache.Close(); err != nil {
		b.Fatalf("Failed to close cache: %v", err)
	}

	cache, err = NewDiskCache(tmpDir, ByteSliceMarshaler{})
	if err != nil {
		b.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	b.ResetTimer()
	b.ReportAllocs()

	keyIdx := 0
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key_%06d", keyIdx))
		_, err := cache.Get(key)
		if err != nil {
			b.Fatalf("Failed to get key: %v", err)
		}
		keyIdx = (keyIdx + 1) % numKeys
	}
}
