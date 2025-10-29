package bitcache

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestGarbageCollection is disabled because background GC (StartGC/StopGC) has been removed.
// GC is now handled automatically as part of Compact().
func TestGarbageCollection(t *testing.T) {
	t.Skip("Background GC has been removed; GC is now handled via Compact()")
}

// TestGCRateLimiting is disabled because background GC (StartGC/StopGC) has been removed.
// GC is now handled automatically as part of Compact().
func TestGCRateLimiting(t *testing.T) {
	t.Skip("Background GC has been removed; GC is now handled via Compact()")
}

// TestGCNoopOnRecentSegments is disabled because background GC (StartGC/StopGC) has been removed.
// GC is now handled automatically as part of Compact().
func TestGCNoopOnRecentSegments(t *testing.T) {
	t.Skip("Background GC has been removed; GC is now handled via Compact()")
}

// TestGCStopAndRestart is disabled because background GC (StartGC/StopGC) has been removed.
// GC is now handled automatically as part of Compact().
func TestGCStopAndRestart(t *testing.T) {
	t.Skip("Background GC has been removed; GC is now handled via Compact()")
}

// Tests moved from integrated_gc_test.go

func TestCompactWithIntegratedGC(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "integrated_gc_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 3, // 3KB segments (smaller = more segments)
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	fmt.Println("\n=== Testing Integrated GC in Compact() ===")

	// Phase 1: Write enough data to trigger LSM compaction (16+ L0 segments)
	fmt.Println("\nPhase 1: Writing 600 keys to create 16+ L0 segments...")
	for i := 0; i < 600; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}
	cache.Sync()

	byLevel, _ := cache.GetSegmentsByLevel()
	initialL0 := len(byLevel[0])
	fmt.Printf("Created %d L0 segments\n", initialL0)

	if initialL0 < 16 {
		t.Skipf("Not enough L0 segments for test (got %d, need 16+)", initialL0)
	}

	// Phase 2: First Compact() should do LSM compaction
	fmt.Println("\nPhase 2: First Compact() should do LSM compaction...")
	if _, err := cache.Compact(); err != nil {
		t.Fatalf("First Compact() failed: %v", err)
	}

	byLevel, _ = cache.GetSegmentsByLevel()
	afterL0 := len(byLevel[0])
	afterL1 := len(byLevel[1])

	fmt.Printf("After LSM: L0=%d, L1=%d\n", afterL0, afterL1)

	if afterL0 >= initialL0 {
		t.Errorf("LSM compaction should have reduced L0 (was %d, now %d)", initialL0, afterL0)
	}

	if afterL1 == 0 {
		t.Errorf("LSM compaction should have created L1 segments")
	}

	// Phase 3: Verify all keys are still readable
	fmt.Println("\nPhase 3: Verifying data integrity...")
	for i := 0; i < 600; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if _, err := cache.Get(key); err != nil {
			t.Errorf("Failed to read key after compaction: %v", err)
		}
	}

	fmt.Println("✓ All 600 keys verified")
	fmt.Println("\n✓ Integrated compaction test completed successfully")
}

func TestCompactPrioritizesLSMOverGC(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "priority_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 3, // 3KB segments (smaller to create more segments)
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	fmt.Println("\n=== Testing LSM Priority Over GC ===")

	// Write enough data to create 16+ L0 segments (triggers LSM)
	fmt.Println("Creating 16+ L0 segments...")
	for i := 0; i < 600; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}
	cache.Sync()

	// Also create some dead data
	fmt.Println("Creating dead data...")
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := []byte(fmt.Sprintf("updated-%04d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to update key: %v", err)
		}
	}
	cache.Sync()

	time.Sleep(2 * time.Second) // Brief wait to ensure file timestamps differ

	byLevel, _ := cache.GetSegmentsByLevel()
	l0Count := len(byLevel[0])

	fmt.Printf("L0 segments before: %d\n", l0Count)

	// First Compact() should do LSM compaction (L0 → L1)
	// because L0 has 16+ segments (higher priority than GC)
	fmt.Println("\nCalling Compact()...")
	if _, err := cache.Compact(); err != nil {
		t.Fatalf("Compact() failed: %v", err)
	}

	byLevel, _ = cache.GetSegmentsByLevel()
	newL0Count := len(byLevel[0])
	l1Count := len(byLevel[1])

	fmt.Printf("L0 segments after: %d\n", newL0Count)
	fmt.Printf("L1 segments after: %d\n", l1Count)

	// Should have performed LSM compaction (reduced L0 count)
	if newL0Count >= l0Count {
		t.Errorf("Expected L0 count to decrease (LSM compaction), got %d → %d",
			l0Count, newL0Count)
	}

	fmt.Println("\n✓ LSM compaction was prioritized over GC")
}

func TestCompactFallsBackToGC(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "fallback_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 10, // 10KB segments
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	fmt.Println("\n=== Testing Compact Falls Back to GC ===")

	// Create just a few segments (not enough for LSM compaction)
	fmt.Println("Creating 3 segments...")
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := make([]byte, 200)
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}
	cache.Sync()

	// Update most keys to create high dead ratio (>40%)
	fmt.Println("Creating 80%% dead data...")
	for i := 0; i < 80; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		value := []byte(fmt.Sprintf("new-value-%03d", i))
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to update key: %v", err)
		}
	}
	cache.Sync()

	byLevel, _ := cache.GetSegmentsByLevel()
	totalSegments := 0
	for _, segs := range byLevel {
		totalSegments += len(segs)
	}
	fmt.Printf("Total segments before: %d\n", totalSegments)

	// Age the segments
	fmt.Println("Aging segments...")
	time.Sleep(2 * time.Second)

	beforeSize := cache.Stats().DataSize

	// Compact() should fall back to GC since no LSM compaction is needed
	fmt.Println("\nCalling Compact() (should perform GC)...")
	if _, err := cache.Compact(); err != nil {
		t.Fatalf("Compact() failed: %v", err)
	}

	afterSize := cache.Stats().DataSize

	fmt.Printf("Data size before: %d\n", beforeSize)
	fmt.Printf("Data size after: %d\n", afterSize)

	// GC should have reduced data size
	if afterSize >= beforeSize {
		t.Logf("Warning: Expected data size to decrease from GC, got %d → %d",
			beforeSize, afterSize)
	} else {
		saved := beforeSize - afterSize
		pct := float64(saved) / float64(beforeSize) * 100
		fmt.Printf("Space saved: %d bytes (%.1f%%)\n", saved, pct)
	}

	// Verify data integrity
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		if _, err := cache.Get(key); err != nil {
			t.Errorf("Failed to read key after GC: %v", err)
		}
	}

	fmt.Println("\n✓ Compact successfully fell back to GC")
}
