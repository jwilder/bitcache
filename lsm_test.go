package bitcache

import (
	"fmt"
	"os"
	"testing"
)

// Tests moved from intelligent_compact_test.go

func TestIntelligentCompaction(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "intelligent_compact_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 30, // Smaller segments to create more L0 segments (30KB)
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write enough data to create 16+ L0 segments for the new batch size
	for i := 0; i < 1200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := make([]byte, 500)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}
	cache.Sync()

	// Check initial state
	byLevel, _ := cache.GetSegmentsByLevel()
	fmt.Printf("\nInitial state:\n")
	printLevels(byLevel)

	initialL0 := len(byLevel[0])
	if initialL0 < 16 {
		t.Skipf("Not enough L0 segments created (got %d, need 16)", initialL0)
	}

	// Test 1: First Compact() should compact L0
	fmt.Printf("\nRunning Compact() - should compact L0...\n")
	if _, err := cache.Compact(); err != nil {
		t.Fatalf("First Compact() failed: %v", err)
	}

	byLevel, _ = cache.GetSegmentsByLevel()
	fmt.Printf("\nAfter first Compact():\n")
	printLevels(byLevel)

	// Verify L0 was compacted and L1 was created
	if len(byLevel[0]) >= initialL0 {
		t.Errorf("L0 was not compacted (still have %d segments)", len(byLevel[0]))
	}
	if len(byLevel[1]) == 0 {
		t.Errorf("L1 was not created after L0 compaction")
	}

	// Test 2: If we have more L0 segments, Compact() should handle them
	if len(byLevel[0]) >= 3 {
		fmt.Printf("\nRunning Compact() again - should compact remaining L0...\n")
		if _, err := cache.Compact(); err != nil {
			t.Fatalf("Second Compact() failed: %v", err)
		}

		byLevel, _ = cache.GetSegmentsByLevel()
		fmt.Printf("\nAfter second Compact():\n")
		printLevels(byLevel)
	}

	// Test 3: Once we have multiple L1 segments, Compact() should handle L1
	if len(byLevel[1]) >= 3 {
		fmt.Printf("\nRunning Compact() - should compact L1...\n")
		if _, err := cache.Compact(); err != nil {
			t.Fatalf("L1 Compact() failed: %v", err)
		}

		byLevel, _ = cache.GetSegmentsByLevel()
		fmt.Printf("\nAfter L1 Compact():\n")
		printLevels(byLevel)

		// Should have created L2
		if len(byLevel[2]) == 0 {
			t.Errorf("L2 was not created after L1 compaction")
		}
	}

	// Test 4: Compact() with nothing to do should be a no-op
	fmt.Printf("\nRunning Compact() with nothing to do...\n")
	if _, err := cache.Compact(); err != nil {
		t.Fatalf("No-op Compact() failed: %v", err)
	}
	fmt.Printf("✓ No-op Compact() succeeded\n")

	// Verify all keys are still readable
	fmt.Printf("\nVerifying all keys...\n")
	for i := 0; i < 1200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		if _, err := cache.Get(key); err != nil {
			t.Errorf("Failed to read key %s: %v", key, err)
		}
	}
	fmt.Printf("✓ All 1200 keys verified\n")
}

func TestIntelligentCompactionL4(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "l4_compact_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 30, // Very small segments
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write a lot of data to force multiple compaction levels
	for i := 0; i < 1200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := make([]byte, 500)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}
	cache.Sync()

	fmt.Printf("\nInitial state:\n")
	byLevel, _ := cache.GetSegmentsByLevel()
	printLevels(byLevel)

	// Compact multiple times to push data to L4
	maxCompactions := 20
	for i := 0; i < maxCompactions; i++ {
		byLevel, _ = cache.GetSegmentsByLevel()

		// Check if we have L4 segments
		if len(byLevel[4]) >= 2 {
			fmt.Printf("\nFound %d L4 segments, next Compact() should merge them\n", len(byLevel[4]))
			break
		}

		// Check if there's anything to compact
		hasWork := false
		for level := uint8(0); level <= 4; level++ {
			if level == 4 && len(byLevel[level]) >= 2 {
				hasWork = true
				break
			} else if level < 4 && len(byLevel[level]) >= 3 {
				hasWork = true
				break
			}
		}

		if !hasWork {
			fmt.Printf("\nNo more compaction needed after %d rounds\n", i)
			break
		}

		fmt.Printf("\nCompaction round %d...\n", i+1)
		if _, err := cache.Compact(); err != nil {
			t.Fatalf("Compaction round %d failed: %v", i+1, err)
		}
	}

	byLevel, _ = cache.GetSegmentsByLevel()
	fmt.Printf("\nFinal state:\n")
	printLevels(byLevel)

	// If we have L4 segments, test that they can be merged
	if len(byLevel[4]) >= 2 {
		fmt.Printf("\nTesting L4 self-compaction...\n")
		initialL4Count := len(byLevel[4])

		if _, err := cache.Compact(); err != nil {
			t.Fatalf("L4 compaction failed: %v", err)
		}

		byLevel, _ = cache.GetSegmentsByLevel()
		finalL4Count := len(byLevel[4])

		fmt.Printf("L4 segments: %d → %d\n", initialL4Count, finalL4Count)

		if finalL4Count >= initialL4Count {
			t.Errorf("L4 compaction did not reduce segment count (%d → %d)", initialL4Count, finalL4Count)
		}
	}

	// Verify all keys are still readable
	fmt.Printf("\nVerifying all keys...\n")
	for i := 0; i < 1200; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		if _, err := cache.Get(key); err != nil {
			t.Errorf("Failed to read key %s: %v", key, err)
		}
	}
	fmt.Printf("✓ All 1200 keys verified\n")
}

func printLevels(byLevel map[uint8][]*SegmentInfo) {
	for level := uint8(0); level <= 4; level++ {
		segments := byLevel[level]
		if len(segments) > 0 {
			fmt.Printf("  L%d: %d segments", level, len(segments))
			if len(segments) <= 5 {
				fmt.Printf(" [")
				for i, seg := range segments {
					if i > 0 {
						fmt.Printf(", ")
					}
					fmt.Printf("%s", seg.ID.String())
				}
				fmt.Printf("]")
			}
			fmt.Printf("\n")
		}
	}
}

// Tests moved from lsm_compaction_test.go

func TestLSMCompaction(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "lsm_compaction_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 50, // Small segments to trigger rotation (50KB)
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	// Write data to create multiple L0 segments (need 8+ for auto compaction)
	for i := 0; i < 800; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value := make([]byte, 500) // 500 bytes
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	// Force sync
	if err := cache.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// List segments before compaction
	byLevel, err := cache.GetSegmentsByLevel()
	if err != nil {
		t.Fatalf("Failed to get segments: %v", err)
	}

	fmt.Printf("\nBefore compaction:\n")
	for level := uint8(0); level <= 4; level++ {
		segments := byLevel[level]
		if len(segments) > 0 {
			fmt.Printf("  L%d: %d segments\n", level, len(segments))
			for _, seg := range segments {
				fmt.Printf("    - %s (%d bytes)\n", seg.ID.String(), seg.TotalBytes)
			}
		}
	}

	// Should have multiple L0 segments (need 8+ for auto compaction)
	l0Count := len(byLevel[0])
	if l0Count < 8 {
		t.Skipf("Not enough L0 segments for compaction test (got %d, need 8+)", l0Count)
	}

	// Perform auto compaction multiple times to ensure it triggers
	fmt.Printf("\nPerforming auto compaction...\n")
	compactionsDone := 0
	for i := 0; i < 3; i++ {
		result, err := cache.Compact()
		if err != nil {
			t.Fatalf("Compaction failed: %v", err)
		}
		if result.Type != "none" {
			compactionsDone++
			fmt.Printf("Compaction %d: %s\n", compactionsDone, result.Type)
		}
	}

	// Close and reopen cache to reload file references
	cache.Close()
	cache, err = NewDiskCache(tmpDir)
	if err != nil {
		t.Fatalf("Failed to reopen cache: %v", err)
	}
	defer cache.Close()

	// List segments after compaction
	byLevel, err = cache.GetSegmentsByLevel()
	if err != nil {
		t.Fatalf("Failed to get segments after compaction: %v", err)
	}

	fmt.Printf("\nAfter compaction:\n")
	for level := uint8(0); level <= 4; level++ {
		segments := byLevel[level]
		if len(segments) > 0 {
			fmt.Printf("  L%d: %d segments\n", level, len(segments))
			for _, seg := range segments {
				fmt.Printf("    - %s (%d bytes)\n", seg.ID.String(), seg.TotalBytes)
			}
		}
	}

	// Verify at least one compaction occurred
	if compactionsDone == 0 {
		t.Errorf("Expected at least one compaction to occur")
	}

	// Verify all keys are still readable
	fmt.Printf("\nVerifying all keys are readable...\n")
	for i := 0; i < 800; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		value, err := cache.Get(key)
		if err != nil {
			t.Errorf("Failed to read key after compaction: %v", err)
		}
		if len(value) != 500 {
			t.Errorf("Wrong value size for key %s: got %d, want 500", key, len(value))
		}
	}

	fmt.Printf("✓ All keys verified successfully\n")
}

func TestLSMMultiLevelCompaction(t *testing.T) {
	// Create temporary cache
	tmpDir, err := os.MkdirTemp("", "lsm_multilevel_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cache, err := NewDiskCacheWithConfig(tmpDir, DiskCacheConfig{
		MaxSegmentSize: 1024 * 50, // Very small segments
	})
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Write enough data to create many segments
	for i := 0; i < 800; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		value := make([]byte, 500)
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := cache.Set(key, value); err != nil {
			t.Fatalf("Failed to set key: %v", err)
		}
	}

	cache.Sync()

	// Get initial state
	byLevel, _ := cache.GetSegmentsByLevel()
	fmt.Printf("\nInitial state:\n")
	printSegmentsByLevel(byLevel)

	// Compact multiple times using auto compaction
	for i := 0; i < 5; i++ {
		byLevel, _ := cache.GetSegmentsByLevel()
		// Check if compaction is needed at any level
		needsCompaction := len(byLevel[0]) >= 4 || len(byLevel[1]) >= 4 || len(byLevel[2]) >= 4
		if !needsCompaction {
			break
		}

		fmt.Printf("\nCompaction round %d\n", i+1)
		if _, err := cache.Compact(); err != nil {
			t.Fatalf("Compaction failed: %v", err)
		}
	}

	// Final state
	byLevel, _ = cache.GetSegmentsByLevel()
	fmt.Printf("\nFinal state:\n")
	printSegmentsByLevel(byLevel)

	// Verify all keys still readable
	fmt.Printf("\nVerifying all keys...\n")
	for i := 0; i < 800; i++ {
		key := []byte(fmt.Sprintf("key-%05d", i))
		if _, err := cache.Get(key); err != nil {
			t.Errorf("Failed to read key %s: %v", key, err)
		}
	}
	fmt.Printf("✓ All %d keys verified\n", 800)
}

func printSegmentsByLevel(byLevel map[uint8][]*SegmentInfo) {
	for level := uint8(0); level <= 4; level++ {
		segments := byLevel[level]
		if len(segments) > 0 {
			fmt.Printf("  L%d: %d segments\n", level, len(segments))
			for _, seg := range segments {
				fmt.Printf("    - %s (%d bytes)\n", seg.ID.String(), seg.TotalBytes)
			}
		}
	}
}
