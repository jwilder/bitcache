package bitcache

import "testing"

func TestPartitionedKeyDir_Basic(t *testing.T) {
	pkd := newPartitionedKeyDir(4)

	// Test Set and Get
	entry1 := &keyEntry{fileID: 1, offset: 100, size: 50}
	pkd.Set("key1", entry1)

	retrieved, exists := pkd.Get("key1")
	if !exists {
		t.Fatal("key1 should exist")
	}
	if retrieved.fileID != 1 || retrieved.offset != 100 {
		t.Fatalf("wrong entry: got %+v, want fileID=1 offset=100", retrieved)
	}

	// Test non-existent key
	_, exists = pkd.Get("nonexistent")
	if exists {
		t.Fatal("nonexistent key should not exist")
	}
}

func TestPartitionedKeyDir_EvictSegment(t *testing.T) {
	pkd := newPartitionedKeyDir(4)

	// Add keys to different segments
	pkd.Set("key1", &keyEntry{fileID: 1, offset: 100, size: 50})
	pkd.Set("key2", &keyEntry{fileID: 1, offset: 200, size: 50})
	pkd.Set("key3", &keyEntry{fileID: 2, offset: 100, size: 50})
	pkd.Set("key4", &keyEntry{fileID: 2, offset: 200, size: 50})

	// Evict segment 1
	removed := pkd.EvictSegment(1)
	if removed != 2 {
		t.Fatalf("expected 2 keys removed, got %d", removed)
	}

	// Verify segment 1 keys are gone
	if _, exists := pkd.Get("key1"); exists {
		t.Fatal("key1 should be evicted")
	}
	if _, exists := pkd.Get("key2"); exists {
		t.Fatal("key2 should be evicted")
	}

	// Verify segment 2 keys remain
	if _, exists := pkd.Get("key3"); !exists {
		t.Fatal("key3 should still exist")
	}
	if _, exists := pkd.Get("key4"); !exists {
		t.Fatal("key4 should still exist")
	}
}

func TestPartitionedKeyDir_Range(t *testing.T) {
	pkd := newPartitionedKeyDir(4)

	pkd.Set("key1", &keyEntry{fileID: 1, offset: 100, size: 50})
	pkd.Set("key2", &keyEntry{fileID: 1, offset: 200, size: 50})
	pkd.Set("key3", &keyEntry{fileID: 2, offset: 100, size: 50})

	count := 0
	pkd.Range(func(key string, entry *keyEntry) bool {
		count++
		return false // continue
	})

	if count != 3 {
		t.Fatalf("expected 3 keys, got %d", count)
	}

	// Test early termination
	count = 0
	pkd.Range(func(key string, entry *keyEntry) bool {
		count++
		return count >= 2 // stop after 2
	})

	if count != 2 {
		t.Fatalf("expected 2 keys with early termination, got %d", count)
	}
}

func TestPartitionedKeyDir_Len(t *testing.T) {
	pkd := newPartitionedKeyDir(4)

	if pkd.Len() != 0 {
		t.Fatalf("expected 0 length, got %d", pkd.Len())
	}

	pkd.Set("key1", &keyEntry{fileID: 1, offset: 100, size: 50})
	pkd.Set("key2", &keyEntry{fileID: 1, offset: 200, size: 50})
	pkd.Set("key3", &keyEntry{fileID: 2, offset: 100, size: 50})

	if pkd.Len() != 3 {
		t.Fatalf("expected 3 length, got %d", pkd.Len())
	}

	pkd.EvictSegment(1)
	if pkd.Len() != 1 {
		t.Fatalf("expected 1 length after eviction, got %d", pkd.Len())
	}
}
