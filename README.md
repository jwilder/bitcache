# BitCache

A simple, fast persistent cache in Go library, inspired by the Bitcask design.

## Features

- **Fast**: O(1) average time complexity for lookups with an in-memory partitioned hash map index
- **Memory Cache Layer**: Optional high-performance read-through cache with bounded memory
- **Reliable**: CRC32 checksums for data integrity
- **Crash-safe**: Recovers from corrupted files by skipping bad entries with CRC validation
- **Compaction**: Reclaims disk space by removing old segments based on age and disk usage limits
- **Concurrent**: Concurrent reads and writes with partitioned locks to minimize contention
- **Low GC Pressure**: Memory cache uses object pooling to reduce allocations
- **Simple**: Clean API with minimal dependencies
- **Write-once**: Keys can be written multiple times but old values are never updated in place

## Design

BitCache uses a log-structured storage approach inspired by Bitcask:

- **Append-only log**: All writes go sequentially to an active segment file. When it reaches the size threshold, it's rotated and a new segment is created.
- **In-memory index**: A partitioned hash map (slice of maps keyed by segment ID, then by key) enables O(1) average time lookups and concurrent access without a global lock. Keys are hashed to determine which partition they belong to.
- **Compaction**: Removes old segments based on age (MaxAge) and/or disk usage limits (MaxDiskUsage). When a segment is removed, all its keys are evicted from the in-memory index by deleting the entire partition map for that segment.
- **Memory cache layer**: Optional read-through cache using sharded hash maps with LRU/LFU eviction and object pooling to reduce GC pressure.
- **Crash recovery**: On startup, the key directory is rebuilt by scanning log files. Corrupted entries are detected via CRC validation and skipped to recover as much data as possible.

## Installation

```bash
go get github.com/jwilder/bitcache
```

## Usage

### Library API

```go
package main

import (
    "fmt"
    "log"
    
    "github.com/jwilder/bitcache"
)

func main() {
    // Open or create a database
    db, err := bitcache.NewDiskCache("/path/to/data")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()
    
    // Set a key-value pair
    err = db.Set([]byte("hello"), []byte("world"))
    if err != nil {
        log.Fatal(err)
    }
    
    // Get a value
    value, err := db.Get([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Value: %s\n", value)
    
    // Check if a key exists
    if db.Has([]byte("hello")) {
        fmt.Println("Key exists!")
    }
    
    // Get statistics
    stats := db.Stats()
    fmt.Printf("Keys: %d\n", stats.Keys)
    fmt.Printf("Data size: %d bytes\n", stats.DataSize)
    fmt.Printf("Reads: %d, Writes: %d\n", stats.Reads, stats.Writes)
    
    // Compact the database to reclaim space
    err = db.Compact()
    if err != nil {
        log.Fatal(err)
    }
}
```

### Using MemCache Layer

MemCache provides a high-performance read-through cache on top of DiskCache with bounded memory usage and low GC pressure through object pooling.

#### Basic MemCache Setup

```go
package main

import (
    "log"
    "github.com/jwilder/bitcache"
)

func main() {
    // Create backing disk cache
    diskCache, err := bitcache.NewDiskCache("/path/to/data")
    if err != nil {
        log.Fatal(err)
    }
    defer diskCache.Close()

    // Configure memory cache
    config := bitcache.MemCacheConfig{
        MaxMemoryBytes: 100 * 1024 * 1024, // 100MB memory limit
        EvictionPolicy: bitcache.EvictionLRU, // LRU or LFU
        ShardCount:     256,                // Reduces lock contention
    }

    // Create memory cache layer
    memCache, err := bitcache.NewMemCache(diskCache, config)
    if err != nil {
        log.Fatal(err)
    }
    defer memCache.Close()

    // Use the same Cache interface - transparently fast!
    memCache.Set([]byte("key"), []byte("value"))
    value, _ := memCache.Get([]byte("key")) // Served from memory
}
```

#### MemCache Features

**LRU vs LFU Eviction**

```go
// LRU: Evicts least recently accessed items (faster, O(1))
config := bitcache.MemCacheConfig{
    MaxMemoryBytes: 100 * 1024 * 1024,
    EvictionPolicy: bitcache.EvictionLRU,
}

// LFU: Evicts least frequently accessed items (good for hot keys)
config := bitcache.MemCacheConfig{
    MaxMemoryBytes: 100 * 1024 * 1024,
    EvictionPolicy: bitcache.EvictionLFU,
}
```

**Custom Cache Policy**

Control which entries are cached in memory:

```go
// Only cache entries with keys starting with "hot-"
policy := func(key []byte, value []byte) bool {
    return len(key) >= 4 && string(key[:4]) == "hot-"
}

config := bitcache.MemCacheConfig{
    MaxMemoryBytes: 100 * 1024 * 1024,
    EvictionPolicy: bitcache.EvictionLRU,
    CachePolicy:    policy, // Custom policy function
}
memCache, _ := bitcache.NewMemCache(diskCache, config)

// "hot-data" will be cached in memory
memCache.Set([]byte("hot-data"), []byte("value1"))

// "cold-data" will only be stored in backing DiskCache
memCache.Set([]byte("cold-data"), []byte("value2"))

// Both are retrievable, but only hot-data is in memory
```

**Cache Warming**

Pre-load frequently accessed keys on startup:

```go
keys := [][]byte{
    []byte("user:1234"),
    []byte("user:5678"),
    []byte("config:main"),
}
err := memCache.Warmup(keys)
if err != nil {
    log.Printf("Warmup failed: %v", err)
}
```

**Cache Invalidation**

Remove entries from memory cache without touching backing store:

```go
// Remove from memory cache only
memCache.Invalidate([]byte("stale-key"))

// Data still in backing store and will be read-through if accessed
value, _ := memCache.Get([]byte("stale-key"))
```

**Memory Compaction**

MemCache automatically compacts memory arenas to reclaim fragmented space:

```go
// Configure compaction thresholds
config := bitcache.MemCacheConfig{
    MaxMemoryBytes:      100 * 1024 * 1024, // 100MB
    EvictionPolicy:      bitcache.EvictionLRU,
    CompactionThreshold: 0.4,  // Compact when fragmentation exceeds 40% (default)
    CompactionInterval:  300,  // Check every 5 minutes (default)
}

// Manually trigger compaction
memCache.Compact()

// Check fragmentation before compacting
stats := memCache.MemStats()
fmt.Printf("Fragmentation: %.2f%%\n", stats.Fragmentation * 100)

if stats.Fragmentation > 0.5 {
    memCache.Compact()
    fmt.Println("Compaction completed")
}
```

When entries are deleted or evicted, memory becomes fragmented. Compaction:
1. Detects fragmentation via background worker monitoring
2. Rebuilds arena by creating a new compact arena and copying active entries
3. Reclaims memory by making old fragmented arenas eligible for GC
4. Maintains data accessibility during the entire process (thread-safe)

**Benefits:** Reduces memory footprint by 80-90% in fragmented scenarios, improves cache locality and performance.

**Cache Management**

```go
// Clear all memory cache entries (preserves backing store)
err := memCache.Clear()
```

**Monitoring**

Get detailed memory cache statistics:

```go
stats := memCache.MemStats()
fmt.Printf("Entries: %d\n", stats.Entries)
fmt.Printf("Memory Used: %d bytes\n", stats.MemoryUsed)
fmt.Printf("Memory Allocated: %d bytes\n", stats.MemoryAllocated)
fmt.Printf("Memory Limit: %d bytes\n", stats.MemoryLimit)
fmt.Printf("Shards: %d\n", stats.Shards)
fmt.Printf("Fragmentation: %.2f%%\n", stats.Fragmentation * 100)

// Memory usage percentage
usagePercent := float64(stats.MemoryUsed) / float64(stats.MemoryLimit) * 100
fmt.Printf("Usage: %.2f%%\n", usagePercent)

// Memory efficiency (how much allocated space is actually used)
efficiency := float64(stats.MemoryUsed) / float64(stats.MemoryAllocated) * 100
fmt.Printf("Efficiency: %.2f%%\n", efficiency)
```

## Performance

Benchmarks run on a MacBook Pro with SSD:

### MemCache Performance

With MemCache enabled, read performance increases dramatically:

```
BenchmarkMemCache/cached_get-10     28,685,608    41.68 ns/op    0 B/op    0 allocs/op
BenchmarkMemCache/miss_get-10          624,360  1,914.00 ns/op  144 B/op    3 allocs/op
BenchmarkMemCache/set-10               584,257  2,052.00 ns/op  144 B/op    3 allocs/op
```

- **Cached reads**: ~42 ns/op with zero allocations
- **Cache misses**: Falls back to disk with automatic caching
- **Low GC pressure**: Object pooling reduces allocations



## API Reference

### Core Interface

```go
type Cache interface {
    // Get retrieves the value for the given key
    // Returns ErrKeyNotFound if the key doesn't exist
    Get(key []byte) ([]byte, error)

    // Set stores a key-value pair in the cache
    Set(key []byte, value []byte) error

    // Has checks if a key exists in the cache
    Has(key []byte) bool

    // Stats returns cache statistics
    Stats() Stats
}
```

### Creating a Database

```go
// NewDiskCache creates or opens a BitCache database
func NewDiskCache(dataDir string) (*DiskCache, error)

// NewMemCache creates a memory cache layer on top of a backing cache
func NewMemCache(backing Cache, config MemCacheConfig) (*MemCache, error)
```

### MemCache Configuration

```go
type MemCacheConfig struct {
    // MaxMemoryBytes is the maximum memory to use for cached entries
    MaxMemoryBytes int64
    
    // EvictionPolicy determines which entries to evict (EvictionLRU or EvictionLFU)
    EvictionPolicy EvictionPolicy
    
    // CachePolicy determines which entries should be cached (nil = cache all)
    CachePolicy CachePolicyFunc
    
    // ShardCount is the number of shards to use (default: 256)
    ShardCount int
    
    // CompactionThreshold is the fragmentation ratio that triggers compaction (default: 0.4)
    CompactionThreshold float64
    
    // CompactionInterval is how often to check for compaction in seconds (default: 300)
    CompactionInterval int
}

// CachePolicyFunc decides whether a key-value pair should be cached
type CachePolicyFunc func(key []byte, value []byte) bool

// EvictionPolicy types
const (
    EvictionLRU EvictionPolicy = iota // Least Recently Used
    EvictionLFU                        // Least Frequently Used
)
```

### MemCache Methods

```go
// Invalidate removes a key from the memory cache (not from backing store)
func (mc *MemCache) Invalidate(key []byte) error

// Clear removes all entries from the memory cache
func (mc *MemCache) Clear() error

// Warmup pre-loads keys from backing store into memory
func (mc *MemCache) Warmup(keys [][]byte) error

// Compact manually triggers memory compaction to reclaim fragmented space
func (mc *MemCache) Compact() error
```

### Additional Methods

```go
// Close cleanly shuts down the database
func (d *DiskCache) Close() error

// Compact removes old segments based on configured age and disk usage limits
func (d *DiskCache) Compact() error

// Sync forces a fsync on the active segment
func (d *DiskCache) Sync() error
```

### Statistics

```go
type Stats struct {
    Keys      int64  // Number of keys in the cache
    DataSize  int64  // Total size of data on disk in bytes
    IndexSize int64  // Size of in-memory index in bytes
    Segments  int    // Number of segment files
    Reads     int64  // Total number of read operations
    Writes    int64  // Total number of write operations
}
```

## Error Handling

```go
import "github.com/jwilder/bitcache"

// Check for key not found
value, err := db.Get([]byte("key"))
if err == bitcache.ErrKeyNotFound {
    // Key doesn't exist
} else if err != nil {
    // Other error
}

// Check for closed database
err = db.Set([]byte("key"), []byte("value"))
if err == bitcache.ErrCacheClosed {
    // Database is closed
}
```

## Configuration

BitCache uses sensible defaults, but can be configured:

```go
config := bitcache.DiskCacheConfig{
    MaxSegmentSize:      16 * 1024 * 1024,  // 16 MB (default)
    MaxDiskUsage:        1024 * 1024 * 1024, // 1 GB limit (0 = no limit)
    MinSegmentAge:       time.Hour,          // Don't remove segments newer than 1 hour
    AutoCompactEnabled:  true,               // Enable automatic compaction
    AutoCompactInterval: 5 * time.Minute,    // Check every 5 minutes
    AutoCompactMaxAge:   7 * 24 * time.Hour, // Remove segments older than 7 days
}
db, err := bitcache.NewDiskCacheWithConfig("/path/to/data", config)
```

**Configuration Options:**

- **MaxSegmentSize**: Maximum size before rotating to a new segment file (default: 16 MB)
- **MaxDiskUsage**: Maximum total disk usage before compaction removes old segments (default: 0 = no limit)
- **MinSegmentAge**: Minimum age before a segment can be removed (default: 1 hour)
- **AutoCompactEnabled**: Enable automatic background compaction (default: false)
- **AutoCompactInterval**: Time between automatic compaction checks (default: 5 minutes)
- **AutoCompactMaxAge**: Maximum age for segments; older segments are removed (default: 0 = age-based removal disabled)

### MemCache Architecture

MemCache provides low GC pressure through object pooling and careful memory management:

**How Object Pooling Works:**

1. **Entry Pooling**: Uses `sync.Pool` to reuse cache entry objects, avoiding repeated allocations
2. **Node Pooling**: LRU list nodes are pooled and reused when entries are evicted
3. **Bounded Memory**: Strict memory limit with automatic eviction prevents unbounded growth
4. **Sharded Maps**: Distributes entries across multiple shards to reduce lock contention

**Memory Accounting:**

Each cached entry consumes:
- Key size (bytes)
- Value size (bytes)
- ~32 bytes overhead (entry metadata)

The memory limit is strictly enforced - entries are evicted before exceeding the limit.

**Thread Safety:**

MemCache is fully thread-safe with:
- Sharded locking to reduce contention (default: 256 shards)
- RWMutex per shard for concurrent reads
- Atomic operations for access counting

### Use Cases

BitCache is primarily designed for **persistent caching** scenarios:

- **Application caching**: Fast local cache with persistence across restarts
- **Computed results**: Caching expensive computations or API responses
- **Temporary data**: Short-lived data that benefits from persistence
- **Prototyping**: Quick storage solution for proof-of-concepts
- **Embedded databases**: Simple key-value storage within applications

As a persistent cache rather than a full storage engine, BitCache has intentional trade-offs:

- Single writer at a time (concurrent reads are fully supported)
- No delete operations (write-once keys with automatic age-based eviction)
- No query language or secondary indexes
- No transactions or batch operations

These limitations make it unsuitable as a primary database for most applications, but ideal for caching scenarios.

## MemCache Best Practices

1. **Size Memory Appropriately**: Set MaxMemoryBytes to 10-20% of available RAM for hot data
2. **Choose Right Eviction Policy**: Use LRU for recency-based workloads, LFU for frequency-based
3. **Use Cache Policy Wisely**: Filter large or rarely-accessed entries to maximize hit rate
4. **Monitor Stats**: Regularly check MemStats to ensure efficient memory usage
5. **Warm Critical Keys**: Pre-load frequently accessed keys on startup
6. **Shard Count**: Use default 256 shards, or increase for very high concurrency

## Limitations

- All keys must fit in memory (values are on disk)
- Single writer (concurrent reads are supported)
- No delete operations (use age-based or size-based compaction instead)
- No transactions or batch operations
- No built-in replication or clustering

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) paper
