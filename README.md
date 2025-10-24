# BitCache

A simple, fast persistent cache in Go library, inspired by the Bitcask design.

## Features

- **Fast**: O(1) lookups with in-memory index
- **Memory Cache Layer**: Optional high-performance read-through cache with bounded memory
- **Reliable**: CRC32 checksums for data integrity
- **Crash-safe**: Hint files enable fast recovery
- **Efficient**: Automatic compaction reclaims disk space
- **Concurrent**: Lock-free reads via atomic radix tree
- **Zero GC Overhead**: Memory cache uses arenas to eliminate GC pressure
- **Simple**: Clean API with minimal dependencies

## Design

BitCache uses a log-structured storage approach inspired by Bitcask:

- **Append-only log**: All writes go sequentially to an active segment file. When it reaches the size threshold, it's rotated and a new segment is created.
- **In-memory index**: An immutable radix tree maps every key to its disk location, enabling O(1) lookups and efficient prefix scans.
- **Memory cache layer**: Optional read-through cache using sharded hash maps with LRU/LFU eviction and zero-GC memory arenas.
- **Compaction**: Periodically rewrites only the latest value for each key, discarding obsolete versions and deleted entries to reclaim space.
- **Crash recovery**: On startup, hint files (embedded segment indexes) allow rebuilding the key directory without scanning the entire log.

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
    
    // Delete a key
    err = db.Delete([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    
    // Scan keys with a prefix
    err = db.Scan([]byte("user:"), func(key []byte) bool {
        fmt.Printf("Found key: %s\n", key)
        return false // continue iteration
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // Get statistics
    stats := db.Stats()
    fmt.Printf("Keys: %d\n", stats.Keys)
    fmt.Printf("Data size: %d bytes\n", stats.DataSize)
    fmt.Printf("Reads: %d, Writes: %d\n", stats.Reads, stats.Writes)
    
    // Compact the database (remove old/deleted entries)
    err = db.CompactN(0) // 0 = compact all segments
    if err != nil {
        log.Fatal(err)
    }
}
```

### Using MemCache Layer

MemCache provides a high-performance read-through cache on top of DiskCache with bounded memory usage and zero GC overhead.

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

### Command-Line Interface

BitCache includes a CLI tool for interactive use and benchmarking.

#### Build the CLI

```bash
cd cmd/bitcache
go build
```

#### Basic Operations

```bash
# Set a key
./bitcache -d /tmp/mydb set mykey myvalue

# Get a key
./bitcache -d /tmp/mydb get mykey

# Delete a key
./bitcache -d /tmp/mydb del mykey

# Scan all keys
./bitcache -d /tmp/mydb scan

# Scan keys with prefix
./bitcache -d /tmp/mydb scan --prefix "user:"

# View statistics
./bitcache -d /tmp/mydb stats

# Compact database
./bitcache -d /tmp/mydb compact

# Compact only oldest 5 segments
./bitcache -d /tmp/mydb compact --count 5
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
- **Zero GC overhead**: Memory arenas eliminate GC pressure

### Write Performance

```
$ ./bitcache -d /tmp/bench bench --op write --keys 10000 --total 100000 --key-size 16 --value-size 128

Running write benchmark...
Configuration:
  Keys: 10000
  Key size: 16 bytes
  Value size: 128 bytes
  Total writes: 100000

=== Write Benchmark Results ===
Total writes: 100000
Unique keys: 10000
Duration: 221ms
Throughput: 452,463 ops/sec
Average latency: 2.21µs
Data written: 13.73 MB
Data throughput: 62.14 MB/sec
```

### Read Performance

```
$ ./bitcache -d /tmp/bench bench --op read --keys 10000 --total 100000 --key-size 16 --value-size 128

Running read benchmark...
Configuration:
  Keys: 10000
  Key size: 16 bytes
  Value size: 128 bytes
  Total reads: 100000

=== Read Benchmark Results ===
Total reads: 100000
Successful reads: 100000
Unique keys: 10000
Duration: 210ms
Throughput: 476,924 ops/sec
Average latency: 2.09µs
Data read: 13.73 MB
Data throughput: 65.50 MB/sec
```

### Custom Benchmarks

```bash
# Write benchmark with custom parameters
./bitcache -d /tmp/bench bench --op write \
  --keys 100000 \
  --total 1000000 \
  --key-size 32 \
  --value-size 256

# Read benchmark
./bitcache -d /tmp/bench bench --op read \
  --keys 100000 \
  --total 1000000 \
  --key-size 32 \
  --value-size 256

# Verify data integrity after benchmark
./bitcache -d /tmp/bench verify --keys 100000 --key-size 32 --value-size 256
```

## API Reference

### Core Interface

```go
type Cache interface {
    // Get retrieves the value for the given key
    // Returns ErrKeyNotFound if the key doesn't exist
    Get(key []byte) ([]byte, error)

    // Set stores a key-value pair in the cache
    Set(key []byte, value []byte) error

    // Delete removes a key from the cache
    Delete(key []byte) error

    // Has checks if a key exists in the cache
    Has(key []byte) bool

    // Stats returns cache statistics
    Stats() Stats

    // Scan iterates through all keys with the given prefix
    // The callback function should return true to stop iteration
    Scan(prefix []byte, fn func(key []byte) bool) error
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

// CompactN compacts the oldest N segments (0 = all segments)
func (d *DiskCache) CompactN(count int) error

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
    Deletes   int64  // Total number of delete operations
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

BitCache uses sensible defaults:

- **Max segment size**: 16 MB (rotates to new segment when reached)
- **File caching**: All segment files are cached once opened (no eviction)
- **Sync policy**: Automatic fsync on segment rotation

### MemCache Architecture

MemCache provides zero-GC overhead through a custom memory arena allocator:

**How Zero-GC Works:**

1. **Large Slab Allocation**: Allocates large byte slices (power-of-2 sizes: 1MB, 2MB, 4MB, 8MB, 16MB, 32MB) upfront
2. **Sub-Allocation**: Keys and values are carved out of these slabs
3. **No Individual Allocations**: After initial slab allocation, no new allocations occur
4. **GC Friendly**: Large slabs are invisible to GC, preventing scanning overhead

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
- **Prototyping**: Quick storage solution for proof-of-concepts
- **Embedded databases**: Simple key-value storage within applications
- No query language or secondary indexes
- Compaction requires temporarily holding data in memory

These limitations make it unsuitable as a primary database for most applications, but ideal for caching scenarios.

## MemCache Best Practices

1. **Size Memory Appropriately**: Set MaxMemoryBytes to 10-20% of available RAM for hot data
2. **Choose Right Eviction Policy**: Use LRU for recency-based workloads, LFU for frequency-based
3. **Use Cache Policy Wisely**: Filter large or rarely-accessed entries to maximize hit rate
4. **Monitor Stats**: Regularly check MemStats to ensure efficient memory usage
5. **Warm Critical Keys**: Pre-load frequently accessed keys on startup
6. **Shard Count**: Use default 256 shards, or increase for very high concurrency

## Limitations

- All keys must fit in memory (values are on disk) but leverages prefix tree compression
- Single writer (concurrent reads are supported)
- No transactions or batch operations
- No built-in replication or clustering

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) paper
- Uses [go-immutable-radix](https://github.com/hashicorp/go-immutable-radix) for the in-memory index
