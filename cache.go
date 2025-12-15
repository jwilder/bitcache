package bitcache

import (
	"encoding/json"
	"errors"
	"sync"
)

var (
	// ErrKeyNotFound is returned when a key is not found in the cache
	ErrKeyNotFound = errors.New("key not found")
	// ErrCacheClosed is returned when attempting to use a closed cache
	ErrCacheClosed = errors.New("cache is closed")
)

// Marshaler defines the interface for converting values to/from byte slices
// This allows DiskCache to persist Go types in any serialization format with minimal allocations
type Marshaler[V any] interface {
	// Marshal encodes a value into the provided buffer
	// Returns the slice of dst that contains the encoded data (may be a new slice if dst is too small)
	// If dst has sufficient capacity, no allocation occurs
	Marshal(value V, dst []byte) ([]byte, error)

	// Unmarshal decodes data into the provided target
	// The caller is responsible for allocating/pooling the target
	Unmarshal(data []byte, target *V) error

	// MarshalSize returns the maximum bytes needed to marshal value
	// Returns -1 if size cannot be determined without marshaling
	MarshalSize(value V) int
}

// Cache defines the interface for a persistent key-value cache with generic value types
type Cache[V any] interface {
	// Get retrieves the value for the given key
	// Returns ErrKeyNotFound if the key doesn't exist
	// Allocates a new V for the result
	Get(key []byte) (V, error)

	// GetInto retrieves the value for the given key into the provided target
	// This allows the caller to reuse/pool V objects to avoid allocations
	// Returns ErrKeyNotFound if the key doesn't exist
	GetInto(key []byte, target *V) error

	// Set stores a key-value pair in the cache
	Set(key []byte, value V) error

	// BatchSet performs bulk inserts for maximum performance
	// Use this for bulk loading operations to avoid batch timeout delays
	BatchSet(entries []struct {
		Key   []byte
		Value V
	}) error

	// Has checks if a key exists in the cache
	Has(key []byte) bool

	// Stats returns cache statistics
	Stats() Stats

	// Close cleanly shuts down the cache, flushing any pending writes and releasing resources
	// After Close is called, any subsequent operations will return ErrCacheClosed
	Close() error
}

// Stats provides information about cache performance and storage
type Stats struct {
	// Keys is the number of keys in the cache
	Keys int64
	// DataSize is the total size of data on disk in bytes
	DataSize int64
	// IndexSize is the size of the in-memory index in bytes
	IndexSize int64
	// Reads is the total number of read operations
	Reads int64
	// Writes is the total number of write operations
	Writes int64
	// Deletes is the total number of delete operations
	Deletes int64
	// Segments is the number of segment files on disk
	Segments int64
}

// ByteSliceMarshaler is a no-op marshaler for []byte values
// Use this when you want to store raw bytes without any encoding
type ByteSliceMarshaler struct{}

func (ByteSliceMarshaler) Marshal(value []byte, dst []byte) ([]byte, error) {
	// Grow dst if needed
	if cap(dst) < len(value) {
		dst = make([]byte, len(value))
	} else {
		dst = dst[:len(value)]
	}
	copy(dst, value)
	return dst, nil
}

func (ByteSliceMarshaler) Unmarshal(data []byte, target *[]byte) error {
	*target = data // Just point to existing data (caller should copy if needed)
	return nil
}

func (ByteSliceMarshaler) MarshalSize(value []byte) int {
	return len(value)
}

// JSONMarshaler marshals values using JSON encoding
// This is useful for storing structs or other Go types
type JSONMarshaler[V any] struct{}

func (JSONMarshaler[V]) Marshal(value V, dst []byte) ([]byte, error) {
	// Try to marshal directly, falling back to allocation if needed
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}

	// Reuse dst buffer if it has sufficient capacity
	if cap(dst) >= len(data) {
		dst = dst[:len(data)]
		copy(dst, data)
		return dst, nil
	}

	return data, nil
}

func (JSONMarshaler[V]) Unmarshal(data []byte, target *V) error {
	return json.Unmarshal(data, target)
}

func (JSONMarshaler[V]) MarshalSize(value V) int {
	// JSON doesn't have predictable size without marshaling
	return -1
}

// BufferPool manages reusable byte buffers for marshaling operations
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a new buffer pool with the specified initial size
func NewBufferPool(initialSize int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, initialSize)
			},
		},
	}
}

// Get retrieves a buffer from the pool, resetting its length to 0
func (p *BufferPool) Get() []byte {
	buf := p.pool.Get().([]byte)
	return buf[:0] // Reset length but keep capacity
}

// Put returns a buffer to the pool
// Extremely large buffers are discarded to prevent memory bloat
func (p *BufferPool) Put(buf []byte) {
	const maxBufferSize = 1024 * 1024 // 1MB
	if cap(buf) < maxBufferSize {
		p.pool.Put(buf)
	}
}
