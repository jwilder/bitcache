package bitcache

import (
	"encoding/json"
	"errors"
)

var (
	// ErrKeyNotFound is returned when a key is not found in the cache
	ErrKeyNotFound = errors.New("key not found")
	// ErrCacheClosed is returned when attempting to use a closed cache
	ErrCacheClosed = errors.New("cache is closed")
)

// Marshaler defines the interface for converting values to/from byte slices
// This allows DiskCache to persist Go types in any serialization format
type Marshaler[V any] interface {
	// Marshal converts a value to bytes for storage
	Marshal(value V) ([]byte, error)
	// Unmarshal converts bytes back to a value
	Unmarshal(data []byte) (V, error)
}

// Cache defines the interface for a persistent key-value cache with generic value types
type Cache[V any] interface {
	// Get retrieves the value for the given key
	// Returns ErrKeyNotFound if the key doesn't exist
	Get(key []byte) (V, error)

	// Set stores a key-value pair in the cache
	Set(key []byte, value V) error

	// Delete removes a key from the cache
	// Returns ErrKeyNotFound if the key doesn't exist
	Delete(key []byte) error

	// Has checks if a key exists in the cache
	Has(key []byte) bool

	// Stats returns cache statistics
	Stats() Stats

	// Scan iterates through all keys with the given prefix and calls the function for each key
	// The function should return true to stop iteration, false to continue
	Scan(prefix []byte, fn func(key []byte) bool) error

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

func (ByteSliceMarshaler) Marshal(value []byte) ([]byte, error) {
	return value, nil
}

func (ByteSliceMarshaler) Unmarshal(data []byte) ([]byte, error) {
	return data, nil
}

// JSONMarshaler marshals values using JSON encoding
// This is useful for storing structs or other Go types
type JSONMarshaler[V any] struct{}

func (JSONMarshaler[V]) Marshal(value V) ([]byte, error) {
	return json.Marshal(value)
}

func (JSONMarshaler[V]) Unmarshal(data []byte) (V, error) {
	var value V
	err := json.Unmarshal(data, &value)
	return value, err
}
