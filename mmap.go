package cdb

import (
	"errors"
	"fmt"
	"iter"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// MmapCDB represents a memory-mapped 64-bit CDB database.
// The returned key and value slices from its methods point directly to the
// memory-mapped file data and are valid only until the database is closed.
// Do not modify the contents of the returned slices.
type MmapCDB struct {
	data []byte
	file *os.File
}

// Open opens a 64-bit CDB file at the given path using memory mapping for reads.
func Open(path string) (*MmapCDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("os.Open(%q): %w", path, err)
	}

	return Mmap(f)
}

// Mmap creates a memory-mapped 64-bit CDB from an open file.
func Mmap(file *os.File) (*MmapCDB, error) {
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close() // not much we can do here.
		return nil, fmt.Errorf("file.Stat: %w", err)
	}
	size := int(stat.Size())
	if size < indexSize {
		_ = file.Close()
		return nil, fmt.Errorf("size < indexSize: %w", syscall.EINVAL)
	}

	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("unix.Mmap: %w", err)
	}

	// Point lookups are the dominant workload; disable readahead to reduce
	// page-cache pressure. Failure is non-fatal — the mapping is still usable.
	_ = unix.Madvise(data, unix.MADV_RANDOM)

	cdb := &MmapCDB{
		data: data,
		file: file,
	}

	return cdb, nil
}

// Get returns the value for a given key using memory-mapped access.
func (cdb *MmapCDB) Get(key []byte) ([]byte, error) {
	return getFromBytes(cdb.data, key)
}

// Close unmaps the file and closes the file descriptor.
func (cdb *MmapCDB) Close() error {
	var errs []error
	if cdb.data != nil {
		if err := unix.Munmap(cdb.data); err != nil {
			if !errors.Is(err, syscall.EINVAL) {
				errs = append(errs, fmt.Errorf("munmap: %w", err))
			}
		}
		cdb.data = nil
	}
	if cdb.file != nil {
		if err := cdb.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("file.Close: %w", err))
		}
		cdb.file = nil
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// InMemoryCDB represents an in-memory 64-bit CDB database.
// The data slice must remain valid for the lifetime of the InMemoryCDB.
// The returned key and value slices from its methods point directly to the
// underlying data and are valid as long as the data slice remains valid.
// Do not modify the contents of the returned slices.
type InMemoryCDB struct {
	data []byte
}

// NewInMemory creates an in-memory 64-bit CDB from a byte slice containing
// a complete CDB database. The caller must ensure the data slice remains
// valid for the lifetime of the InMemoryCDB and is not modified.
func NewInMemory(data []byte) (*InMemoryCDB, error) {
	if len(data) < indexSize {
		return nil, fmt.Errorf("data size < indexSize: %w", syscall.EINVAL)
	}
	return &InMemoryCDB{data: data}, nil
}

// Get returns the value for a given key from the in-memory CDB.
func (cdb *InMemoryCDB) Get(key []byte) ([]byte, error) {
	return getFromBytes(cdb.data, key)
}

// Close is a no-op for InMemoryCDB since there are no resources to release.
// The caller is responsible for managing the lifetime of the underlying data slice.
func (cdb *InMemoryCDB) Close() error {
	return nil
}

// Size returns the size of the in-memory data.
func (cdb *InMemoryCDB) Size() int {
	return len(cdb.data)
}

// All returns an iterator over all key-value pairs in the database.
func (cdb *InMemoryCDB) All() iter.Seq2[[]byte, []byte] {
	return allFromBytes(cdb.data)
}

// Keys returns an iterator over all keys in the database.
func (cdb *InMemoryCDB) Keys() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for key := range cdb.All() {
			if !yield(key) {
				return
			}
		}
	}
}

// Values returns an iterator over all values in the database.
func (cdb *InMemoryCDB) Values() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, value := range cdb.All() {
			if !yield(value) {
				return
			}
		}
	}
}

// Size returns the size of the memory-mapped data.
func (cdb *MmapCDB) Size() int {
	return len(cdb.data)
}

// All returns an iterator over all key-value pairs in the database.
func (cdb *MmapCDB) All() iter.Seq2[[]byte, []byte] {
	return allFromBytes(cdb.data)
}

// Keys returns an iterator over all keys in the database.
func (cdb *MmapCDB) Keys() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for key := range cdb.All() {
			if !yield(key) {
				return
			}
		}
	}
}

// Values returns an iterator over all values in the database.
func (cdb *MmapCDB) Values() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, value := range cdb.All() {
			if !yield(value) {
				return
			}
		}
	}
}
