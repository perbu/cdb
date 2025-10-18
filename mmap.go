package cdb

import (
	"bytes"
	"encoding/binary"
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

	cdb := &MmapCDB{
		data: data,
		file: file,
	}

	return cdb, nil
}

// Get returns the value for a given key using memory-mapped access.
func (cdb *MmapCDB) Get(key []byte) ([]byte, error) {
	hash := cdbHash(key)

	table := readTableAt(cdb.data, uint8(hash&0xff))
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (uint64(hash) >> 8) % table.length
	slot := startingSlot

	for {
		slotOffset := table.offset + (16 * slot)
		slotHash, offset := readTupleMmap(cdb.data, slotOffset)

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == uint64(hash) {
			value := getValueAt(cdb.data, offset, key)
			if value != nil {
				return value, nil
			}
		}

		slot = (slot + 1) % table.length
		if slot == startingSlot {
			break
		}
	}

	return nil, nil
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
		if err := cdb.file.Close(); err == nil {
			errs = append(errs, err)
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
	hash := cdbHash(key)

	table := readTableAt(cdb.data, uint8(hash&0xff))
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (uint64(hash) >> 8) % table.length
	slot := startingSlot

	for {
		slotOffset := table.offset + (16 * slot)
		slotHash, offset := readTupleMmap(cdb.data, slotOffset)

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == uint64(hash) {
			value := getValueAt(cdb.data, offset, key)
			if value != nil {
				return value, nil
			}
		}

		slot = (slot + 1) % table.length
		if slot == startingSlot {
			break
		}
	}

	return nil, nil
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
	return func(yield func([]byte, []byte) bool) {
		// Find the minimum table offset to determine where data section ends
		var endPos uint64
		endPos = uint64(len(cdb.data)) // Start with file size, then find minimum table offset

		for i := 0; i < 256; i++ {
			table := readTableAt(cdb.data, uint8(i))
			if table.length > 0 && table.offset < endPos {
				endPos = table.offset
			}
		}

		// If no hash tables exist, data goes to end of file
		if endPos == uint64(len(cdb.data)) {
			// For empty database, endPos should be indexSize
			if endPos == uint64(indexSize) {
				endPos = uint64(indexSize)
			}
		}

		pos := uint64(indexSize)
		for pos < endPos {
			// Ensure we don't read past the end of data
			if int(pos)+16 > len(cdb.data) {
				return
			}

			keyLength, valueLength := readTupleMmap(cdb.data, pos)

			// Calculate total record size and check bounds
			totalSize := 16 + keyLength + valueLength
			if int(pos+totalSize) > len(cdb.data) {
				return
			}

			// Extract key and value directly from data
			dataStart := int(pos + 16)
			keyEnd := dataStart + int(keyLength)
			valueEnd := keyEnd + int(valueLength)

			key := cdb.data[dataStart:keyEnd]
			value := cdb.data[keyEnd:valueEnd]

			// Yield the key-value pair
			if !yield(key, value) {
				return // Early termination requested
			}

			pos += totalSize
		}
	}
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

// readTupleMmap reads a 64-bit tuple from memory-mapped data.
func readTupleMmap(data []byte, offset uint64) (uint64, uint64) {
	if int(offset)+16 > len(data) {
		return 0, 0
	}
	first := binary.LittleEndian.Uint64(data[offset : offset+8])
	second := binary.LittleEndian.Uint64(data[offset+8 : offset+16])
	return first, second
}

// readTableAt reads a table entry from the data at the given table number.
func readTableAt(data []byte, tableNum uint8) table {
	off := int(tableNum) * 16
	return table{
		offset: binary.LittleEndian.Uint64(data[off : off+8]),
		length: binary.LittleEndian.Uint64(data[off+8 : off+16]),
	}
}

// getValueAt retrieves a value at the given offset from the data.
func getValueAt(data []byte, offset uint64, expectedKey []byte) []byte {
	if int(offset)+16 > len(data) {
		return nil
	}

	keyLength, valueLength := readTupleMmap(data, offset)

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil
	}

	dataStart := int(offset + 16)
	dataEnd := dataStart + int(keyLength+valueLength)
	if dataEnd > len(data) {
		return nil
	}

	keyEnd := dataStart + int(keyLength)
	key := data[dataStart:keyEnd]

	// If the keys don't match, this isn't it.
	if !bytes.Equal(key, expectedKey) {
		return nil
	}

	return data[keyEnd:dataEnd]
}

// Size returns the size of the memory-mapped data.
func (cdb *MmapCDB) Size() int {
	return len(cdb.data)
}

// All returns an iterator over all key-value pairs in the database.
func (cdb *MmapCDB) All() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		// Find the minimum table offset to determine where data section ends
		var endPos uint64
		endPos = uint64(len(cdb.data)) // Start with file size, then find minimum table offset

		for i := 0; i < 256; i++ {
			table := readTableAt(cdb.data, uint8(i))
			if table.length > 0 && table.offset < endPos {
				endPos = table.offset
			}
		}

		// If no hash tables exist, data goes to end of file
		if endPos == uint64(len(cdb.data)) {
			// For empty database, endPos should be indexSize
			if endPos == uint64(indexSize) {
				endPos = uint64(indexSize)
			}
		}

		pos := uint64(indexSize)
		for pos < endPos {
			// Ensure we don't read past the end of mapped data
			if int(pos)+16 > len(cdb.data) {
				return
			}

			keyLength, valueLength := readTupleMmap(cdb.data, pos)

			// Calculate total record size and check bounds
			totalSize := 16 + keyLength + valueLength
			if int(pos+totalSize) > len(cdb.data) {
				return
			}

			// Extract key and value directly from mmap data
			dataStart := int(pos + 16)
			keyEnd := dataStart + int(keyLength)
			valueEnd := keyEnd + int(valueLength)

			key := cdb.data[dataStart:keyEnd]
			value := cdb.data[keyEnd:valueEnd]

			// Yield the key-value pair
			if !yield(key, value) {
				return // Early termination requested
			}

			pos += totalSize
		}
	}
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
