package cdb

import (
	"bytes"
	"encoding/binary"
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
	data  []byte
	index index
	file  *os.File
}

// OpenMmap opens a 64-bit CDB file at the given path using memory mapping for reads.
func OpenMmap(path string) (*MmapCDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return NewMmap(f)
}

// NewMmap creates a memory-mapped 64-bit CDB from an open file.
func NewMmap(file *os.File) (*MmapCDB, error) {

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	size := int(stat.Size())
	if size < indexSize {
		file.Close()
		return nil, syscall.EINVAL
	}

	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	cdb := &MmapCDB{
		data: data,
		file: file,
	}

	err = cdb.readIndex()
	if err != nil {
		cdb.Close()
		return nil, err
	}

	return cdb, nil
}

// Get returns the value for a given key using memory-mapped access.
func (cdb *MmapCDB) Get(key []byte) ([]byte, error) {
	hash := cdbHash(key)

	table := cdb.index[hash&0xff]
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
			value := cdb.getValueAtMmap(offset, key)
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
	var err error
	if cdb.data != nil {
		err = unix.Munmap(cdb.data)
		cdb.data = nil
	}
	if cdb.file != nil {
		if closeErr := cdb.file.Close(); err == nil {
			err = closeErr
		}
		cdb.file = nil
	}
	return err
}

// readIndex reads the index from the memory-mapped data.
func (cdb *MmapCDB) readIndex() error {
	if len(cdb.data) < indexSize {
		return syscall.EINVAL
	}

	for i := 0; i < 256; i++ {
		off := i * 16
		cdb.index[i] = table{
			offset: binary.LittleEndian.Uint64(cdb.data[off : off+8]),
			length: binary.LittleEndian.Uint64(cdb.data[off+8 : off+16]),
		}
	}

	return nil
}

// getValueAtMmap retrieves a value at the given offset using memory-mapped data.
func (cdb *MmapCDB) getValueAtMmap(offset uint64, expectedKey []byte) []byte {
	if int(offset)+16 > len(cdb.data) {
		return nil
	}

	keyLength, valueLength := readTupleMmap(cdb.data, offset)

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil
	}

	dataStart := int(offset + 16)
	dataEnd := dataStart + int(keyLength+valueLength)
	if dataEnd > len(cdb.data) {
		return nil
	}

	keyEnd := dataStart + int(keyLength)
	key := cdb.data[dataStart:keyEnd]

	// If the keys don't match, this isn't it.
	if !bytes.Equal(key, expectedKey) {
		return nil
	}

	return cdb.data[keyEnd:dataEnd]
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
			if cdb.index[i].length > 0 && cdb.index[i].offset < endPos {
				endPos = cdb.index[i].offset
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
		for key, _ := range cdb.All() {
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
