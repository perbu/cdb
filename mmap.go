package cdb

import (
	"encoding/binary"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// MmapCDB represents a memory-mapped 64-bit CDB database.
type MmapCDB struct {
	data  []byte
	hash  func([]byte) uint32
	index index
	file  *os.File
}

// OpenMmap opens a 64-bit CDB file using memory mapping.
func OpenMmap(path string) (*MmapCDB, error) {
	return OpenMmapWithHash(path, nil)
}

// OpenMmapWithHash opens a 64-bit CDB file using memory mapping with a custom hash function.
func OpenMmapWithHash(path string, hash func([]byte) uint32) (*MmapCDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	if hash == nil {
		hash = cdbHash
	}

	return NewMmap(f, hash)
}

// NewMmap creates a memory-mapped 64-bit CDB from an open file.
func NewMmap(file *os.File, hash func([]byte) uint32) (*MmapCDB, error) {
	if hash == nil {
		hash = cdbHash
	}

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
		hash: hash,
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
	hash := cdb.hash(key)

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
	if !equalBytes(key, expectedKey) {
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

// equalBytes compares two byte slices for equality.
// This is equivalent to bytes.Equal but avoids the import.
func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Size returns the size of the memory-mapped data.
func (cdb *MmapCDB) Size() int {
	return len(cdb.data)
}
