/*
Package cdb provides a native implementation of cdb, a constant key/value
database with some very nice properties.

For more information on cdb, see the original design doc at http://cr.yp.to/cdb.html.
*/
package cdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

const indexSize = 256 * 8
const indexSize64 = 256 * 16

// Generic types for both uint32 and uint64
type tableGeneric[T Unsigned] struct {
	offset T
	length T
}

type indexGeneric[T Unsigned] [256]tableGeneric[T]

// Original types (for backward compatibility during migration)
type index [256]table
type index64 [256]table64

// CDB represents an open CDB database. It can only be used for reads; to
// create a database, use Writer.
type CDB struct {
	reader io.ReaderAt
	hash   func([]byte) uint32
	index  index
}

// CDB64 represents an open 64-bit CDB database. It can only be used for reads; to
// create a database, use Writer64.
type CDB64 struct {
	reader io.ReaderAt
	hash   func([]byte) uint32
	index  index64
}

// CDBGeneric represents a generic CDB database that works with any unsigned integer size.
// It can only be used for reads; to create a database, use WriterGeneric.
type CDBGeneric[T Unsigned] struct {
	reader io.ReaderAt
	hash   func([]byte) uint32
	index  indexGeneric[T]
}

type table struct {
	offset uint32
	length uint32
}

type table64 struct {
	offset uint64
	length uint64
}

// Open opens an existing CDB database at the given path.
func Open(path string) (*CDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return New(f, nil)
}

// Open64 opens an existing 64-bit CDB database at the given path.
func Open64(path string) (*CDB64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return New64(f, nil)
}

// New opens a new CDB instance for the given io.ReaderAt. It can only be used
// for reads; to create a database, use Writer. The returned CDB instance is
// thread-safe as long as reader is.
//
// If hash is nil, it will default to the CDB hash function. If a database
// was created with a particular hash function, that same hash function must be
// passed to New, or the database will return incorrect results.
func New(reader io.ReaderAt, hash func([]byte) uint32) (*CDB, error) {
	if hash == nil {
		hash = cdbHash
	}

	cdb := &CDB{reader: reader, hash: hash}
	err := cdb.readIndex()
	if err != nil {
		return nil, err
	}

	return cdb, nil
}

// New64 opens a new 64-bit CDB instance for the given io.ReaderAt. It can only be used
// for reads; to create a database, use Writer64. The returned CDB64 instance is
// thread-safe as long as reader is.
//
// If hash is nil, it will default to the CDB hash function. If a database
// was created with a particular hash function, that same hash function must be
// passed to New64, or the database will return incorrect results.
func New64(reader io.ReaderAt, hash func([]byte) uint32) (*CDB64, error) {
	if hash == nil {
		hash = cdbHash
	}

	cdb := &CDB64{reader: reader, hash: hash}
	err := cdb.readIndex()
	if err != nil {
		return nil, err
	}

	return cdb, nil
}

// Get returns the value for a given key, or nil if it can't be found.
func (cdb *CDB) Get(key []byte) ([]byte, error) {
	hash := cdb.hash(key)

	table := cdb.index[hash&0xff]
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (hash >> 8) % table.length
	slot := startingSlot

	for {
		slotOffset := table.offset + (8 * slot)
		slotHash, offset, err := readTuple(cdb.reader, slotOffset)
		if err != nil {
			return nil, err
		}

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == hash {
			value, err := cdb.getValueAt(offset, key)
			if err != nil {
				return nil, err
			} else if value != nil {
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

// Close closes the database to further reads.
func (cdb *CDB) Close() error {
	if closer, ok := cdb.reader.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

func (cdb *CDB) readIndex() error {
	buf := make([]byte, indexSize)
	_, err := cdb.reader.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	for i := 0; i < 256; i++ {
		off := i * 8
		cdb.index[i] = table{
			offset: binary.LittleEndian.Uint32(buf[off : off+4]),
			length: binary.LittleEndian.Uint32(buf[off+4 : off+8]),
		}
	}

	return nil
}

func (cdb *CDB) getValueAt(offset uint32, expectedKey []byte) ([]byte, error) {
	keyLength, valueLength, err := readTuple(cdb.reader, offset)
	if err != nil {
		return nil, err
	}

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil, nil
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = cdb.reader.ReadAt(buf, int64(offset+8))
	if err != nil {
		return nil, err
	}

	// If they keys don't match, this isn't it.
	if bytes.Compare(buf[:keyLength], expectedKey) != 0 {
		return nil, nil
	}

	return buf[keyLength:], nil
}

// Get returns the value for a given key, or nil if it can't be found.
func (cdb *CDB64) Get(key []byte) ([]byte, error) {
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
		slotHash, offset, err := readTuple64(cdb.reader, slotOffset)
		if err != nil {
			return nil, err
		}

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == uint64(hash) {
			value, err := cdb.getValueAt(offset, key)
			if err != nil {
				return nil, err
			} else if value != nil {
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

// Close closes the database to further reads.
func (cdb *CDB64) Close() error {
	if closer, ok := cdb.reader.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

func (cdb *CDB64) readIndex() error {
	buf := make([]byte, indexSize64)
	_, err := cdb.reader.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	for i := 0; i < 256; i++ {
		off := i * 16
		cdb.index[i] = table64{
			offset: binary.LittleEndian.Uint64(buf[off : off+8]),
			length: binary.LittleEndian.Uint64(buf[off+8 : off+16]),
		}
	}

	return nil
}

func (cdb *CDB64) getValueAt(offset uint64, expectedKey []byte) ([]byte, error) {
	keyLength, valueLength, err := readTuple64(cdb.reader, offset)
	if err != nil {
		return nil, err
	}

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil, nil
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = cdb.reader.ReadAt(buf, int64(offset+16))
	if err != nil {
		return nil, err
	}

	// If they keys don't match, this isn't it.
	if bytes.Compare(buf[:keyLength], expectedKey) != 0 {
		return nil, nil
	}

	return buf[keyLength:], nil
}

// Generic CDB methods

// NewGeneric opens a new generic CDB instance for the given io.ReaderAt.
// If hash is nil, it will default to the CDB hash function.
func NewGeneric[T Unsigned](reader io.ReaderAt, hash func([]byte) uint32) (*CDBGeneric[T], error) {
	if hash == nil {
		hash = cdbHash
	}

	cdb := &CDBGeneric[T]{reader: reader, hash: hash}
	err := cdb.readIndex()
	if err != nil {
		return nil, err
	}

	return cdb, nil
}

// Get returns the value for a given key, or nil if it can't be found.
func (cdb *CDBGeneric[T]) Get(key []byte) ([]byte, error) {
	hash := cdb.hash(key)

	table := cdb.index[hash&0xff]
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (T(hash) >> 8) % table.length
	slot := startingSlot

	for {
		var slotSize T
		switch any(*new(T)).(type) {
		case uint32:
			slotSize = 8
		case uint64:
			slotSize = 16
		}

		slotOffset := table.offset + (slotSize * slot)
		slotHash, offset, err := readTupleGeneric[T](cdb.reader, slotOffset)
		if err != nil {
			return nil, err
		}

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == T(hash) {
			value, err := cdb.getValueAt(offset, key)
			if err != nil {
				return nil, err
			} else if value != nil {
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

// Close closes the database to further reads.
func (cdb *CDBGeneric[T]) Close() error {
	if closer, ok := cdb.reader.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

// readIndex reads and parses the index from the database header.
func (cdb *CDBGeneric[T]) readIndex() error {
	var size int
	switch any(*new(T)).(type) {
	case uint32:
		size = indexSize
	case uint64:
		size = indexSize64
	}

	buf := make([]byte, size)
	_, err := cdb.reader.ReadAt(buf, 0)
	if err != nil {
		return err
	}

	var entrySize int
	switch any(*new(T)).(type) {
	case uint32:
		entrySize = 8
	case uint64:
		entrySize = 16
	}

	for i := 0; i < 256; i++ {
		off := i * entrySize
		var offset, length T
		switch any(*new(T)).(type) {
		case uint32:
			offset = T(binary.LittleEndian.Uint32(buf[off : off+4]))
			length = T(binary.LittleEndian.Uint32(buf[off+4 : off+8]))
		case uint64:
			offset = T(binary.LittleEndian.Uint64(buf[off : off+8]))
			length = T(binary.LittleEndian.Uint64(buf[off+8 : off+16]))
		}
		cdb.index[i] = tableGeneric[T]{
			offset: offset,
			length: length,
		}
	}

	return nil
}

// getValueAt reads and validates a key-value pair at the given offset.
func (cdb *CDBGeneric[T]) getValueAt(offset T, expectedKey []byte) ([]byte, error) {
	keyLength, valueLength, err := readTupleGeneric[T](cdb.reader, offset)
	if err != nil {
		return nil, err
	}

	// We can compare key lengths before reading the key at all.
	if int(keyLength) != len(expectedKey) {
		return nil, nil
	}

	buf := make([]byte, keyLength+valueLength)
	var headerSize T
	switch any(*new(T)).(type) {
	case uint32:
		headerSize = 8
	case uint64:
		headerSize = 16
	}

	_, err = cdb.reader.ReadAt(buf, int64(offset+headerSize))
	if err != nil {
		return nil, err
	}

	// If they keys don't match, this isn't it.
	if bytes.Compare(buf[:keyLength], expectedKey) != 0 {
		return nil, nil
	}

	return buf[keyLength:], nil
}

// Backward compatibility type aliases
type CDB32 = CDBGeneric[uint32]
type CDB64Alt = CDBGeneric[uint64]
