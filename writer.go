package cdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"sync"
)

var ErrTooMuchData = errors.New("CDB files are limited to 4GB of data")
var ErrTooMuchData64 = errors.New("CDB64 files are limited to 8EB of data")

// Writer provides an API for creating a CDB database record by record.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
type Writer struct {
	hash         func([]byte) uint32
	writer       io.WriteSeeker
	entries      [256][]entry
	finalizeOnce sync.Once

	bufferedWriter      *bufio.Writer
	bufferedOffset      int64
	estimatedFooterSize int64
}

// Generic entry type for both uint32 and uint64
type entryGeneric[T Unsigned] struct {
	hash   uint32
	offset T
}

type entry struct {
	hash   uint32
	offset uint32
}

// Writer64 provides an API for creating a 64-bit CDB database record by record.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
type Writer64 struct {
	hash         func([]byte) uint32
	writer       io.WriteSeeker
	entries      [256][]entry64
	finalizeOnce sync.Once

	bufferedWriter      *bufio.Writer
	bufferedOffset      int64
	estimatedFooterSize int64
}

type entry64 struct {
	hash   uint32
	offset uint64
}

// WriterGeneric provides a generic API for creating CDB databases with configurable integer sizes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
type WriterGeneric[T Unsigned] struct {
	hash         func([]byte) uint32
	writer       io.WriteSeeker
	entries      [256][]entryGeneric[T]
	finalizeOnce sync.Once

	bufferedWriter      *bufio.Writer
	bufferedOffset      int64
	estimatedFooterSize int64
}

// Create opens a CDB database at the given path. If the file exists, it will
// be overwritten. The returned database is not safe for concurrent writes.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return NewWriter(f, nil)
}

// NewWriter opens a CDB database for the given io.WriteSeeker.
//
// If hash is nil, it will default to the CDB hash function.
func NewWriter(writer io.WriteSeeker, hash func([]byte) uint32) (*Writer, error) {
	// Leave 256 * 8 bytes for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, indexSize))
	if err != nil {
		return nil, err
	}

	if hash == nil {
		hash = cdbHash
	}

	return &Writer{
		hash:           hash,
		writer:         writer,
		bufferedWriter: bufio.NewWriterSize(writer, 65536),
		bufferedOffset: indexSize,
	}, nil
}

// Create64 opens a 64-bit CDB database at the given path. If the file exists, it will
// be overwritten. The returned database is not safe for concurrent writes.
func Create64(path string) (*Writer64, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return NewWriter64(f, nil)
}

// NewWriter64 opens a 64-bit CDB database for the given io.WriteSeeker.
//
// If hash is nil, it will default to the CDB hash function.
func NewWriter64(writer io.WriteSeeker, hash func([]byte) uint32) (*Writer64, error) {
	// Leave 256 * 16 bytes for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, indexSize64))
	if err != nil {
		return nil, err
	}

	if hash == nil {
		hash = cdbHash
	}

	return &Writer64{
		hash:           hash,
		writer:         writer,
		bufferedWriter: bufio.NewWriterSize(writer, 65536),
		bufferedOffset: indexSize64,
	}, nil
}

// Put adds a key/value pair to the database. If the amount of data written
// would exceed the limit, Put returns ErrTooMuchData.
func (cdb *Writer) Put(key, value []byte) error {
	entrySize := int64(8 + len(key) + len(value))
	if (cdb.bufferedOffset + entrySize + cdb.estimatedFooterSize + 16) > math.MaxUint32 {
		return ErrTooMuchData
	}

	// Record the entry in the hash table, to be written out at the end.
	hash := cdb.hash(key)
	table := hash & 0xff

	entry := entry{hash: hash, offset: uint32(cdb.bufferedOffset)}
	cdb.entries[table] = append(cdb.entries[table], entry)

	// Write the key length, then value length, then key, then value.
	err := writeTuple(cdb.bufferedWriter, uint32(len(key)), uint32(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(value)
	if err != nil {
		return err
	}

	cdb.bufferedOffset += entrySize
	cdb.estimatedFooterSize += 16
	return nil
}

// Close finalizes the database, then closes it to further writes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Close() error {
	var err error
	cdb.finalizeOnce.Do(func() {
		_, err = cdb.finalize()
	})

	if err != nil {
		return err
	}

	if closer, ok := cdb.writer.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

// Freeze finalizes the database, then opens it for reads. If the stream cannot
// be converted to a io.ReaderAt, Freeze will return os.ErrInvalid.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Freeze() (*CDB, error) {
	var err error
	var index index
	cdb.finalizeOnce.Do(func() {
		index, err = cdb.finalize()
	})

	if err != nil {
		return nil, err
	}

	if readerAt, ok := cdb.writer.(io.ReaderAt); ok {
		return &CDB{reader: readerAt, index: index, hash: cdb.hash}, nil
	} else {
		return nil, os.ErrInvalid
	}
}

func (cdb *Writer) finalize() (index, error) {
	var index index

	// Write the hashtables out, one by one, at the end of the file.
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := uint32(len(tableEntries) << 1)

		index[i] = table{
			offset: uint32(cdb.bufferedOffset),
			length: tableSize,
		}

		sorted := make([]entry, tableSize)
		for _, entry := range tableEntries {
			slot := (entry.hash >> 8) % tableSize

			for {
				if sorted[slot].hash == 0 {
					sorted[slot] = entry
					break
				}

				slot = (slot + 1) % tableSize
			}
		}

		for _, entry := range sorted {
			err := writeTuple(cdb.bufferedWriter, entry.hash, entry.offset)
			if err != nil {
				return index, err
			}

			cdb.bufferedOffset += 8
			if cdb.bufferedOffset > math.MaxUint32 {
				return index, ErrTooMuchData
			}
		}
	}

	// We're done with the buffer.
	err := cdb.bufferedWriter.Flush()
	cdb.bufferedWriter = nil
	if err != nil {
		return index, err
	}

	// Seek to the beginning of the file and write out the index.
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return index, err
	}

	buf := make([]byte, indexSize)
	for i, table := range index {
		off := i * 8
		binary.LittleEndian.PutUint32(buf[off:off+4], table.offset)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], table.length)
	}

	_, err = cdb.writer.Write(buf)
	if err != nil {
		return index, err
	}

	return index, nil
}

// Put adds a key/value pair to the 64-bit database. If the amount of data written
// would exceed the limit, Put returns ErrTooMuchData64.
func (cdb *Writer64) Put(key, value []byte) error {
	const maxInt64 = int64(^uint64(0) >> 1)

	entrySize := int64(16 + len(key) + len(value))
	// Proactively check if adding this entry and its footer data could overflow.
	if maxInt64-entrySize < cdb.bufferedOffset ||
		maxInt64-(entrySize+32) < cdb.bufferedOffset+cdb.estimatedFooterSize {
		return ErrTooMuchData64
	}

	// Record the entry in the hash table, to be written out at the end.
	hash := cdb.hash(key)
	table := hash & 0xff

	entry := entry64{hash: hash, offset: uint64(cdb.bufferedOffset)}
	cdb.entries[table] = append(cdb.entries[table], entry)

	// Write the key length, then value length, then key, then value.
	err := writeTuple64(cdb.bufferedWriter, uint64(len(key)), uint64(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(value)
	if err != nil {
		return err
	}

	cdb.bufferedOffset += entrySize
	cdb.estimatedFooterSize += 32
	return nil
}

// Close finalizes the database, then closes it to further writes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer64) Close() error {
	var err error
	cdb.finalizeOnce.Do(func() {
		_, err = cdb.finalize()
	})

	if err != nil {
		return err
	}

	if closer, ok := cdb.writer.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

// Freeze finalizes the database, then opens it for reads. If the stream cannot
// be converted to a io.ReaderAt, Freeze will return os.ErrInvalid.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer64) Freeze() (*CDB64, error) {
	var err error
	var index index64
	cdb.finalizeOnce.Do(func() {
		index, err = cdb.finalize()
	})

	if err != nil {
		return nil, err
	}

	if readerAt, ok := cdb.writer.(io.ReaderAt); ok {
		return &CDB64{reader: readerAt, index: index, hash: cdb.hash}, nil
	} else {
		return nil, os.ErrInvalid
	}
}

func (cdb *Writer64) finalize() (index64, error) {
	var index index64

	// Write the hashtables out, one by one, at the end of the file.
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := uint64(len(tableEntries) << 1)

		index[i] = table64{
			offset: uint64(cdb.bufferedOffset),
			length: tableSize,
		}

		sorted := make([]entry64, tableSize)
		for _, entry := range tableEntries {
			slot := (uint64(entry.hash) >> 8) % tableSize

			for {
				if sorted[slot].hash == 0 {
					sorted[slot] = entry
					break
				}

				slot = (slot + 1) % tableSize
			}
		}

		for _, entry := range sorted {
			err := writeTuple64(cdb.bufferedWriter, uint64(entry.hash), entry.offset)
			if err != nil {
				return index, err
			}

			const maxInt64 = int64(^uint64(0) >> 1)
			if maxInt64-16 < cdb.bufferedOffset {
				return index, ErrTooMuchData64
			}
			cdb.bufferedOffset += 16
		}
	}

	// We're done with the buffer.
	err := cdb.bufferedWriter.Flush()
	cdb.bufferedWriter = nil
	if err != nil {
		return index, err
	}

	// Seek to the beginning of the file and write out the index.
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return index, err
	}

	buf := make([]byte, indexSize64)
	for i, table := range index {
		off := i * 16
		binary.LittleEndian.PutUint64(buf[off:off+8], table.offset)
		binary.LittleEndian.PutUint64(buf[off+8:off+16], table.length)
	}

	_, err = cdb.writer.Write(buf)
	if err != nil {
		return index, err
	}

	return index, nil
}

// Generic Writer methods

// NewWriterGeneric creates a new generic CDB writer for the given io.WriteSeeker.
// If hash is nil, it will default to the CDB hash function.
func NewWriterGeneric[T Unsigned](writer io.WriteSeeker, hash func([]byte) uint32) (*WriterGeneric[T], error) {
	var size int64
	switch any(*new(T)).(type) {
	case uint32:
		size = indexSize
	case uint64:
		size = indexSize64
	}

	// Leave space for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, size))
	if err != nil {
		return nil, err
	}

	if hash == nil {
		hash = cdbHash
	}

	return &WriterGeneric[T]{
		hash:           hash,
		writer:         writer,
		bufferedWriter: bufio.NewWriterSize(writer, 65536),
		bufferedOffset: size,
	}, nil
}

// Put adds a key/value pair to the database.
func (cdb *WriterGeneric[T]) Put(key, value []byte) error {
	var headerSize int64
	switch any(*new(T)).(type) {
	case uint32:
		headerSize = 8
	case uint64:
		headerSize = 16
	}

	entrySize := headerSize + int64(len(key)) + int64(len(value))

	var maxSize int64
	switch any(*new(T)).(type) {
	case uint32:
		maxSize = math.MaxUint32
	case uint64:
		maxSize = int64(^uint64(0) >> 1) // MaxInt64
	}

	if (cdb.bufferedOffset + entrySize + cdb.estimatedFooterSize + headerSize) > maxSize {
		switch any(*new(T)).(type) {
		case uint32:
			return ErrTooMuchData
		case uint64:
			return ErrTooMuchData64
		}
	}

	// Record the entry in the hash table, to be written out at the end.
	hash := cdb.hash(key)
	table := hash & 0xff

	entry := entryGeneric[T]{hash: hash, offset: T(cdb.bufferedOffset)}
	cdb.entries[table] = append(cdb.entries[table], entry)

	// Write the key length, then value length, then key, then value.
	err := writeTupleGeneric[T](cdb.bufferedWriter, T(len(key)), T(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(value)
	if err != nil {
		return err
	}

	cdb.bufferedOffset += entrySize
	cdb.estimatedFooterSize += headerSize
	return nil
}

// Close finalizes the database, then closes it to further writes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *WriterGeneric[T]) Close() error {
	var err error
	cdb.finalizeOnce.Do(func() {
		_, err = cdb.finalize()
	})

	if err != nil {
		return err
	}

	if closer, ok := cdb.writer.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

// Freeze finalizes the database, then opens it for reads.
func (cdb *WriterGeneric[T]) Freeze() (*CDBGeneric[T], error) {
	var err error
	var index indexGeneric[T]
	cdb.finalizeOnce.Do(func() {
		index, err = cdb.finalize()
	})

	if err != nil {
		return nil, err
	}

	if readerAt, ok := cdb.writer.(io.ReaderAt); ok {
		return &CDBGeneric[T]{reader: readerAt, index: index, hash: cdb.hash}, nil
	} else {
		return nil, os.ErrInvalid
	}
}

func (cdb *WriterGeneric[T]) finalize() (indexGeneric[T], error) {
	var index indexGeneric[T]

	// Write the hashtables out, one by one, at the end of the file.
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := T(len(tableEntries) << 1)

		index[i] = tableGeneric[T]{
			offset: T(cdb.bufferedOffset),
			length: tableSize,
		}

		sorted := make([]entryGeneric[T], tableSize)
		for _, entry := range tableEntries {
			slot := (T(entry.hash) >> 8) % tableSize

			for {
				if sorted[slot].hash == 0 {
					sorted[slot] = entry
					break
				}

				slot = (slot + 1) % tableSize
			}
		}

		for _, entry := range sorted {
			err := writeTupleGeneric[T](cdb.bufferedWriter, T(entry.hash), entry.offset)
			if err != nil {
				return index, err
			}

			var headerSize int64
			switch any(*new(T)).(type) {
			case uint32:
				headerSize = 8
			case uint64:
				headerSize = 16
			}

			var maxSize int64
			switch any(*new(T)).(type) {
			case uint32:
				maxSize = math.MaxUint32
			case uint64:
				maxSize = int64(^uint64(0) >> 1) // MaxInt64
			}

			if maxSize-headerSize < cdb.bufferedOffset {
				switch any(*new(T)).(type) {
				case uint32:
					return index, ErrTooMuchData
				case uint64:
					return index, ErrTooMuchData64
				}
			}
			cdb.bufferedOffset += headerSize
		}
	}

	// We're done with the buffer.
	err := cdb.bufferedWriter.Flush()
	cdb.bufferedWriter = nil
	if err != nil {
		return index, err
	}

	// Seek to the beginning of the file and write out the index.
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return index, err
	}

	var size int
	var entrySize int
	switch any(*new(T)).(type) {
	case uint32:
		size = indexSize
		entrySize = 8
	case uint64:
		size = indexSize64
		entrySize = 16
	}

	buf := make([]byte, size)
	for i, table := range index {
		off := i * entrySize
		switch any(*new(T)).(type) {
		case uint32:
			binary.LittleEndian.PutUint32(buf[off:off+4], uint32(table.offset))
			binary.LittleEndian.PutUint32(buf[off+4:off+8], uint32(table.length))
		case uint64:
			binary.LittleEndian.PutUint64(buf[off:off+8], uint64(table.offset))
			binary.LittleEndian.PutUint64(buf[off+8:off+16], uint64(table.length))
		}
	}

	_, err = cdb.writer.Write(buf)
	if err != nil {
		return index, err
	}

	return index, nil
}

// Backward compatibility type aliases
type Writer32 = WriterGeneric[uint32]
type Writer64Alt = WriterGeneric[uint64]
