package cdb

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

var ErrTooMuchData = errors.New("CDB files are limited to 8EB of data")

const indexSize = 256 * 16

type table struct {
	offset uint64
	length uint64
}

type index [256]table

type entry struct {
	hash   uint32
	offset uint64
}

// Writer provides an API for creating a 64-bit CDB database record by record.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
type Writer struct {
	writer       io.WriteSeeker
	entries      [256][]entry
	finalizeOnce sync.Once

	bufferedWriter      *bufio.Writer
	bufferedOffset      int64
	estimatedFooterSize int64
}

// Create opens a 64-bit CDB database at the given path. If the file exists, it will
// be overwritten. The returned database is not safe for concurrent writes.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return NewWriter(f)
}

// NewWriter opens a 64-bit CDB database for the given io.WriteSeeker.
func NewWriter(writer io.WriteSeeker) (*Writer, error) {
	// Leave 256 * 16 bytes for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, indexSize))
	if err != nil {
		return nil, err
	}

	return &Writer{
		writer:         writer,
		bufferedWriter: bufio.NewWriterSize(writer, 65536),
		bufferedOffset: indexSize,
	}, nil
}

// Put adds a key/value pair to the database. If the amount of data written
// would exceed the limit, Put returns ErrTooMuchData.
func (cdb *Writer) Put(key, value []byte) error {
	entrySize := int64(16 + len(key) + len(value))
	const maxInt64 = int64(^uint64(0) >> 1)
	if (cdb.bufferedOffset + entrySize + cdb.estimatedFooterSize + 32) > maxInt64 {
		return ErrTooMuchData
	}

	// Record the entry in the hash table, to be written out at the end.
	hash := cdbHash(key)
	table := hash & 0xff

	entry := entry{hash: hash, offset: uint64(cdb.bufferedOffset)}
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

	// We approximate the footer size: 16 bytes per entry and 16 per table.
	// This approximation becomes more accurate over time.
	totalEntries := len(cdb.entries[table])
	cdb.estimatedFooterSize += 16
	if totalEntries&(totalEntries-1) == 0 {
		// Reallocate hash tables
		cdb.estimatedFooterSize += 16 * int64(totalEntries)
	}

	return nil
}

// Close finalizes the database and closes the underlying io.WriteSeeker.
func (cdb *Writer) Close() error {
	err := cdb.bufferedWriter.Flush()
	if err != nil {
		return err
	}

	_, err = cdb.finalize()
	if err != nil {
		return err
	}

	if closer, ok := cdb.writer.(io.Closer); ok {
		if closeErr := closer.Close(); err == nil {
			err = closeErr
		}
	}

	return err
}

// Freeze finalizes the database and returns an MmapCDB instance for reading.
func (cdb *Writer) Freeze() (*MmapCDB, error) {
	err := cdb.bufferedWriter.Flush()
	if err != nil {
		return nil, err
	}

	_, err = cdb.finalize()
	if err != nil {
		return nil, err
	}

	// Convert io.WriteSeeker to *os.File if possible
	if file, ok := cdb.writer.(*os.File); ok {
		return NewMmap(file)
	}

	// If it's not a file, we can't create a memory-mapped version
	return nil, errors.New("cannot create memory-mapped CDB from non-file WriteSeeker")
}

func (cdb *Writer) finalize() (index, error) {
	var err error
	cdb.finalizeOnce.Do(func() {
		err = cdb.doFinalize()
	})

	// Return empty index since doFinalize already writes the index to file
	return index{}, err
}

func (cdb *Writer) doFinalize() error {
	// Store table offsets as we write hash tables
	var tableOffsets [256]uint64

	// Create hash tables and write them to the file
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := uint64(len(tableEntries) << 1)

		if tableSize == 0 {
			tableOffsets[i] = 0 // No table for this bucket
			continue
		}

		// Record where this table will be written
		tableOffsets[i] = uint64(cdb.bufferedOffset)

		// Create hash table
		hashTable := make([]entry, tableSize)
		for _, entry := range tableEntries {
			startingSlot := (uint64(entry.hash) >> 8) % tableSize
			slot := startingSlot

			for {
				if hashTable[slot].hash == 0 {
					hashTable[slot] = entry
					break
				}
				slot = (slot + 1) % tableSize
				if slot == startingSlot {
					return errors.New("hash table full")
				}
			}
		}

		// Write hash table
		for _, entry := range hashTable {
			err := writeTuple64(cdb.bufferedWriter, uint64(entry.hash), entry.offset)
			if err != nil {
				return err
			}
			cdb.bufferedOffset += 16
		}
	}

	// Flush the buffered writer before seeking
	err := cdb.bufferedWriter.Flush()
	if err != nil {
		return err
	}

	// Write index using actual table offsets
	buf := make([]byte, indexSize)
	for i := 0; i < 256; i++ {
		tableEntries := cdb.entries[i]
		tableSize := uint64(len(tableEntries) << 1)

		binary.LittleEndian.PutUint64(buf[i*16:i*16+8], tableOffsets[i])
		binary.LittleEndian.PutUint64(buf[i*16+8:i*16+16], tableSize)
	}

	// Seek to beginning and write index
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	_, err = cdb.writer.Write(buf)
	return err
}

func writeTuple64(w io.Writer, first, second uint64) error {
	tuple := make([]byte, 16)
	binary.LittleEndian.PutUint64(tuple[:8], first)
	binary.LittleEndian.PutUint64(tuple[8:], second)

	_, err := w.Write(tuple)
	return err
}
