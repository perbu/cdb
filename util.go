package cdb

import (
	"encoding/binary"
	"io"
)

// Unsigned is a constraint for unsigned integer types used in CDB
type Unsigned interface {
	~uint32 | ~uint64
}

func readTuple(r io.ReaderAt, offset uint32) (uint32, uint32, error) {
	tuple := make([]byte, 8)
	_, err := r.ReadAt(tuple, int64(offset))
	if err != nil {
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint32(tuple[:4])
	second := binary.LittleEndian.Uint32(tuple[4:])
	return first, second, nil
}

func writeTuple(w io.Writer, first, second uint32) error {
	tuple := make([]byte, 8)
	binary.LittleEndian.PutUint32(tuple[:4], first)
	binary.LittleEndian.PutUint32(tuple[4:], second)

	_, err := w.Write(tuple)
	return err
}

func readTuple64(r io.ReaderAt, offset uint64) (uint64, uint64, error) {
	tuple := make([]byte, 16)
	_, err := r.ReadAt(tuple, int64(offset))
	if err != nil {
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint64(tuple[:8])
	second := binary.LittleEndian.Uint64(tuple[8:])
	return first, second, nil
}

func writeTuple64(w io.Writer, first, second uint64) error {
	tuple := make([]byte, 16)
	binary.LittleEndian.PutUint64(tuple[:8], first)
	binary.LittleEndian.PutUint64(tuple[8:], second)

	_, err := w.Write(tuple)
	return err
}

// Generic tuple functions for both uint32 and uint64

func readTupleGeneric[T Unsigned](r io.ReaderAt, offset T) (T, T, error) {
	var size int
	switch any(*new(T)).(type) {
	case uint32:
		size = 8
	case uint64:
		size = 16
	}

	tuple := make([]byte, size)
	_, err := r.ReadAt(tuple, int64(offset))
	if err != nil {
		return 0, 0, err
	}

	var first, second T
	switch any(*new(T)).(type) {
	case uint32:
		first = T(binary.LittleEndian.Uint32(tuple[:4]))
		second = T(binary.LittleEndian.Uint32(tuple[4:]))
	case uint64:
		first = T(binary.LittleEndian.Uint64(tuple[:8]))
		second = T(binary.LittleEndian.Uint64(tuple[8:]))
	}
	return first, second, nil
}

func writeTupleGeneric[T Unsigned](w io.Writer, first, second T) error {
	var tuple []byte
	switch any(*new(T)).(type) {
	case uint32:
		tuple = make([]byte, 8)
		binary.LittleEndian.PutUint32(tuple[:4], uint32(first))
		binary.LittleEndian.PutUint32(tuple[4:], uint32(second))
	case uint64:
		tuple = make([]byte, 16)
		binary.LittleEndian.PutUint64(tuple[:8], uint64(first))
		binary.LittleEndian.PutUint64(tuple[8:], uint64(second))
	}

	_, err := w.Write(tuple)
	return err
}
