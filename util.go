package cdb

import (
	"encoding/binary"
	"io"
)

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
