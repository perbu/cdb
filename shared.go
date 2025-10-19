package cdb

import (
	"bytes"
	"encoding/binary"
	"iter"
)

// readTuple reads two 64-bit values from data at the given offset.
func readTuple(data []byte, offset uint64) (uint64, uint64) {
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

	keyLength, valueLength := readTuple(data, offset)

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

// dataEndPos finds the end of the data section by finding the minimum table offset.
func dataEndPos(data []byte) uint64 {
	endPos := uint64(len(data))
	for i := 0; i < 256; i++ {
		table := readTableAt(data, uint8(i))
		if table.length > 0 && table.offset < endPos {
			endPos = table.offset
		}
	}
	if endPos == uint64(len(data)) {
		// empty DB -> data section is exactly indexSize
		return uint64(indexSize)
	}
	return endPos
}

// getFromBytes implements Get logic over a raw byte slice.
func getFromBytes(data []byte, key []byte) ([]byte, error) {
	hash := cdbHash(key)

	table := readTableAt(data, uint8(hash&0xff))
	if table.length == 0 {
		return nil, nil
	}

	// Probe the given hash table, starting at the given slot.
	startingSlot := (uint64(hash) >> 8) % table.length
	slot := startingSlot

	for {
		slotOffset := table.offset + (16 * slot)
		slotHash, offset := readTuple(data, slotOffset)

		// An empty slot means the key doesn't exist.
		if slotHash == 0 {
			break
		} else if slotHash == uint64(hash) {
			value := getValueAt(data, offset, key)
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

// allFromBytes returns an iterator over all key-value pairs in the data.
func allFromBytes(data []byte) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		pos := uint64(indexSize)
		end := dataEndPos(data)

		for pos < end {
			if int(pos)+16 > len(data) {
				return
			}

			keyLength, valueLength := readTuple(data, pos)

			// Calculate total record size and check bounds
			totalSize := 16 + keyLength + valueLength
			if int(pos+totalSize) > len(data) {
				return
			}

			// Extract key and value directly from data
			dataStart := int(pos + 16)
			keyEnd := dataStart + int(keyLength)
			valueEnd := keyEnd + int(valueLength)

			key := data[dataStart:keyEnd]
			value := data[keyEnd:valueEnd]

			// Yield the key-value pair
			if !yield(key, value) {
				return // Early termination requested
			}

			pos += totalSize
		}
	}
}
