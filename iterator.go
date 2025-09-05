package cdb

// Iterator represents a sequential iterator over a CDB database.
type Iterator struct {
	db     *CDB
	pos    uint32
	endPos uint32
	err    error
	key    []byte
	value  []byte
}

// Iter creates an Iterator that can be used to iterate the database.
func (cdb *CDB) Iter() *Iterator {
	return &Iterator{
		db:     cdb,
		pos:    uint32(indexSize),
		endPos: cdb.index[0].offset,
	}
}

// Next reads the next key/value pair and advances the iterator one record.
// It returns false when the scan stops, either by reaching the end of the
// database or an error. After Next returns false, the Err method will return
// any error that occurred while iterating.
func (iter *Iterator) Next() bool {
	if iter.pos >= iter.endPos {
		return false
	}

	keyLength, valueLength, err := readTuple(iter.db.reader, iter.pos)
	if err != nil {
		iter.err = err
		return false
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = iter.db.reader.ReadAt(buf, int64(iter.pos+8))
	if err != nil {
		iter.err = err
		return false
	}

	// Update iterator state
	iter.key = buf[:keyLength]
	iter.value = buf[keyLength:]
	iter.pos += 8 + keyLength + valueLength

	return true
}

// Key returns the current key.
func (iter *Iterator) Key() []byte {
	return iter.key
}

// Value returns the current value.
func (iter *Iterator) Value() []byte {
	return iter.value
}

// Err returns the current error.
func (iter *Iterator) Err() error {
	return iter.err
}

// Iterator64 represents a sequential iterator over a 64-bit CDB database.
type Iterator64 struct {
	db     *CDB64
	pos    uint64
	endPos uint64
	err    error
	key    []byte
	value  []byte
}

// Iter creates an Iterator64 that can be used to iterate the database.
func (cdb *CDB64) Iter() *Iterator64 {
	return &Iterator64{
		db:     cdb,
		pos:    uint64(indexSize64),
		endPos: cdb.index[0].offset,
	}
}

// Next reads the next key/value pair and advances the iterator one record.
// It returns false when the scan stops, either by reaching the end of the
// database or an error. After Next returns false, the Err method will return
// any error that occurred while iterating.
func (iter *Iterator64) Next() bool {
	if iter.pos >= iter.endPos {
		return false
	}

	keyLength, valueLength, err := readTuple64(iter.db.reader, iter.pos)
	if err != nil {
		iter.err = err
		return false
	}

	buf := make([]byte, keyLength+valueLength)
	_, err = iter.db.reader.ReadAt(buf, int64(iter.pos+16))
	if err != nil {
		iter.err = err
		return false
	}

	// Update iterator state
	iter.key = buf[:keyLength]
	iter.value = buf[keyLength:]
	iter.pos += 16 + keyLength + valueLength

	return true
}

// Key returns the current key.
func (iter *Iterator64) Key() []byte {
	return iter.key
}

// Value returns the current value.
func (iter *Iterator64) Value() []byte {
	return iter.value
}

// Err returns the current error.
func (iter *Iterator64) Err() error {
	return iter.err
}

// IteratorGeneric represents a sequential iterator over a generic CDB database.
type IteratorGeneric[T Unsigned] struct {
	db     *CDBGeneric[T]
	pos    T
	endPos T
	err    error
	key    []byte
	value  []byte
}

// Iter creates an IteratorGeneric that can be used to iterate the database.
func (cdb *CDBGeneric[T]) Iter() *IteratorGeneric[T] {
	var startPos T
	switch any(*new(T)).(type) {
	case uint32:
		startPos = T(indexSize)
	case uint64:
		startPos = T(indexSize64)
	}

	return &IteratorGeneric[T]{
		db:     cdb,
		pos:    startPos,
		endPos: cdb.index[0].offset,
	}
}

// Next reads the next key/value pair and advances the iterator one record.
// It returns false when the scan stops, either by reaching the end of the
// database or an error. After Next returns false, the Err method will return
// any error that occurred while iterating.
func (iter *IteratorGeneric[T]) Next() bool {
	if iter.pos >= iter.endPos {
		return false
	}

	keyLength, valueLength, err := readTupleGeneric[T](iter.db.reader, iter.pos)
	if err != nil {
		iter.err = err
		return false
	}

	buf := make([]byte, keyLength+valueLength)

	var headerSize T
	switch any(*new(T)).(type) {
	case uint32:
		headerSize = 8
	case uint64:
		headerSize = 16
	}

	_, err = iter.db.reader.ReadAt(buf, int64(iter.pos+headerSize))
	if err != nil {
		iter.err = err
		return false
	}

	// Update iterator state
	iter.key = buf[:keyLength]
	iter.value = buf[keyLength:]
	iter.pos += headerSize + keyLength + valueLength

	return true
}

// Key returns the current key.
func (iter *IteratorGeneric[T]) Key() []byte {
	return iter.key
}

// Value returns the current value.
func (iter *IteratorGeneric[T]) Value() []byte {
	return iter.value
}

// Err returns the current error.
func (iter *IteratorGeneric[T]) Err() error {
	return iter.err
}

// Backward compatibility type aliases
type Iterator32 = IteratorGeneric[uint32]
type Iterator64Alt = IteratorGeneric[uint64]
