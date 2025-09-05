package cdb

const start uint32 = 5381

// cdbHash returns a 32-bit hash, all through the offsets are 64b, a 32b hash should be fine for most use cases.
func cdbHash(data []byte) uint32 {
	v := start
	for _, b := range data {
		v = ((v << 5) + v) ^ uint32(b)
	}

	return v
}
