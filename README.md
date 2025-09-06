# CDB - 64-bit Constant Database

A native Go implementation of CDB,Constant Database, with 64-bit support and memory-mapped reading. Originates
from [github.com/colinmarc/cdb](https://github.com/colinmarc/cdb). Although there is little left of the original code,
the algorithm is the same.

Originally, CDB was described as:
> CDB is a fast, reliable, simple package for creating and reading constant databases. Its database structure provides
> several features:
> - **Fast lookups**: A successful lookup in a large database normally takes just two disk accesses. An unsuccessful
    lookup takes only one.
> - **Low overhead**: A database uses 4096 bytes for the index, plus 16 bytes per hash table entry, plus the space for
    keys and data.
> - **Large file support**: This 64-bit implementation can handle databases up to 8 exabytes (2^63 bytes). There are no
    other restrictions; records don't even have to fit into memory.
> - **Machine-independent format**: Databases are stored in a consistent binary format across platforms.

With mmap reads, a further improvement is gained. Care should be taken when using this on many large databases, as the
memory pressure will be different from what you might be used to.

## Features

- **64-bit only**: Simplified implementation supporting only 64-bit databases. These are only marginally larger than the
  32-bit equivalent and have no size restrictions.
- **Memory-mapped reads**: Zero-copy access using mmap for optimal read performance. Reduces allocations by 90%.
- **Native Go iterators**: Support for Go 1.23+ `range` syntax over keys, values, and key-value pairs
- **Buffered writes**: 64KB write buffer for efficient database creation

## Quick Start

```go
package main

import (
	"log"
	"github.com/perbu/cdb"
)

func main() {
	// Create a new database
	writer, err := cdb.Create("/tmp/example.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Write some key/value pairs
	writer.Put([]byte("Alice"), []byte("Practice"))
	writer.Put([]byte("Bob"), []byte("Hope"))
	writer.Put([]byte("Charlie"), []byte("Horse"))

	// Freeze the database and open for reads
	db, err := writer.Freeze()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Read a value
	value, err := db.Get([]byte("Alice"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(string(value)) // Output: Practice

	// Iterate over all key-value pairs (Go 1.23+)
	for key, value := range db.All() {
		log.Printf("%s: %s", key, value)
	}

	// Iterate over just keys
	for key := range db.Keys() {
		log.Printf("Key: %s", key)
	}

	// Iterate over just values  
	for value := range db.Values() {
		log.Printf("Value: %s", value)
	}
}
```

## File Format

This implementation uses a 64-bit CDB format:

- **Index**: 4096 bytes at file start (256 tables × 16 bytes each)
- **Data section**: Key-value pairs with 64-bit length prefixes (16 bytes per record header)
- **Hash tables**: Linear probing collision resolution with 64-bit offsets

## Performance

The performance goal was to get rid of the context switching and allocations that came with the original
CDB implementation. A read through a memory map will have no slowdown if the content is in the page cache already,
avoiding both the seek and read syscalls.

The most important metric for me is the time to iterate over the database. At below 2 ns, it is hard to see how it can
be faster. The benchmarks show a clear lead to Apple Silicon, likely because of lower memory latency.

```
goos: darwin
goarch: arm64
pkg: github.com/perbu/cdb
cpu: Apple M4
BenchmarkGet-10                         54848398                22.04 ns/op            0 B/op          0 allocs/op
BenchmarkMmapIteratorAll-10             628005786                1.896 ns/op           0 B/op          0 allocs/op
BenchmarkMmapIteratorKeys-10            708844383                1.678 ns/op           0 B/op          0 allocs/op
BenchmarkMmapIteratorValues-10          708912603                1.677 ns/op           0 B/op          0 allocs/op
BenchmarkPut-10                          1731052               715.6 ns/op           730 B/op          8 allocs/op
```

Performance on a 64-bit Linux machine is similar:

```
goos: linux
goarch: amd64
pkg: github.com/perbu/cdb
cpu: AMD Ryzen 7 9800X3D 8-Core Processor
BenchmarkGet-16                         45158083                26.57 ns/op            0 B/op          0 allocs/op
BenchmarkMmapIteratorAll-16             445780636                2.675 ns/op           0 B/op          0 allocs/op
BenchmarkMmapIteratorKeys-16            452996073                2.677 ns/op           0 B/op          0 allocs/op
BenchmarkMmapIteratorValues-16          451605426                2.650 ns/op           0 B/op          0 allocs/op
BenchmarkPut-16                          1572086               745.9 ns/op           734 B/op          8 allocs/op```
```
## API Reference

### Writing

```go
writer, err := cdb.Create(path) // Create new database file
writer, err := cdb.NewWriter(io.WriteSeeker) // Use custom WriteSeeker
err := writer.Put(key, value []byte) // Add key-value pair
db, err := writer.Freeze() // Finalize and return reader
err := writer.Close() // Finalize and close file
```

### Reading

```go
db, err := cdb.OpenMmap(path) // Open with memory mapping
db, err := cdb.NewMmap(*os.File) // Create from open file  
value, err := db.Get(key []byte) // Lookup value
size := db.Size() // Get file size
err := db.Close() // Close and unmap
```

### Iteration (Go 1.23+)

```go
for key, value := range db.All() { }      // Iterate key-value pairs
for key := range db.Keys() { }            // Iterate keys only
for value := range db.Values() { } // Iterate values only
```

---

Based on the original [CDB specification](http://cr.yp.to/cdb.html) by D. J. Bernstein.