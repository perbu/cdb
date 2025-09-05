# CDB - 64-bit Constant Database

A native Go implementation of CDB (Constant Database) with 64-bit support and memory-mapped reading.

> CDB is a fast, reliable, simple package for creating and reading constant databases. Its database structure provides several features:
> - **Fast lookups**: A successful lookup in a large database normally takes just two disk accesses. An unsuccessful lookup takes only one.
> - **Low overhead**: A database uses 4096 bytes for the index, plus 16 bytes per hash table entry, plus the space for keys and data.
> - **Large file support**: This 64-bit implementation can handle databases up to 8 exabytes (2^63 bytes). There are no other restrictions; records don't even have to fit into memory.
> - **Machine-independent format**: Databases are stored in a consistent binary format across platforms.

## Features

- **64-bit only**: Simplified implementation supporting only 64-bit databases for better performance
- **Memory-mapped reads**: Zero-copy access using mmap for optimal read performance  
- **Native Go iterators**: Support for Go 1.23+ `range` syntax over keys, values, and key-value pairs
- **Buffered writes**: 64KB write buffer for efficient database creation
- **Custom hash support**: Pluggable hash functions (must be consistent between write/read)

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

Benchmarks on Apple M4:
- **Get**: ~21 ns/op (memory-mapped lookups)
- **Put**: ~670 ns/op (buffered writes)

## API Reference

### Writing

```go
writer, err := cdb.Create(path)           // Create new database file
writer, err := cdb.NewWriter(io.WriteSeeker) // Use custom WriteSeeker
err := writer.Put(key, value []byte)      // Add key-value pair
db, err := writer.Freeze()                // Finalize and return reader
err := writer.Close()                     // Finalize and close file
```

### Reading

```go
db, err := cdb.OpenMmap(path)             // Open with memory mapping
db, err := cdb.NewMmap(*os.File)          // Create from open file  
value, err := db.Get(key []byte)          // Lookup value
size := db.Size()                         // Get file size
err := db.Close()                         // Close and unmap
```

### Iteration (Go 1.23+)

```go
for key, value := range db.All() { }      // Iterate key-value pairs
for key := range db.Keys() { }            // Iterate keys only
for value := range db.Values() { }        // Iterate values only
```

---

Based on the original [CDB specification](http://cr.yp.to/cdb.html) by D. J. Bernstein.