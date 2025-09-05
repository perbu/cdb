CDB
===

[![GoDoc](https://godoc.org/github.com/colinmarc/cdb/web?status.svg)](https://godoc.org/github.com/colinmarc/cdb) [![build](https://travis-ci.org/colinmarc/cdb.svg?branch=master)](https://travis-ci.org/colinmarc/hdfs)

This is a native Go implementation of [cdb][1], a constant key/value database
with some very nice properties. From the [design doc][1]:

> cdb is a fast, reliable, simple package for creating and reading constant databases. Its database structure provides several features:
> - Fast lookups: A successful lookup in a large database normally takes just two disk accesses. An unsuccessful lookup takes only one.
> - Low overhead: A database uses 2048 bytes, plus 24 bytes per record, plus the space for keys and data.
> - No random limits: cdb can handle any database up to 4 gigabytes. There are no other restrictions; records don't even have to fit into memory. Databases are stored in a machine-independent format.

[1]: http://cr.yp.to/cdb.html

64-bit support
--------------

This package supports both classic 32-bit cdb and a 64-bit variant suitable for databases larger than 4GB. Use the `*64` types to create and read 64-bit databases:

```go
// Create a 64-bit database (offsets use uint64)
writer, err := cdb.Create64("/tmp/example64.cdb")
if err != nil {
    log.Fatal(err)
}
writer.Put([]byte("Alice"), []byte("Practice"))

// Freeze and open for reads
db, err := writer.Freeze()
if err != nil {
    log.Fatal(err)
}

v, err := db.Get([]byte("Alice"))
if err != nil {
    log.Fatal(err)
}
log.Println(string(v))

// Iterate (64-bit)
iter := db.Iter()
for iter.Next() {
    _ = iter.Key()
    _ = iter.Value()
}
if err := iter.Err(); err != nil {
    log.Fatal(err)
}
```

For standard (up to ~4GB) databases, continue to use the 32-bit API shown below.

Usage (32-bit)
-----

```go
writer, err := cdb.Create("/tmp/example.cdb")
if err != nil {
  log.Fatal(err)
}

// Write some key/value pairs to the database.
writer.Put([]byte("Alice"), []byte("Practice"))
writer.Put([]byte("Bob"), []byte("Hope"))
writer.Put([]byte("Charlie"), []byte("Horse"))

// Freeze the database, and open it for reads.
db, err := writer.Freeze()
if err != nil {
  log.Fatal(err)
}

// Fetch a value.
v, err := db.Get([]byte("Alice"))
if err != nil {
  log.Fatal(err)
}

log.Println(string(v))
// => Practice

// Iterate over the database
iter := db.Iter()
for iter.Next() {
    log.Printf("The key %s has a value of length %d\n", string(iter.Key()), len(iter.Value()))
}

if err := iter.Err(); err != nil {
    log.Fatal(err)
}
```

Generics
--------

This repository includes notes and a proposed direction for reducing duplication between the 32-bit and 64-bit implementations using Go generics (Go 1.18+). See `generics.md` for a conceptual design. The public API remains source-compatible, with potential type aliases like `type CDB32 = CDB[uint32]` and `type CDB64 = CDB[uint64]` if/when the refactor is adopted.

Compatibility
-------------

- 32-bit API (`Create`, `Open`, `Iter`, etc.) remains unchanged.
- 64-bit support is available via `Create64`/`Open64`. Method names (e.g., `Get`, `Iter`) are the same; concrete types differ (`CDB64`, `Iterator64`).
- Go 1.18+ is recommended to explore the generics-based refactor notes; current code builds with standard supported Go versions.
