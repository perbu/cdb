package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/colinmarc/cdb"
)

// createLargeCDBFileColinmarc creates a CDB file using colinmarc/cdb
func createLargeCDBFileColinmarc(filename string, numEntries int) error {
	writer, err := cdb.Create(filename)
	if err != nil {
		return err
	}
	defer writer.Close()

	// Generate predictable test data
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key_%08d", i)
		value := fmt.Sprintf("value_%08d_data_payload", i)
		err := writer.Put([]byte(key), []byte(value))
		if err != nil {
			return err
		}
	}

	return nil
}

const benchmarkEntries = 100000

func BenchmarkColinmarcIterator(b *testing.B) {
	// Create test file
	filename := "/tmp/benchmark_colinmarc.cdb"
	err := createLargeCDBFileColinmarc(filename, benchmarkEntries)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(filename)

	// Open the database
	db, err := cdb.Open(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Measure the performance of individual iteration steps
	iterations := 0
	for i := 0; i < b.N; i++ {
		// Create iterator
		iter := db.Iter()
		for iter.Next() {
			// Access the key and value to ensure they're actually read
			key := iter.Key()
			value := iter.Value()
			_ = key[0]   // Force access to key data
			_ = value[0] // Force access to value data
			iterations++
			if iterations >= b.N {
				return
			}
		}
		if err := iter.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColinmarcGet(b *testing.B) {
	// Create test file
	filename := "/tmp/benchmark_colinmarc_get.cdb"
	err := createLargeCDBFileColinmarc(filename, benchmarkEntries)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(filename)

	// Open the database
	db, err := cdb.Open(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Test random key lookups
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%08d", i%benchmarkEntries)
		_, err := db.Get([]byte(key))
		if err != nil {
			b.Fatal(err)
		}
	}
}
