package cdb_test

import (
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/colinmarc/cdb"
)

func TestIterator(t *testing.T) {
	db, err := cdb.Open("./test/test.cdb")
	requireNoError(t, err)
	requireNotNil(t, db)

	n := 0
	iter := db.Iter()
	for iter.Next() {
		assertEqual(t, string(expectedRecords[n][0]), string(iter.Key()))
		assertEqual(t, string(expectedRecords[n][1]), string(iter.Value()))
		requireNoError(t, iter.Err())
		n++
	}

	assertEqual(t, len(expectedRecords)-1, n)

	requireNoError(t, iter.Err())
}

func BenchmarkIterator(b *testing.B) {
	db, _ := cdb.Open("./test/test.cdb")
	iter := db.Iter()
	b.ResetTimer()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		for iter.Next() {
		}
	}
}

// BenchmarkIterator64 benchmarks the 64-bit iterator using a dynamically created database
func BenchmarkIterator64(b *testing.B) {
	// Create a test database
	f, err := os.CreateTemp("", "bench-iterator64")
	requireNoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter64(f, nil)
	requireNoError(b, err)

	// Add test data
	for i := 0; i < 1000; i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte("test_value_" + strconv.Itoa(i))
		writer.Put(key, value)
	}

	db, err := writer.Freeze()
	requireNoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := db.Iter()
		for iter.Next() {
			// Access the data to ensure it's read
			_ = iter.Key()
			_ = iter.Value()
		}
	}
}

func ExampleIterator() {
	db, err := cdb.Open("./test/test.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Create an iterator for the database.
	iter := db.Iter()
	for iter.Next() {
		// Do something with iter.Key()/iter.Value()
	}

	if err := iter.Err(); err != nil {
		log.Fatal(err)
	}
}
