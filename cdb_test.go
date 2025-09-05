package cdb_test

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/colinmarc/cdb"
)

var expectedRecords = [][][]byte{
	{[]byte("foo"), []byte("bar")},
	{[]byte("baz"), []byte("quuuux")},
	{[]byte("playwright"), []byte("wow")},
	{[]byte("crystal"), []byte("CASTLES")},
	{[]byte("CRYSTAL"), []byte("castles")},
	{[]byte("snush"), []byte("collision!")}, // 'playwright' collides with 'snush' in cdbhash
	{[]byte("a"), []byte("a")},
	{[]byte("empty_value"), []byte("")},
	{[]byte(""), []byte("empty_key")},
	{[]byte("not in the table"), nil},
}

func TestGet(t *testing.T) {
	db, err := cdb.OpenMmap("./test/test.cdb")
	requireNoError(t, err)
	requireNotNil(t, db)

	records := append(append(expectedRecords, expectedRecords...), expectedRecords...)
	shuffle(records)

	for _, record := range records {
		msg := "while fetching " + string(record[0])

		value, err := db.Get(record[0])
		requireNoError(t, err, msg)
		assertEqual(t, string(record[1]), string(value), msg)
	}
}

func TestClosesFile(t *testing.T) {
	f, err := os.Open("./test/test.cdb")
	requireNoError(t, err)

	db, err := cdb.NewMmap(f, nil)
	requireNoError(t, err)
	requireNotNil(t, db)

	err = db.Close()
	requireNoError(t, err)

	err = f.Close()
	assertError(t, err)
}

func BenchmarkGet(b *testing.B) {
	db, _ := cdb.OpenMmap("./test/test.cdb")
	b.ResetTimer()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		record := expectedRecords[rand.Intn(len(expectedRecords))]
		db.Get(record[0])
	}
}

func Example() {
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

	fmt.Println(string(v))
	// Output: Practice
}

func ExampleMmapCDB() {
	db, err := cdb.OpenMmap("./test/test.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Fetch a value.
	v, err := db.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(v))
	// Output: bar
}

func shuffle(a [][][]byte) {
	rand.Seed(time.Now().UnixNano())
	for i := range a {
		j := rand.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

func TestCDB64WithPythonFile(t *testing.T) {
	// Test reading a 64-bit CDB file created by python-pure-cdb
	db, err := cdb.OpenMmap("test_64bit.cdb")
	if err != nil {
		t.Skip("Skipping CDB64 test - test_64bit.cdb not found:", err)
		return
	}
	defer db.Close()

	// Test known key-value pairs from our Python test script
	testCases := map[string]string{
		"Alice":   "Practice",
		"Bob":     "Hope",
		"Charlie": "Horse",
		"foo":     "bar",
		"baz":     "quuuux",
	}

	for key, expectedValue := range testCases {
		value, err := db.Get([]byte(key))
		requireNoError(t, err, "Key: %s", key)
		assertEqual(t, []byte(expectedValue), value, "Key: %s", key)
	}

	// Test non-existent key
	value, err := db.Get([]byte("nonexistent"))
	requireNoError(t, err)
	requireNil(t, value)

}

func TestWriter64(t *testing.T) {
	// Test writing and reading a 64-bit CDB file with Go implementation
	writer, err := cdb.Create("test_go_64bit.cdb")
	requireNoError(t, err)
	defer os.Remove("test_go_64bit.cdb")

	// Write test data
	testData := map[string]string{
		"Alice":   "Practice",
		"Bob":     "Hope",
		"Charlie": "Horse",
		"foo":     "bar",
		"baz":     "quuuux",
		"empty":   "",
		"":        "empty_key",
	}

	for key, value := range testData {
		err := writer.Put([]byte(key), []byte(value))
		requireNoError(t, err, "Failed to put key: %s", key)
	}

	// Freeze and get reader
	db, err := writer.Freeze()
	requireNoError(t, err)
	defer db.Close()

	// Read back and verify
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		requireNoError(t, err, "Failed to get key: %s", key)
		assertEqual(t, []byte(expectedValue), value, "Key: %s", key)
	}

	// Test non-existent key
	value, err := db.Get([]byte("nonexistent"))
	requireNoError(t, err)
	requireNil(t, value)

}
