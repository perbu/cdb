package cdb_test

import (
	"bytes"
	"math/rand"
	"os"
	"testing"

	"github.com/perbu/cdb"
)

var expectedRecords = [][][]byte{
	{[]byte("key"), []byte("value")},
	{[]byte("alpha"), []byte("first")},
	{[]byte("beta"), []byte("second")},
	{[]byte("gamma"), []byte("third")},
	{[]byte("counter:1"), []byte("1")},
	{[]byte("counter:2"), []byte("2")},
	{[]byte("empty"), []byte("")},
	{[]byte("binary"), []byte("\x00\x01\x02\xff\xfe")},
	{[]byte("newline"), []byte("line1\nline2\n")},
	{[]byte("json"), []byte("{\"ok\":true,\"n\":42}")},
	{[]byte("path:/var/log/syslog"), []byte("/var/log/syslog")},
	{[]byte("user:1001"), []byte("per")},
	{[]byte("user:1002"), []byte("anna")},
	{[]byte("duplicate"), []byte("v1")},
	{[]byte("null-in-key:\x00suffix"), []byte("works")},
	{[]byte("long:value"), []byte(string(bytes.Repeat([]byte("A"), 1024)))}, // 1 KiB of 'A'
	{[]byte("kv:small"), []byte("s")},
	{[]byte("kv:medium"), []byte(string(bytes.Repeat([]byte("m"), 128)))}, // 128 'm' characters
	{[]byte("utf8:key"), []byte("norsk: ø æ å")},
	{[]byte("not in the table"), nil},
}

const testFile = "./test/test.cdb64"

func TestGet(t *testing.T) {
	db, err := cdb.Open(testFile)
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}
	defer db.Close()
	// The last record is the test for a key that doesn't exist.
	missRecord := expectedRecords[len(expectedRecords)-1]
	records := expectedRecords[:len(expectedRecords)-1]

	// Duplicate and shuffle the records that should exist
	records = append(append(records, records...), records...)
	shuffle(records)

	for _, record := range records {
		msg := "while fetching " + string(record[0])
		value, err := db.Get(record[0])
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
		if string(record[1]) != string(value) {
			t.Errorf("%s: expected %q, got %q", msg, string(record[1]), string(value))
		}
	}

	// Separately test the key that should not be found
	value, err := db.Get(missRecord[0])
	if err != nil {
		t.Fatalf("for missing key: %v", err)
	}
	if value != nil {
		t.Errorf("for missing key: expected nil, got %v", value)
	}
}

func TestClosesFile(t *testing.T) {
	f, err := os.Open(testFile)
	if err != nil {
		t.Fatal(err)
	}

	db, err := cdb.Mmap(f)
	if err != nil {
		t.Fatal(err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = f.Close()
	if err == nil {
		t.Error("expected error when closing already-closed file")
	}
}

func BenchmarkGet(b *testing.B) {
	// Create a temporary CDB file for benchmarking
	writer, err := cdb.Create("/tmp/benchmark.cdb")
	if err != nil {
		b.Fatal(err)
	}

	// Add the expected records to the database
	for _, record := range expectedRecords[:len(expectedRecords)-1] { // Skip the "not in table" entry
		if record[1] != nil { // Skip nil values
			writer.Put(record[0], record[1])
		}
	}

	db, err := writer.Freeze()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Only use records that actually exist in the database
		record := expectedRecords[rand.Intn(len(expectedRecords)-1)]
		if record[1] != nil {
			db.Get(record[0])
		}
	}
}
func shuffle(a [][][]byte) {
	for i := range a {
		j := rand.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

func TestWriter64(t *testing.T) {
	// Test writing and reading a 64-bit CDB file with Go implementation
	writer, err := cdb.Create("test_go_64bit.cdb")
	if err != nil {
		t.Fatal(err)
	}
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
		if err != nil {
			t.Fatalf("Failed to put key: %s: %v", key, err)
		}
	}

	// Freeze and get reader
	db, err := writer.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Read back and verify
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key: %s: %v", key, err)
		}
		if expected := []byte(expectedValue); string(expected) != string(value) {
			t.Errorf("Key: %s: expected %q, got %q", key, expectedValue, string(value))
		}
	}

	// Test non-existent key
	value, err := db.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Errorf("expected nil value for nonexistent key, got: %v", value)
	}
}
