package cdb_test

import (
	"os"
	"testing"

	"github.com/colinmarc/cdb"
)

func TestMmapCDB(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-mmap64")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	requireNoError(t, err)

	// Write test data
	testData := map[string]string{
		"foo":       "bar",
		"baz":       "quuuux",
		"empty":     "",
		"":          "empty_key",
		"collision": "test",
	}

	for key, value := range testData {
		err := writer.Put([]byte(key), []byte(value))
		requireNoError(t, err)
	}

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test memory-mapped reading
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)
	defer db.Close()

	// Verify all data can be read correctly
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		requireNoError(t, err, "Failed to get key: %s", key)
		assertEqual(t, expectedValue, string(value), "Key: %s", key)
	}

	// Test non-existent key
	value, err := db.Get([]byte("nonexistent"))
	requireNoError(t, err)
	requireNil(t, value)

	// Test size method
	assertTrue(t, db.Size() > 0)
}

func TestMmapWithCustomHash(t *testing.T) {
	// Test with custom hash function
	customHash := func(data []byte) uint32 {
		// Simple custom hash for testing
		var hash uint32
		for _, b := range data {
			hash = hash*31 + uint32(b)
		}
		return hash
	}

	// Create database with custom hash
	f, err := os.CreateTemp("", "test-mmap-hash")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, customHash)
	requireNoError(t, err)

	err = writer.Put([]byte("test"), []byte("value"))
	requireNoError(t, err)

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Read with same custom hash
	db, err := cdb.OpenMmapWithHash(f.Name(), customHash)
	requireNoError(t, err)
	defer db.Close()

	value, err := db.Get([]byte("test"))
	requireNoError(t, err)
	assertEqual(t, "value", string(value))
}

func TestMmapErrorHandling(t *testing.T) {
	// Test opening non-existent file
	db, err := cdb.OpenMmap("nonexistent.cdb")
	assertError(t, err)
	requireNil(t, db)

	// Test with empty file (too small)
	f, err := os.CreateTemp("", "test-empty")
	requireNoError(t, err)
	defer os.Remove(f.Name())
	f.Close()

	db, err = cdb.OpenMmap(f.Name())
	assertError(t, err)
	requireNil(t, db)
}

func TestMmapClose(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-close")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	requireNoError(t, err)

	err = writer.Put([]byte("test"), []byte("value"))
	requireNoError(t, err)

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test that Close() can be called multiple times
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)

	err = db.Close()
	requireNoError(t, err)

	err = db.Close() // Should not panic
	requireNoError(t, err)
}
