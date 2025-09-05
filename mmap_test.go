package cdb_test

import (
	"os"
	"testing"

	"github.com/colinmarc/cdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMmapCDB(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-mmap")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	require.NoError(t, err)

	// Write test data
	testData := map[string]string{
		"foo":       "bar",
		"baz":       "quuuux",
		"empty":     "",
		"":          "empty_key",
		"collision": "test", // This may collide with other keys
	}

	for key, value := range testData {
		err := writer.Put([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	_, err = writer.Freeze()
	require.NoError(t, err)
	f.Close()

	// Test memory-mapped reading
	db, err := cdb.OpenMmap(f.Name())
	require.NoError(t, err)
	defer db.Close()

	// Verify all data can be read correctly
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		require.NoError(t, err, "Failed to get key: %s", key)
		assert.Equal(t, expectedValue, string(value), "Key: %s", key)
	}

	// Test non-existent key
	value, err := db.Get([]byte("nonexistent"))
	require.NoError(t, err)
	assert.Nil(t, value)

	// Test size method
	assert.True(t, db.Size() > 0)
}

func TestMmapCDB64(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-mmap64")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter64(f, nil)
	require.NoError(t, err)

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
		require.NoError(t, err)
	}

	_, err = writer.Freeze()
	require.NoError(t, err)
	f.Close()

	// Test memory-mapped reading
	db, err := cdb.OpenMmap64(f.Name())
	require.NoError(t, err)
	defer db.Close()

	// Verify all data can be read correctly
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		require.NoError(t, err, "Failed to get key: %s", key)
		assert.Equal(t, expectedValue, string(value), "Key: %s", key)
	}

	// Test non-existent key
	value, err := db.Get([]byte("nonexistent"))
	require.NoError(t, err)
	assert.Nil(t, value)

	// Test size method
	assert.True(t, db.Size() > 0)
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
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, customHash)
	require.NoError(t, err)

	err = writer.Put([]byte("test"), []byte("value"))
	require.NoError(t, err)

	_, err = writer.Freeze()
	require.NoError(t, err)
	f.Close()

	// Read with same custom hash
	db, err := cdb.OpenMmapWithHash(f.Name(), customHash)
	require.NoError(t, err)
	defer db.Close()

	value, err := db.Get([]byte("test"))
	require.NoError(t, err)
	assert.Equal(t, "value", string(value))
}

func TestMmapErrorHandling(t *testing.T) {
	// Test opening non-existent file
	db, err := cdb.OpenMmap("nonexistent.cdb")
	assert.Error(t, err)
	assert.Nil(t, db)

	// Test with empty file (too small)
	f, err := os.CreateTemp("", "test-empty")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	f.Close()

	db, err = cdb.OpenMmap(f.Name())
	assert.Error(t, err)
	assert.Nil(t, db)
}

func TestMmapClose(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-close")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	require.NoError(t, err)

	err = writer.Put([]byte("test"), []byte("value"))
	require.NoError(t, err)

	_, err = writer.Freeze()
	require.NoError(t, err)
	f.Close()

	// Test that Close() can be called multiple times
	db, err := cdb.OpenMmap(f.Name())
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	err = db.Close() // Should not panic
	require.NoError(t, err)
}

// Test compatibility between regular CDB and memory-mapped CDB
func TestCompatibility_Regular_vs_Mmap(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-compat")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	require.NoError(t, err)

	// Write the same test data used in regular tests
	for _, record := range expectedRecords[:len(expectedRecords)-1] { // Skip the "not in the table" record
		if record[1] != nil { // Skip records with nil values
			err := writer.Put(record[0], record[1])
			require.NoError(t, err)
		}
	}

	_, err = writer.Freeze()
	require.NoError(t, err)
	f.Close()

	// Open with both regular and mmap readers
	regularDB, err := cdb.Open(f.Name())
	require.NoError(t, err)
	defer regularDB.Close()

	mmapDB, err := cdb.OpenMmap(f.Name())
	require.NoError(t, err)
	defer mmapDB.Close()

	// Test that both return the same results
	for _, record := range expectedRecords[:len(expectedRecords)-1] {
		if record[1] != nil {
			regularValue, err1 := regularDB.Get(record[0])
			mmapValue, err2 := mmapDB.Get(record[0])

			require.NoError(t, err1)
			require.NoError(t, err2)
			assert.Equal(t, regularValue, mmapValue, "Values differ for key: %s", string(record[0]))
		}
	}

	// Test non-existent key
	regularValue, err1 := regularDB.Get([]byte("definitely_not_there"))
	mmapValue, err2 := mmapDB.Get([]byte("definitely_not_there"))

	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, regularValue, mmapValue)
	assert.Nil(t, regularValue)
	assert.Nil(t, mmapValue)
}
