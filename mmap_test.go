package cdb_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/perbu/cdb"
)

func TestMmapCDB(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-mmap64")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
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

	writer, err := cdb.NewWriter(f)
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

func TestMmapIterator(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-iterator")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	requireNoError(t, err)

	// Write test data in a predictable order
	testData := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
		{"", "empty_key"},
		{"empty_value", ""},
	}

	for _, item := range testData {
		err := writer.Put([]byte(item.key), []byte(item.value))
		requireNoError(t, err)
	}

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test iterator
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)
	defer db.Close()

	// Collect all items from iterator using native iterator pattern
	var items []struct {
		key   string
		value string
	}

	for key, value := range db.All() {
		// Make copies since the slices point into mmap'd memory
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		items = append(items, struct {
			key   string
			value string
		}{string(keyCopy), string(valueCopy)})
	}

	// Verify we got all items (order might be different due to hashing)
	assertEqual(t, len(testData), len(items))

	// Create maps for easier comparison
	expectedMap := make(map[string]string)
	for _, item := range testData {
		expectedMap[item.key] = item.value
	}

	actualMap := make(map[string]string)
	for _, item := range items {
		actualMap[item.key] = item.value
	}

	// Verify all expected items are present
	for key, expectedValue := range expectedMap {
		actualValue, exists := actualMap[key]
		assertTrue(t, exists, "Key not found in iterator results: %s", key)
		assertEqual(t, expectedValue, actualValue, "Value mismatch for key: %s", key)
	}
}

func TestMmapIteratorEmpty(t *testing.T) {
	// Create an empty database
	f, err := os.CreateTemp("", "test-empty-iterator")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	requireNoError(t, err)

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test iterator on empty database
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)
	defer db.Close()

	// Count items using native iterator
	count := 0
	for range db.All() {
		count++
	}

	assertEqual(t, 0, count, "Empty database should not have any items")
}

func TestMmapIteratorKeys(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-keys-iterator")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	requireNoError(t, err)

	// Write test data
	testData := map[string]string{
		"alpha": "value1",
		"beta":  "value2",
		"gamma": "value3",
	}

	for key, value := range testData {
		err := writer.Put([]byte(key), []byte(value))
		requireNoError(t, err)
	}

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test Keys() iterator
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)
	defer db.Close()

	// Collect all keys
	var keys []string
	for key := range db.Keys() {
		// Make copy since slice points into mmap'd memory
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keys = append(keys, string(keyCopy))
	}

	// Verify we got all keys
	assertEqual(t, len(testData), len(keys))

	// Convert to set for comparison
	keySet := make(map[string]bool)
	for _, key := range keys {
		keySet[key] = true
	}

	// Verify all expected keys are present
	for expectedKey := range testData {
		assertTrue(t, keySet[expectedKey], "Key not found in Keys() results: %s", expectedKey)
	}
}

func TestMmapIteratorValues(t *testing.T) {
	// Create a test database
	f, err := os.CreateTemp("", "test-values-iterator")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	requireNoError(t, err)

	// Write test data with unique values
	testData := map[string]string{
		"key1": "alpha",
		"key2": "beta",
		"key3": "gamma",
	}

	for key, value := range testData {
		err := writer.Put([]byte(key), []byte(value))
		requireNoError(t, err)
	}

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test Values() iterator
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)
	defer db.Close()

	// Collect all values
	var values []string
	for value := range db.Values() {
		// Make copy since slice points into mmap'd memory
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		values = append(values, string(valueCopy))
	}

	// Verify we got all values
	assertEqual(t, len(testData), len(values))

	// Convert to set for comparison
	valueSet := make(map[string]bool)
	for _, value := range values {
		valueSet[value] = true
	}

	// Verify all expected values are present
	for _, expectedValue := range testData {
		assertTrue(t, valueSet[expectedValue], "Value not found in Values() results: %s", expectedValue)
	}
}

func TestMmapIteratorEarlyTermination(t *testing.T) {
	// Create a test database with many items
	f, err := os.CreateTemp("", "test-early-termination")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	requireNoError(t, err)

	// Write 10 items
	for i := 0; i < 10; i++ {
		key := []byte("key" + string(rune('0'+i)))
		value := []byte("value" + string(rune('0'+i)))
		err := writer.Put(key, value)
		requireNoError(t, err)
	}

	_, err = writer.Freeze()
	requireNoError(t, err)
	f.Close()

	// Test early termination
	db, err := cdb.OpenMmap(f.Name())
	requireNoError(t, err)
	defer db.Close()

	// Only iterate through first 3 items
	count := 0
	for key, value := range db.All() {
		count++
		if count >= 3 {
			// Test that we can access key and value at termination point
			assertTrue(t, len(key) > 0, "Key should not be empty")
			assertTrue(t, len(value) > 0, "Value should not be empty")
			break
		}
	}

	assertEqual(t, 3, count, "Should have stopped after 3 items")
}

// createLargeCDBFile creates a CDB file with the specified number of entries for benchmarking
func createLargeCDBFile(filename string, numEntries int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	writer, err := cdb.NewWriter(f)
	if err != nil {
		return err
	}

	// Generate predictable test data
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key_%08d", i))
		value := []byte(fmt.Sprintf("value_%08d_data_payload", i))
		err := writer.Put(key, value)
		if err != nil {
			return err
		}
	}

	_, err = writer.Freeze()
	return err
}

const benchmarkEntries = 100000

func BenchmarkMmapIteratorAll(b *testing.B) {
	// Create test file
	filename := "/tmp/benchmark_iterator_all.cdb"
	err := createLargeCDBFile(filename, benchmarkEntries)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(filename)

	// Open the database
	db, err := cdb.OpenMmap(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Measure the performance of individual iteration steps
	iterations := 0
	for i := 0; i < b.N; i++ {
		// Create iterator once per benchmark iteration
		for key, value := range db.All() {
			// Access the key and value to ensure they're actually read
			_ = key[0]   // Force access to key data
			_ = value[0] // Force access to value data
			iterations++
			if iterations >= b.N {
				return
			}
		}
	}
}

func BenchmarkMmapIteratorKeys(b *testing.B) {
	// Create test file
	filename := "/tmp/benchmark_iterator_keys.cdb"
	err := createLargeCDBFile(filename, benchmarkEntries)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(filename)

	// Open the database
	db, err := cdb.OpenMmap(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Measure the performance of individual iteration steps
	iterations := 0
	for i := 0; i < b.N; i++ {
		// Create iterator once per benchmark iteration
		for key := range db.Keys() {
			// Access the key to ensure it's actually read
			_ = key[0] // Force access to key data
			iterations++
			if iterations >= b.N {
				return
			}
		}
	}
}

func BenchmarkMmapIteratorValues(b *testing.B) {
	// Create test file
	filename := "/tmp/benchmark_iterator_values.cdb"
	err := createLargeCDBFile(filename, benchmarkEntries)
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(filename)

	// Open the database
	db, err := cdb.OpenMmap(filename)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	// Measure the performance of individual iteration steps
	iterations := 0
	for i := 0; i < b.N; i++ {
		// Create iterator once per benchmark iteration
		for value := range db.Values() {
			// Access the value to ensure it's actually read
			_ = value[0] // Force access to value data
			iterations++
			if iterations >= b.N {
				return
			}
		}
	}
}
