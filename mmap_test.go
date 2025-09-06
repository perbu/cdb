package cdb_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/perbu/cdb"
)

// Test helper functions

// createTestDB creates a temporary CDB database with the given data
func createTestDB(t *testing.T, prefix string, data map[string]string) (string, func()) {
	f, err := os.CreateTemp("", prefix)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		os.Remove(f.Name())
	}

	writer, err := cdb.NewWriter(f)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}

	for key, value := range data {
		err := writer.Put([]byte(key), []byte(value))
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
	}

	_, err = writer.Freeze()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		cleanup()
		t.Fatal(err)
	}

	return f.Name(), cleanup
}

// createTestDBWithSlices creates a temporary CDB database with slice data
func createTestDBWithSlices(t *testing.T, prefix string, data []struct{ key, value string }) (string, func()) {
	f, err := os.CreateTemp("", prefix)
	if err != nil {
		t.Fatal(err)
	}

	cleanup := func() {
		os.Remove(f.Name())
	}

	writer, err := cdb.NewWriter(f)
	if err != nil {
		cleanup()
		t.Fatal(err)
	}

	for _, item := range data {
		err := writer.Put([]byte(item.key), []byte(item.value))
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
	}

	_, err = writer.Freeze()
	if err != nil {
		cleanup()
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		cleanup()
		t.Fatal(err)
	}

	return f.Name(), cleanup
}

// compareMapResults compares expected map with actual results
func compareMapResults(t *testing.T, expected map[string]string, actual map[string]string) {
	for key, expectedValue := range expected {
		actualValue, exists := actual[key]
		if !exists {
			t.Errorf("Key not found in results: %s", key)
		}
		if expectedValue != actualValue {
			t.Errorf("Value mismatch for key %s: expected %q, got %q", key, expectedValue, actualValue)
		}
	}
}

// collectAllItems collects all key-value pairs from iterator and makes safe copies
func collectAllItems(db *cdb.MmapCDB) []struct{ key, value string } {
	var items []struct{ key, value string }
	for key, value := range db.All() {
		// Make copies since the slices point into mmap'd memory
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		items = append(items, struct{ key, value string }{string(keyCopy), string(valueCopy)})
	}
	return items
}

// collectKeys collects all keys from iterator and makes safe copies
func collectKeys(db *cdb.MmapCDB) []string {
	var keys []string
	for key := range db.Keys() {
		// Make copy since slice points into mmap'd memory
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		keys = append(keys, string(keyCopy))
	}
	return keys
}

// collectValues collects all values from iterator and makes safe copies
func collectValues(db *cdb.MmapCDB) []string {
	var values []string
	for value := range db.Values() {
		// Make copy since slice points into mmap'd memory
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		values = append(values, string(valueCopy))
	}
	return values
}

// setupBenchmarkDB creates a large CDB file for benchmarking and returns cleanup function
func setupBenchmarkDB(b *testing.B, filename string, numEntries int) (*cdb.MmapCDB, func()) {
	err := createLargeCDBFile(filename, numEntries)
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		os.Remove(filename)
	}

	// Open the database
	db, err := cdb.Open(filename)
	if err != nil {
		cleanup()
		b.Fatal(err)
	}

	return db, func() {
		db.Close()
		cleanup()
	}
}

func TestMmapCDB(t *testing.T) {
	testData := map[string]string{
		"foo":       "bar",
		"baz":       "quuuux",
		"empty":     "",
		"":          "empty_key",
		"collision": "test",
	}

	filename, cleanup := createTestDB(t, "test-mmap64", testData)
	defer cleanup()

	// Test memory-mapped reading
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Verify all data can be read correctly
	for key, expectedValue := range testData {
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get key: %s: %v", key, err)
		}
		if expectedValue != string(value) {
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

	// Test size method
	if !(db.Size() > 0) {
		t.Error("expected db.Size() > 0")
	}
}

func TestMmapErrorHandling(t *testing.T) {
	// Test opening non-existent file
	db, err := cdb.Open("nonexistent.cdb")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
	if db != nil {
		t.Error("expected nil db on error")
	}

	// Test with empty file (too small)
	f, err := os.CreateTemp("", "test-empty")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = cdb.Open(f.Name())
	if err == nil {
		t.Error("expected error for empty file")
	}
	if db != nil {
		t.Error("expected nil db on error")
	}
}

func TestMmapClose(t *testing.T) {
	testData := map[string]string{
		"test": "value",
	}

	filename, cleanup := createTestDB(t, "test-close", testData)
	defer cleanup()

	// Test that Close() can be called multiple times
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMmapIterator(t *testing.T) {
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

	filename, cleanup := createTestDBWithSlices(t, "test-iterator", testData)
	defer cleanup()

	// Test iterator
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Collect all items from iterator
	items := collectAllItems(db)

	// Verify we got all items (order might be different due to hashing)
	if len(testData) != len(items) {
		t.Errorf("expected %d items, got %d", len(testData), len(items))
	}

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
	compareMapResults(t, expectedMap, actualMap)
}

func TestMmapIteratorEmpty(t *testing.T) {
	// Create an empty database
	filename, cleanup := createTestDB(t, "test-empty-iterator", map[string]string{})
	defer cleanup()

	// Test iterator on empty database
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Count items using native iterator
	count := 0
	for range db.All() {
		count++
	}

	if 0 != count {
		t.Errorf("Empty database should not have any items: got %d", count)
	}
}

func TestMmapIteratorKeys(t *testing.T) {
	testData := map[string]string{
		"alpha": "value1",
		"beta":  "value2",
		"gamma": "value3",
	}

	filename, cleanup := createTestDB(t, "test-keys-iterator", testData)
	defer cleanup()

	// Test Keys() iterator
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Collect all keys
	keys := collectKeys(db)

	// Verify we got all keys
	if len(testData) != len(keys) {
		t.Errorf("expected %d keys, got %d", len(testData), len(keys))
	}

	// Convert to set for comparison
	keySet := make(map[string]bool)
	for _, key := range keys {
		keySet[key] = true
	}

	// Verify all expected keys are present
	for expectedKey := range testData {
		if !keySet[expectedKey] {
			t.Errorf("Key not found in Keys() results: %s", expectedKey)
		}
	}
}

func TestMmapIteratorValues(t *testing.T) {
	// Write test data with unique values
	testData := map[string]string{
		"key1": "alpha",
		"key2": "beta",
		"key3": "gamma",
	}

	filename, cleanup := createTestDB(t, "test-values-iterator", testData)
	defer cleanup()

	// Test Values() iterator
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Collect all values
	values := collectValues(db)

	// Verify we got all values
	if len(testData) != len(values) {
		t.Errorf("expected %d values, got %d", len(testData), len(values))
	}

	// Convert to set for comparison
	valueSet := make(map[string]bool)
	for _, value := range values {
		valueSet[value] = true
	}

	// Verify all expected values are present
	for _, expectedValue := range testData {
		if !valueSet[expectedValue] {
			t.Errorf("Value not found in Values() results: %s", expectedValue)
		}
	}
}

func TestMmapIteratorEarlyTermination(t *testing.T) {
	// Create test data with 10 items
	testData := make(map[string]string)
	for i := 0; i < 10; i++ {
		key := "key" + string(rune('0'+i))
		value := "value" + string(rune('0'+i))
		testData[key] = value
	}

	filename, cleanup := createTestDB(t, "test-early-termination", testData)
	defer cleanup()

	// Test early termination
	db, err := cdb.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Only iterate through first 3 items
	count := 0
	for key, value := range db.All() {
		count++
		if count >= 3 {
			// Test that we can access key and value at termination point
			if !(len(key) > 0) {
				t.Error("Key should not be empty")
			}
			if !(len(value) > 0) {
				t.Error("Value should not be empty")
			}
			break
		}
	}

	if 3 != count {
		t.Errorf("Should have stopped after 3 items: got %d", count)
	}
}

// createLargeCDBFile creates a CDB file with the specified number of entries for benchmarking
func createLargeCDBFile(filename string, numEntries int) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("os.Create(%q): %v", filename, err)
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

	if _, err := writer.Freeze(); err != nil {
		return fmt.Errorf("writer.Freeze(): %w", err)
	}
	return nil
}

const benchmarkEntries = 100000

func BenchmarkMmapIteratorAll(b *testing.B) {
	db, cleanup := setupBenchmarkDB(b, "/tmp/benchmark_iterator_all.cdb", benchmarkEntries)
	defer cleanup()

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
	db, cleanup := setupBenchmarkDB(b, "/tmp/benchmark_iterator_keys.cdb", benchmarkEntries)
	defer cleanup()

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
	db, cleanup := setupBenchmarkDB(b, "/tmp/benchmark_iterator_values.cdb", benchmarkEntries)
	defer cleanup()

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
