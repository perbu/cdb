package cdb_test

import (
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/colinmarc/cdb"
)

func randomString(r *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func fnvHash(data []byte) uint32 {
	h := fnv.New32a()
	h.Write(data)
	return h.Sum32()
}

func testWritesReadable(t *testing.T, writer *cdb.Writer) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	expected := make([][][]byte, 0, 100)
	for i := 0; i < cap(expected); i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(randomString(r, 10))
		err := writer.Put(key, value)
		requireNoError(t, err)

		expected = append(expected, [][]byte{key, value})
	}

	db, err := writer.Freeze()
	requireNoError(t, err)

	for _, record := range expected {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		requireNil(t, err)
		assertEqual(t, string(record[1]), string(val), msg)
	}
}

func TestWritesReadable(t *testing.T) {
	f, err := os.CreateTemp("", "test-cdb")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	requireNoError(t, err)
	requireNotNil(t, writer)

	testWritesReadable(t, writer)
}

func TestWritesReadableFnv(t *testing.T) {
	f, err := os.CreateTemp("", "test-cdb")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, fnvHash)
	requireNoError(t, err)
	requireNotNil(t, writer)

	testWritesReadable(t, writer)
}

func testWritesRandom(t *testing.T, writer *cdb.Writer) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	records := make([][][]byte, 0, 1000)
	seenKeys := make(map[string]bool)
	stringType := reflect.TypeOf("")

	// Make sure we don't end up with duplicate keys, since that makes testing
	// hard.
	for len(records) < cap(records) {
		key, _ := quick.Value(stringType, random)
		if !seenKeys[key.String()] {
			value, _ := quick.Value(stringType, random)
			keyBytes := []byte(key.String())
			valueBytes := []byte(value.String())
			records = append(records, [][]byte{keyBytes, valueBytes})
			seenKeys[key.String()] = true
		}
	}

	for _, record := range records {
		err := writer.Put(record[0], record[1])
		requireNoError(t, err)
	}

	db, err := writer.Freeze()
	requireNoError(t, err)

	for _, record := range records {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		requireNil(t, err)
		assertEqual(t, string(record[1]), string(val), msg)
	}
}

func TestWritesRandom(t *testing.T) {
	f, err := os.CreateTemp("", "test-cdb")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	requireNoError(t, err)
	requireNotNil(t, writer)

	testWritesRandom(t, writer)
}

func TestWritesRandomFnv(t *testing.T) {
	f, err := os.CreateTemp("", "test-cdb")
	requireNoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, fnvHash)
	requireNoError(t, err)
	requireNotNil(t, writer)

	testWritesRandom(t, writer)
}

func benchmarkPut(b *testing.B, writer *cdb.Writer) {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	stringType := reflect.TypeOf("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := quick.Value(stringType, random)
		value, _ := quick.Value(stringType, random)
		keyBytes := []byte(key.String())
		valueBytes := []byte(value.String())

		writer.Put(keyBytes, valueBytes)
	}
}

func BenchmarkPut(b *testing.B) {
	f, err := os.CreateTemp("", "test-cdb")
	requireNoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter(f, nil)
	requireNoError(b, err)

	benchmarkPut(b, writer)
}

func BenchmarkPutFnv(b *testing.B) {
	f, err := os.CreateTemp("", "test-cdb")
	requireNoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter(f, fnvHash)
	requireNoError(b, err)

	benchmarkPut(b, writer)
}

// BenchmarkPut64 benchmarks the 64-bit writer Put method
func BenchmarkPut64(b *testing.B) {
	f, err := os.CreateTemp("", "test-cdb64")
	requireNoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter64(f, nil)
	requireNoError(b, err)

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	stringType := reflect.TypeOf("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := quick.Value(stringType, random)
		value, _ := quick.Value(stringType, random)
		keyBytes := []byte(key.String())
		valueBytes := []byte(value.String())

		writer.Put(keyBytes, valueBytes)
	}
}

func ExampleWriter() {
	writer, err := cdb.Create("/tmp/example.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Write some key/value pairs to the database.
	writer.Put([]byte("Alice"), []byte("Practice"))
	writer.Put([]byte("Bob"), []byte("Hope"))
	writer.Put([]byte("Charlie"), []byte("Horse"))

	// It's important to call Close or Freeze when you're finished writing
	// records.
	writer.Close()
}
