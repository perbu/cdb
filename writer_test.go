package cdb_test

import (
	"bytes"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/perbu/cdb"
)

func randomString(r *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

func testWritesReadable(t *testing.T, writer *cdb.Writer) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	expected := make([][][]byte, 0, 100)
	for i := 0; i < cap(expected); i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(randomString(r, 10))
		err := writer.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}

		expected = append(expected, [][]byte{key, value})
	}

	db, err := writer.Freeze()
	if err != nil {
		t.Fatal(err)
	}

	for _, record := range expected {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
		if !bytes.Equal(val, record[1]) {
			t.Errorf("%s: expected %q, got %q", msg, string(record[1]), string(val))
		}
	}
}

func TestWritesReadable(t *testing.T) {
	f, err := os.CreateTemp("", "test-cdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	if writer == nil {
		t.Fatal("writer is nil")
	}

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
		if err != nil {
			t.Fatal(err)
		}
	}

	db, err := writer.Freeze()
	if err != nil {
		t.Fatal(err)
	}

	for _, record := range records {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		if err != nil {
			t.Fatalf("%s: %v", msg, err)
		}
		if !bytes.Equal(val, record[1]) {
			t.Errorf("%s: expected %q, got %q", msg, string(record[1]), string(val))
		}
	}
}

func TestWritesRandom(t *testing.T) {
	f, err := os.CreateTemp("", "test-cdb")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	if writer == nil {
		t.Fatal("writer is nil")
	}

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
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter(f)
	if err != nil {
		b.Fatal(err)
	}

	benchmarkPut(b, writer)
}
