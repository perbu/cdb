package cdb_test

import (
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"testing/quick"
	"time"

	"github.com/colinmarc/cdb"
	"github.com/stretchr/testify/require"
)

// Helper functions for benchmark data generation

func generateTestData(n int, keySize, valueSize int) [][][]byte {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	data := make([][][]byte, n)

	for i := 0; i < n; i++ {
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		random.Read(key)
		random.Read(value)
		// Ensure keys are unique by adding index
		key = append([]byte(strconv.Itoa(i)+"_"), key...)
		data[i] = [][]byte{key, value}
	}
	return data
}

func generateRandomStringData(n int) [][][]byte {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	stringType := reflect.TypeOf("")
	data := make([][][]byte, n)
	seenKeys := make(map[string]bool)

	for i := 0; i < n; {
		key, _ := quick.Value(stringType, random)
		keyStr := strconv.Itoa(i) + "_" + key.String() // Make unique
		if !seenKeys[keyStr] {
			value, _ := quick.Value(stringType, random)
			data[i] = [][]byte{[]byte(keyStr), []byte(value.String())}
			seenKeys[keyStr] = true
			i++
		}
	}
	return data
}

// Writer64 Benchmarks

func BenchmarkWriter64_Put(b *testing.B) {
	f, err := os.CreateTemp("", "bench-cdb64")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter64(f, nil)
	require.NoError(b, err)

	// Generate test data
	testData := generateRandomStringData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Put(testData[i][0], testData[i][1])
	}
}

func BenchmarkWriter64_PutBatch(b *testing.B) {
	batchSizes := []int{100, 1000, 10000}

	for _, batchSize := range batchSizes {
		b.Run(strconv.Itoa(batchSize), func(b *testing.B) {
			testData := generateTestData(batchSize, 20, 100)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				f, err := os.CreateTemp("", "bench-cdb64")
				require.NoError(b, err)

				writer, err := cdb.NewWriter64(f, nil)
				require.NoError(b, err)

				for _, record := range testData {
					writer.Put(record[0], record[1])
				}

				writer.Close()
				f.Close()
				os.Remove(f.Name())
			}
		})
	}
}

func BenchmarkWriter64_Freeze(b *testing.B) {
	recordCounts := []int{1000, 10000, 100000}

	for _, recordCount := range recordCounts {
		b.Run(strconv.Itoa(recordCount), func(b *testing.B) {
			testData := generateTestData(recordCount, 20, 100)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				f, err := os.CreateTemp("", "bench-cdb64")
				require.NoError(b, err)

				writer, err := cdb.NewWriter64(f, nil)
				require.NoError(b, err)

				// Pre-populate the writer
				for _, record := range testData {
					writer.Put(record[0], record[1])
				}

				b.StartTimer()
				db, err := writer.Freeze()
				require.NoError(b, err)

				b.StopTimer()
				db.Close()
				f.Close()
				os.Remove(f.Name())
			}
		})
	}
}

func BenchmarkWriter64_LargeData(b *testing.B) {
	valueSizes := []int{1024, 10240, 102400} // 1KB, 10KB, 100KB values

	for _, valueSize := range valueSizes {
		b.Run(strconv.Itoa(valueSize)+"B", func(b *testing.B) {
			testData := generateTestData(100, 20, valueSize)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				f, err := os.CreateTemp("", "bench-cdb64")
				require.NoError(b, err)

				writer, err := cdb.NewWriter64(f, nil)
				require.NoError(b, err)

				b.StartTimer()
				for _, record := range testData {
					writer.Put(record[0], record[1])
				}

				b.StopTimer()
				writer.Close()
				f.Close()
				os.Remove(f.Name())
			}
		})
	}
}

func BenchmarkWriter64_CreateAndWrite(b *testing.B) {
	testData := generateTestData(1000, 20, 100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := os.CreateTemp("", "bench-cdb64")
		require.NoError(b, err)

		writer, err := cdb.NewWriter64(f, nil)
		require.NoError(b, err)

		for _, record := range testData {
			writer.Put(record[0], record[1])
		}

		db, err := writer.Freeze()
		require.NoError(b, err)

		db.Close()
		f.Close()
		os.Remove(f.Name())
	}
}

// Iterator64 Benchmarks

func BenchmarkIterator64_Next(b *testing.B) {
	// Create a test database
	f, err := os.CreateTemp("", "bench-cdb64")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter64(f, nil)
	require.NoError(b, err)

	testData := generateTestData(10000, 20, 100)
	for _, record := range testData {
		writer.Put(record[0], record[1])
	}

	db, err := writer.Freeze()
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := db.Iter()
		for iter.Next() {
			// Access key and value to ensure they're read
			_ = iter.Key()
			_ = iter.Value()
		}
	}
}

func BenchmarkIterator64_FullScan(b *testing.B) {
	recordCounts := []int{1000, 10000, 100000}

	for _, recordCount := range recordCounts {
		b.Run(strconv.Itoa(recordCount), func(b *testing.B) {
			// Create a test database
			f, err := os.CreateTemp("", "bench-cdb64")
			require.NoError(b, err)
			defer func() {
				f.Close()
				os.Remove(f.Name())
			}()

			writer, err := cdb.NewWriter64(f, nil)
			require.NoError(b, err)

			testData := generateTestData(recordCount, 20, 100)
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			db, err := writer.Freeze()
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				iter := db.Iter()
				for iter.Next() {
					count++
				}
				if count != recordCount {
					b.Fatalf("Expected %d records, got %d", recordCount, count)
				}
			}
		})
	}
}

func BenchmarkIterator64_LargeDB(b *testing.B) {
	valueSizes := []int{1024, 10240} // 1KB, 10KB values

	for _, valueSize := range valueSizes {
		b.Run(strconv.Itoa(valueSize)+"B", func(b *testing.B) {
			// Create a test database with large values
			f, err := os.CreateTemp("", "bench-cdb64")
			require.NoError(b, err)
			defer func() {
				f.Close()
				os.Remove(f.Name())
			}()

			writer, err := cdb.NewWriter64(f, nil)
			require.NoError(b, err)

			testData := generateTestData(1000, 20, valueSize)
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			db, err := writer.Freeze()
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter := db.Iter()
				for iter.Next() {
					// Access the data to ensure it's fully read
					_ = len(iter.Key())
					_ = len(iter.Value())
				}
			}
		})
	}
}

// Generic Implementation Benchmarks

func BenchmarkWriterGeneric_uint32(b *testing.B) {
	f, err := os.CreateTemp("", "bench-cdb-generic32")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriterGeneric[uint32](f, nil)
	require.NoError(b, err)

	testData := generateRandomStringData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Put(testData[i][0], testData[i][1])
	}
}

func BenchmarkWriterGeneric_uint64(b *testing.B) {
	f, err := os.CreateTemp("", "bench-cdb-generic64")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriterGeneric[uint64](f, nil)
	require.NoError(b, err)

	testData := generateRandomStringData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Put(testData[i][0], testData[i][1])
	}
}

func BenchmarkIteratorGeneric_uint32(b *testing.B) {
	// Create a test database
	f, err := os.CreateTemp("", "bench-cdb-generic32")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriterGeneric[uint32](f, nil)
	require.NoError(b, err)

	testData := generateTestData(10000, 20, 100)
	for _, record := range testData {
		writer.Put(record[0], record[1])
	}

	db, err := writer.Freeze()
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := db.Iter()
		for iter.Next() {
			_ = iter.Key()
			_ = iter.Value()
		}
	}
}

func BenchmarkIteratorGeneric_uint64(b *testing.B) {
	// Create a test database
	f, err := os.CreateTemp("", "bench-cdb-generic64")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriterGeneric[uint64](f, nil)
	require.NoError(b, err)

	testData := generateTestData(10000, 20, 100)
	for _, record := range testData {
		writer.Put(record[0], record[1])
	}

	db, err := writer.Freeze()
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := db.Iter()
		for iter.Next() {
			_ = iter.Key()
			_ = iter.Value()
		}
	}
}

// Comparison Benchmarks (32-bit vs 64-bit)

func BenchmarkComparison_Writer_Put_32vs64(b *testing.B) {
	testData := generateTestData(1000, 20, 100)

	b.Run("Writer32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			f, err := os.CreateTemp("", "bench-cmp-32")
			require.NoError(b, err)

			writer, err := cdb.NewWriter(f, nil)
			require.NoError(b, err)

			b.StartTimer()
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			b.StopTimer()
			writer.Close()
			f.Close()
			os.Remove(f.Name())
		}
	})

	b.Run("Writer64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			f, err := os.CreateTemp("", "bench-cmp-64")
			require.NoError(b, err)

			writer, err := cdb.NewWriter64(f, nil)
			require.NoError(b, err)

			b.StartTimer()
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			b.StopTimer()
			writer.Close()
			f.Close()
			os.Remove(f.Name())
		}
	})
}

func BenchmarkComparison_Iterator_Scan_32vs64(b *testing.B) {
	// Prepare 32-bit database
	f32, err := os.CreateTemp("", "bench-cmp-32")
	require.NoError(b, err)
	defer func() {
		f32.Close()
		os.Remove(f32.Name())
	}()

	writer32, err := cdb.NewWriter(f32, nil)
	require.NoError(b, err)

	testData := generateTestData(10000, 20, 100)
	for _, record := range testData {
		writer32.Put(record[0], record[1])
	}

	db32, err := writer32.Freeze()
	require.NoError(b, err)

	// Prepare 64-bit database
	f64, err := os.CreateTemp("", "bench-cmp-64")
	require.NoError(b, err)
	defer func() {
		f64.Close()
		os.Remove(f64.Name())
	}()

	writer64, err := cdb.NewWriter64(f64, nil)
	require.NoError(b, err)

	for _, record := range testData {
		writer64.Put(record[0], record[1])
	}

	db64, err := writer64.Freeze()
	require.NoError(b, err)

	b.Run("Iterator32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := db32.Iter()
			for iter.Next() {
				_ = iter.Key()
				_ = iter.Value()
			}
		}
	})

	b.Run("Iterator64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := db64.Iter()
			for iter.Next() {
				_ = iter.Key()
				_ = iter.Value()
			}
		}
	})
}

func BenchmarkComparison_Generic_32vs64(b *testing.B) {
	testData := generateTestData(1000, 20, 100)

	b.Run("Generic32", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			f, err := os.CreateTemp("", "bench-generic-32")
			require.NoError(b, err)

			writer, err := cdb.NewWriterGeneric[uint32](f, nil)
			require.NoError(b, err)

			b.StartTimer()
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			b.StopTimer()
			writer.Close()
			f.Close()
			os.Remove(f.Name())
		}
	})

	b.Run("Generic64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			f, err := os.CreateTemp("", "bench-generic-64")
			require.NoError(b, err)

			writer, err := cdb.NewWriterGeneric[uint64](f, nil)
			require.NoError(b, err)

			b.StartTimer()
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			b.StopTimer()
			writer.Close()
			f.Close()
			os.Remove(f.Name())
		}
	})
}
