package cdb_test

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/colinmarc/cdb"
)

// Benchmark memory-mapped CDB vs regular CDB performance

func setupTestDatabase(name string, recordCount int) (string, [][][]byte) {
	f, err := os.CreateTemp("", name)
	if err != nil {
		panic(err)
	}

	writer, err := cdb.NewWriter(f, nil)
	if err != nil {
		panic(err)
	}

	testData := generateTestData(recordCount, 20, 100)
	for _, record := range testData {
		writer.Put(record[0], record[1])
	}

	_, err = writer.Freeze()
	if err != nil {
		panic(err)
	}

	f.Close()
	return f.Name(), testData
}

func setupTestDatabase64(name string, recordCount int) (string, [][][]byte) {
	f, err := os.CreateTemp("", name)
	if err != nil {
		panic(err)
	}

	writer, err := cdb.NewWriter64(f, nil)
	if err != nil {
		panic(err)
	}

	testData := generateTestData(recordCount, 20, 100)
	for _, record := range testData {
		writer.Put(record[0], record[1])
	}

	_, err = writer.Freeze()
	if err != nil {
		panic(err)
	}

	f.Close()
	return f.Name(), testData
}

// Single key lookup benchmarks

func BenchmarkGet_Regular_vs_Mmap(b *testing.B) {
	dbPath, testData := setupTestDatabase("bench-regular", 10000)
	defer os.Remove(dbPath)

	b.Run("Regular", func(b *testing.B) {
		db, err := cdb.Open(dbPath)
		requireNoError(b, err)
		defer db.Close()

		rand.Seed(time.Now().UnixNano())
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := testData[rand.Intn(len(testData))]
			_, err := db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Mmap", func(b *testing.B) {
		db, err := cdb.OpenMmap(dbPath)
		requireNoError(b, err)
		defer db.Close()

		rand.Seed(time.Now().UnixNano())
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := testData[rand.Intn(len(testData))]
			_, err := db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGet64_Regular_vs_Mmap(b *testing.B) {
	dbPath, testData := setupTestDatabase64("bench-regular64", 10000)
	defer os.Remove(dbPath)

	b.Run("Regular64", func(b *testing.B) {
		db, err := cdb.Open64(dbPath)
		requireNoError(b, err)
		defer db.Close()

		rand.Seed(time.Now().UnixNano())
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := testData[rand.Intn(len(testData))]
			_, err := db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Mmap64", func(b *testing.B) {
		db, err := cdb.OpenMmap64(dbPath)
		requireNoError(b, err)
		defer db.Close()

		rand.Seed(time.Now().UnixNano())
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := testData[rand.Intn(len(testData))]
			_, err := db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Sequential access benchmarks (simulating cache-friendly access patterns)

func BenchmarkSequentialGet_Regular_vs_Mmap(b *testing.B) {
	dbPath, testData := setupTestDatabase("bench-seq", 10000)
	defer os.Remove(dbPath)

	b.Run("Regular_Sequential", func(b *testing.B) {
		db, err := cdb.Open(dbPath)
		requireNoError(b, err)
		defer db.Close()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < len(testData) && j < 1000; j++ { // Limit to prevent timeout
				_, err := db.Get(testData[j][0])
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("Mmap_Sequential", func(b *testing.B) {
		db, err := cdb.OpenMmap(dbPath)
		requireNoError(b, err)
		defer db.Close()

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			for j := 0; j < len(testData) && j < 1000; j++ { // Limit to prevent timeout
				_, err := db.Get(testData[j][0])
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// Different database sizes

func BenchmarkDifferentSizes_Regular_vs_Mmap(b *testing.B) {
	sizes := []int{100, 1000, 10000}

	for _, size := range sizes {
		b.Run("Size_"+strconv.Itoa(size), func(b *testing.B) {
			dbPath, testData := setupTestDatabase("bench-size-"+strconv.Itoa(size), size)
			defer os.Remove(dbPath)

			b.Run("Regular", func(b *testing.B) {
				db, err := cdb.Open(dbPath)
				requireNoError(b, err)
				defer db.Close()

				rand.Seed(time.Now().UnixNano())
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					record := testData[rand.Intn(len(testData))]
					_, err := db.Get(record[0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("Mmap", func(b *testing.B) {
				db, err := cdb.OpenMmap(dbPath)
				requireNoError(b, err)
				defer db.Close()

				rand.Seed(time.Now().UnixNano())
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					record := testData[rand.Intn(len(testData))]
					_, err := db.Get(record[0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// Different value sizes to test impact on memory mapping

func BenchmarkDifferentValueSizes_Regular_vs_Mmap(b *testing.B) {
	valueSizes := []int{10, 100, 1000, 10000} // 10B to 10KB values

	for _, valueSize := range valueSizes {
		b.Run("ValueSize_"+strconv.Itoa(valueSize)+"B", func(b *testing.B) {
			// Create database with specific value size
			f, err := os.CreateTemp("", "bench-valuesize")
			requireNoError(b, err)
			defer os.Remove(f.Name())

			writer, err := cdb.NewWriter(f, nil)
			requireNoError(b, err)

			testData := generateTestData(1000, 20, valueSize)
			for _, record := range testData {
				writer.Put(record[0], record[1])
			}

			_, err = writer.Freeze()
			requireNoError(b, err)
			f.Close()

			b.Run("Regular", func(b *testing.B) {
				db, err := cdb.Open(f.Name())
				requireNoError(b, err)
				defer db.Close()

				rand.Seed(time.Now().UnixNano())
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					record := testData[rand.Intn(len(testData))]
					_, err := db.Get(record[0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("Mmap", func(b *testing.B) {
				db, err := cdb.OpenMmap(f.Name())
				requireNoError(b, err)
				defer db.Close()

				rand.Seed(time.Now().UnixNano())
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					record := testData[rand.Intn(len(testData))]
					_, err := db.Get(record[0])
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// Cold vs warm cache performance

func BenchmarkColdCache_Regular_vs_Mmap(b *testing.B) {
	dbPath, testData := setupTestDatabase("bench-cold", 10000)
	defer os.Remove(dbPath)

	b.Run("Regular_Cold", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db, err := cdb.Open(dbPath)
			requireNoError(b, err)

			record := testData[rand.Intn(len(testData))]
			b.StartTimer()

			_, err = db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			db.Close()
		}
	})

	b.Run("Mmap_Cold", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db, err := cdb.OpenMmap(dbPath)
			requireNoError(b, err)

			record := testData[rand.Intn(len(testData))]
			b.StartTimer()

			_, err = db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			db.Close()
		}
	})
}

// Hash collision stress test

func BenchmarkHashCollisions_Regular_vs_Mmap(b *testing.B) {
	// Create a database with keys that are likely to cause hash collisions
	f, err := os.CreateTemp("", "bench-collisions")
	requireNoError(b, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f, nil)
	requireNoError(b, err)

	// Use keys with similar patterns to increase collision probability
	var testData [][][]byte
	for i := 0; i < 1000; i++ {
		key := []byte("collision_key_" + strconv.Itoa(i))
		value := []byte("value_" + strconv.Itoa(i))
		writer.Put(key, value)
		testData = append(testData, [][]byte{key, value})
	}

	_, err = writer.Freeze()
	requireNoError(b, err)
	f.Close()

	b.Run("Regular_Collisions", func(b *testing.B) {
		db, err := cdb.Open(f.Name())
		requireNoError(b, err)
		defer db.Close()

		rand.Seed(time.Now().UnixNano())
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := testData[rand.Intn(len(testData))]
			_, err := db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Mmap_Collisions", func(b *testing.B) {
		db, err := cdb.OpenMmap(f.Name())
		requireNoError(b, err)
		defer db.Close()

		rand.Seed(time.Now().UnixNano())
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			record := testData[rand.Intn(len(testData))]
			_, err := db.Get(record[0])
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// Memory usage baseline (not a timing benchmark, but useful for comparison)

func BenchmarkMemoryFootprint_Regular_vs_Mmap(b *testing.B) {
	dbPath, testData := setupTestDatabase("bench-memory", 10000)
	defer os.Remove(dbPath)

	b.Run("Regular_Memory", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db, err := cdb.Open(dbPath)
			requireNoError(b, err)

			b.StartTimer()
			// Perform several lookups to show memory pattern
			for j := 0; j < 100; j++ {
				record := testData[j%len(testData)]
				_, err = db.Get(record[0])
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			db.Close()
		}
	})

	b.Run("Mmap_Memory", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db, err := cdb.OpenMmap(dbPath)
			requireNoError(b, err)

			b.StartTimer()
			// Perform several lookups to show memory pattern
			for j := 0; j < 100; j++ {
				record := testData[j%len(testData)]
				_, err = db.Get(record[0])
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			db.Close()
		}
	})
}
