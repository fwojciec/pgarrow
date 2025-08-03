package pgarrow_test

import (
	"runtime/debug"
	"testing"

	"github.com/fwojciec/pgarrow"
)

func TestBufferPool_Basic(t *testing.T) {
	t.Parallel()

	pool := pgarrow.NewBufferPool()

	// Test byte slice pools
	smallSlice := pool.GetByteSlice(100)
	if cap(smallSlice) != pgarrow.SmallBufferSizeBytes {
		t.Errorf("Expected small slice capacity %d, got %d", pgarrow.SmallBufferSizeBytes, cap(smallSlice))
	}

	mediumSlice := pool.GetByteSlice(1000)
	if cap(mediumSlice) != pgarrow.MediumBufferSizeBytes {
		t.Errorf("Expected medium slice capacity %d, got %d", pgarrow.MediumBufferSizeBytes, cap(mediumSlice))
	}

	largeSlice := pool.GetByteSlice(10000)
	if cap(largeSlice) != pgarrow.LargeBufferSizeBytes {
		t.Errorf("Expected large slice capacity %d, got %d", pgarrow.LargeBufferSizeBytes, cap(largeSlice))
	}

	// Test putting slices back
	pool.PutByteSlice(smallSlice)
	pool.PutByteSlice(mediumSlice)
	pool.PutByteSlice(largeSlice)

	// Test reuse
	reusedSmallSlice := pool.GetByteSlice(100)
	if cap(reusedSmallSlice) != pgarrow.SmallBufferSizeBytes {
		t.Errorf("Expected reused small slice capacity %d, got %d", pgarrow.SmallBufferSizeBytes, cap(reusedSmallSlice))
	}
}

func TestGetOptimalBatchSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dataType uint32
		expected int
	}{
		{"Bool", pgarrow.TypeOIDBool, pgarrow.OptimalBatchSizeSmall},
		{"Int2", pgarrow.TypeOIDInt2, pgarrow.OptimalBatchSizeSmall},
		{"Int4", pgarrow.TypeOIDInt4, pgarrow.OptimalBatchSizeSmall},
		{"Int8", pgarrow.TypeOIDInt8, pgarrow.OptimalBatchSizeMedium},
		{"Float4", pgarrow.TypeOIDFloat4, pgarrow.OptimalBatchSizeMedium},
		{"Float8", pgarrow.TypeOIDFloat8, pgarrow.OptimalBatchSizeMedium},
		{"Text", pgarrow.TypeOIDText, pgarrow.OptimalBatchSizeLarge},
		{"Varchar", pgarrow.TypeOIDVarchar, pgarrow.OptimalBatchSizeLarge},
		{"Bytea", pgarrow.TypeOIDBytea, pgarrow.OptimalBatchSizeLarge},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual := pgarrow.GetOptimalBatchSize(tt.dataType)
			if actual != tt.expected {
				t.Errorf("GetOptimalBatchSize(%d) = %d, want %d", tt.dataType, actual, tt.expected)
			}
		})
	}
}

func TestGetOptimalBatchSizeForSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fieldOIDs []uint32
		expected  int
	}{
		{
			name:      "Empty schema",
			fieldOIDs: []uint32{},
			expected:  pgarrow.OptimalBatchSizeGo,
		},
		{
			name:      "Mostly small types",
			fieldOIDs: []uint32{pgarrow.TypeOIDBool, pgarrow.TypeOIDInt2, pgarrow.TypeOIDInt4},
			expected:  pgarrow.OptimalBatchSizeSmall,
		},
		{
			name:      "Mostly large types",
			fieldOIDs: []uint32{pgarrow.TypeOIDText, pgarrow.TypeOIDVarchar, pgarrow.TypeOIDBytea},
			expected:  pgarrow.OptimalBatchSizeLarge,
		},
		{
			name:      "Mixed types",
			fieldOIDs: []uint32{pgarrow.TypeOIDInt4, pgarrow.TypeOIDInt8, pgarrow.TypeOIDText},
			expected:  pgarrow.OptimalBatchSizeMedium,
		},
		{
			name:      "Mostly medium types",
			fieldOIDs: []uint32{pgarrow.TypeOIDInt8, pgarrow.TypeOIDFloat8, pgarrow.TypeOIDDate},
			expected:  pgarrow.OptimalBatchSizeMedium,
		},
	}

	for _, tt := range tests {
		tt := tt // Capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual := pgarrow.GetOptimalBatchSizeForSchema(tt.fieldOIDs)
			if actual != tt.expected {
				t.Errorf("GetOptimalBatchSizeForSchema(%v) = %d, want %d", tt.fieldOIDs, actual, tt.expected)
			}
		})
	}
}

// BenchmarkBufferPoolEfficiency measures buffer pool reuse efficiency
func BenchmarkBufferPoolEfficiency(b *testing.B) {
	pool := pgarrow.NewBufferPool()

	b.Run("WithPool", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			slice := pool.GetByteSlice(1000)
			// Simulate some work with the slice
			slice = slice[:100]
			for j := 0; j < 100; j++ {
				slice[j] = byte(j % 256)
			}
			pool.PutByteSlice(slice)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			slice := make([]byte, 100, 1000)
			// Simulate some work with the slice
			for j := 0; j < 100; j++ {
				slice[j] = byte(j % 256)
			}
			// Let GC handle the slice
			_ = slice
		}
	})
}

// BenchmarkMemoryLayoutOptimalBatchSizes tests different batch sizes for Go GC performance
func BenchmarkMemoryLayoutOptimalBatchSizes(b *testing.B) {
	batchSizes := []int{64, 128, 256, 512, 1024}

	for _, batchSize := range batchSizes {
		b.Run(formatBatchSizeName(batchSize), func(b *testing.B) {
			benchmarkBatchSizeGCImpact(b, batchSize)
		})
	}
}

func formatBatchSizeName(batchSize int) string {
	return "Batch_" + string(rune('0'+batchSize/100)) + string(rune('0'+(batchSize%100)/10)) + string(rune('0'+batchSize%10))
}

func benchmarkBatchSizeGCImpact(b *testing.B, batchSize int) {
	b.Helper()
	b.ReportAllocs()

	var gcStatsBefore, gcStatsAfter debug.GCStats
	debug.ReadGCStats(&gcStatsBefore)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate batch processing with different sizes
		batch := make([][]byte, batchSize)
		for j := 0; j < batchSize; j++ {
			batch[j] = make([]byte, 64) // Simulate field data
			// Simulate some processing
			for k := 0; k < 64; k++ {
				batch[j][k] = byte(k % 256)
			}
		}
		// Process the batch
		totalBytes := 0
		for _, field := range batch {
			totalBytes += len(field)
		}
		_ = totalBytes
	}

	b.StopTimer()

	debug.ReadGCStats(&gcStatsAfter)
	gcPauseIncrease := gcStatsAfter.PauseTotal - gcStatsBefore.PauseTotal

	if gcPauseIncrease > 0 {
		gcPausePerOp := float64(gcPauseIncrease.Nanoseconds()) / float64(b.N)
		b.ReportMetric(gcPausePerOp, "gc-ns/op")
	}
}

// BenchmarkCacheAlignedStructs tests cache alignment benefits
func BenchmarkCacheAlignedStructs(b *testing.B) {
	const numItems = 1000

	b.Run("CacheAligned", func(b *testing.B) {
		// Simulate cache-aligned FieldData
		type AlignedFieldData struct {
			Data   []byte
			IsNull bool
			_      [7]byte
		}

		fields := make([]AlignedFieldData, numItems)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Sequential access pattern (cache-friendly)
			for j := 0; j < numItems; j++ {
				fields[j].IsNull = j%10 == 0
				if !fields[j].IsNull {
					fields[j].Data = make([]byte, 32)
				}
			}
		}
	})

	b.Run("NotAligned", func(b *testing.B) {
		// Simulate non-aligned structure
		type NotAlignedFieldData struct {
			Data   []byte
			IsNull bool
			// No padding
		}

		fields := make([]NotAlignedFieldData, numItems)

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Sequential access pattern
			for j := 0; j < numItems; j++ {
				fields[j].IsNull = j%10 == 0
				if !fields[j].IsNull {
					fields[j].Data = make([]byte, 32)
				}
			}
		}
	})
}

// BenchmarkGCPressureReduction measures GC impact with and without optimizations
func BenchmarkGCPressureReduction(b *testing.B) {
	b.Run("WithOptimizations", func(b *testing.B) {
		pool := pgarrow.NewBufferPool()

		b.ReportAllocs()

		// Measure GC before
		var gcBefore debug.GCStats
		debug.ReadGCStats(&gcBefore)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Use optimal batch size
			batchSize := pgarrow.OptimalBatchSizeGo

			// Reuse buffers from pool
			rowBuffer := pool.GetRowBuffer()

			for j := 0; j < batchSize; j++ {
				fieldData := pool.GetFieldDataSlice()

				// Simulate processing
				for k := 0; k < 5; k++ { // 5 columns
					fieldData = append(fieldData, pgarrow.FieldData{
						Data:   pool.GetByteSlice(32),
						IsNull: k%10 == 0,
					})
				}

				rowBuffer = append(rowBuffer, fieldData)
			}

			// Return to pool
			for _, row := range rowBuffer {
				for _, field := range row {
					pool.PutByteSlice(field.Data)
				}
				pool.PutFieldDataSlice(row)
			}
			pool.PutRowBuffer(rowBuffer)
		}

		b.StopTimer()

		// Measure GC after
		var gcAfter debug.GCStats
		debug.ReadGCStats(&gcAfter)

		gcIncrease := gcAfter.PauseTotal - gcBefore.PauseTotal
		if gcIncrease > 0 {
			gcPerOp := float64(gcIncrease.Nanoseconds()) / float64(b.N)
			b.ReportMetric(gcPerOp, "gc-ns/op")
		}
	})

	b.Run("WithoutOptimizations", func(b *testing.B) {
		b.ReportAllocs()

		// Measure GC before
		var gcBefore debug.GCStats
		debug.ReadGCStats(&gcBefore)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Use larger, less optimal batch size
			batchSize := 1024

			// Allocate new slices every time
			rowBuffer := make([][]pgarrow.FieldData, 0, batchSize)

			for j := 0; j < batchSize; j++ {
				fieldData := make([]pgarrow.FieldData, 0, 5)

				// Simulate processing with fresh allocations
				for k := 0; k < 5; k++ { // 5 columns
					fieldData = append(fieldData, pgarrow.FieldData{
						Data:   make([]byte, 32),
						IsNull: k%10 == 0,
					})
				}

				rowBuffer = append(rowBuffer, fieldData)
			}

			// Let GC handle cleanup
			_ = rowBuffer
		}

		b.StopTimer()

		// Measure GC after
		var gcAfter debug.GCStats
		debug.ReadGCStats(&gcAfter)

		gcIncrease := gcAfter.PauseTotal - gcBefore.PauseTotal
		if gcIncrease > 0 {
			gcPerOp := float64(gcIncrease.Nanoseconds()) / float64(b.N)
			b.ReportMetric(gcPerOp, "gc-ns/op")
		}
	})
}

// BenchmarkMemoryAllocationPatterns compares allocation patterns
func BenchmarkMemoryAllocationPatterns(b *testing.B) {
	b.Run("OptimalBatchSize", func(b *testing.B) {
		batchSize := pgarrow.OptimalBatchSizeGo
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := make([][]byte, batchSize)
			for j := 0; j < batchSize; j++ {
				data[j] = make([]byte, 64)
			}
		}
	})

	b.Run("LargeBatchSize", func(b *testing.B) {
		batchSize := 1024
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := make([][]byte, batchSize)
			for j := 0; j < batchSize; j++ {
				data[j] = make([]byte, 64)
			}
		}
	})

	b.Run("SmallBatchSize", func(b *testing.B) {
		batchSize := 64
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			data := make([][]byte, batchSize)
			for j := 0; j < batchSize; j++ {
				data[j] = make([]byte, 64)
			}
		}
	})
}

func TestMemoryPoolStatistics(t *testing.T) {
	t.Parallel()

	pool := pgarrow.NewBatchParserPool()

	// Record some operations
	pool.RecordAllocation()
	pool.RecordPoolHit()
	pool.RecordPoolHit()
	pool.RecordPoolMiss()

	allocs, hits, misses := pool.GetStats()

	if allocs != 1 {
		t.Errorf("Expected 1 allocation, got %d", allocs)
	}
	if hits != 2 {
		t.Errorf("Expected 2 pool hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 pool miss, got %d", misses)
	}

	efficiency := pool.GetPoolEfficiency()
	expectedEfficiency := float64(2) / float64(3) * 100.0
	if efficiency != expectedEfficiency {
		t.Errorf("Expected efficiency %.2f%%, got %.2f%%", expectedEfficiency, efficiency)
	}
}

// TestMemoryLayoutConstants verifies our Go-optimized constants
func TestMemoryLayoutConstants(t *testing.T) {
	t.Parallel()

	// Verify cache line alignment
	if pgarrow.CacheLineSizeBytes != 64 {
		t.Errorf("Expected cache line size 64, got %d", pgarrow.CacheLineSizeBytes)
	}

	// Verify Go page size
	if pgarrow.GoPageSizeBytes != 8192 {
		t.Errorf("Expected Go page size 8192, got %d", pgarrow.GoPageSizeBytes)
	}

	// Verify optimal batch sizes are reasonable
	if pgarrow.OptimalBatchSizeGo < pgarrow.MinBatchSizeGo || pgarrow.OptimalBatchSizeGo > pgarrow.MaxBatchSizeGo {
		t.Errorf("Optimal batch size %d is outside range [%d, %d]",
			pgarrow.OptimalBatchSizeGo, pgarrow.MinBatchSizeGo, pgarrow.MaxBatchSizeGo)
	}

	// Verify type-specific batch sizes make sense
	if pgarrow.OptimalBatchSizeSmall <= pgarrow.OptimalBatchSizeMedium {
		t.Errorf("Small batch size (%d) should be larger than medium (%d)",
			pgarrow.OptimalBatchSizeSmall, pgarrow.OptimalBatchSizeMedium)
	}

	if pgarrow.OptimalBatchSizeMedium <= pgarrow.OptimalBatchSizeLarge {
		t.Errorf("Medium batch size (%d) should be larger than large (%d)",
			pgarrow.OptimalBatchSizeMedium, pgarrow.OptimalBatchSizeLarge)
	}
}
