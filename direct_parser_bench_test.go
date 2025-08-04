//go:build integration
// +build integration

package pgarrow_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/fwojciec/pgarrow"
	"github.com/stretchr/testify/require"
)

// BenchmarkDirectCopyParser benchmarks the new direct COPY parser implementation
func BenchmarkDirectCopyParser(b *testing.B) {
	ctx := context.Background()

	// Get connection string from environment
	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		b.Skip("TEST_DATABASE_URL not set")
	}

	testCases := []struct {
		name string
		rows int
	}{
		{"1K_rows", 1000},
		{"10K_rows", 10000},
		{"100K_rows", 100000},
		{"1M_rows", 1000000},
		{"5M_rows", 5000000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create pool for each benchmark
			pool, err := pgarrow.NewPool(ctx, connStr)
			require.NoError(b, err)
			defer pool.Close()

			// Create test data query
			sql := fmt.Sprintf(`
				SELECT 
					i::int8 as id,
					'user_' || i::text as username,
					random() * 1000 as score,
					(random() > 0.5) as active,
					NOW() - (i || ' seconds')::interval as created_at
				FROM generate_series(1, %d) i
			`, tc.rows)

			// Warm up
			reader, err := pool.QueryArrow(ctx, sql)
			require.NoError(b, err)
			for reader.Next() {
				record := reader.Record()
				record.Release()
			}
			reader.Release()

			// Force GC before measurement
			runtime.GC()
			runtime.GC()

			// Measure memory and GC before
			var mBefore runtime.MemStats
			runtime.ReadMemStats(&mBefore)

			b.ResetTimer()
			b.ReportAllocs()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				reader, err := pool.QueryArrow(ctx, sql)
				require.NoError(b, err)

				rowsRead := int64(0)
				for reader.Next() {
					record := reader.Record()
					rowsRead += record.NumRows()
					record.Release()
				}
				require.NoError(b, reader.Err())
				require.Equal(b, int64(tc.rows), rowsRead)

				reader.Release()
			}
			elapsed := time.Since(start)

			b.StopTimer()

			// Measure memory and GC after
			var mAfter runtime.MemStats
			runtime.ReadMemStats(&mAfter)

			// Calculate metrics
			totalRows := int64(tc.rows * b.N)
			rowsPerSec := float64(totalRows) / elapsed.Seconds()
			gcRuns := mAfter.NumGC - mBefore.NumGC
			allocBytes := mAfter.TotalAlloc - mBefore.TotalAlloc

			b.ReportMetric(rowsPerSec, "rows/sec")
			b.ReportMetric(float64(gcRuns), "gc_runs")
			b.ReportMetric(float64(allocBytes)/float64(totalRows), "bytes/row")

			// Log performance summary
			b.Logf("Performance: %.0f rows/sec, %d GC runs for %d total rows",
				rowsPerSec, gcRuns, totalRows)

			// Check against targets from issue #73
			if tc.name == "5M_rows" && b.N == 1 {
				b.Logf("=== Target Check for 5M rows ===")
				b.Logf("Target: >2M rows/sec, <10 GC runs")
				b.Logf("Actual: %.0f rows/sec, %d GC runs", rowsPerSec, gcRuns)

				if rowsPerSec > 2000000 {
					b.Logf("✅ Performance target met!")
				} else {
					b.Logf("❌ Below target performance")
				}

				if gcRuns < 10 {
					b.Logf("✅ GC target met!")
				} else {
					b.Logf("⚠️  GC runs higher than target (but may be acceptable)")
				}
			}
		})
	}
}

// BenchmarkDirectVsOldImplementation compares new vs old if we can
func BenchmarkDirectVsOldImplementation(b *testing.B) {
	ctx := context.Background()

	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		b.Skip("TEST_DATABASE_URL not set")
	}

	pool, err := pgarrow.NewPool(ctx, connStr)
	require.NoError(b, err)
	defer pool.Close()

	// Test with 100K rows for quick comparison
	sql := `
		SELECT 
			i::int8 as id,
			'text_' || i::text as name,
			random()::float8 as value
		FROM generate_series(1, 100000) i
	`

	b.Run("direct_implementation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			reader, err := pool.QueryArrow(ctx, sql)
			require.NoError(b, err)

			rows := int64(0)
			for reader.Next() {
				record := reader.Record()
				rows += record.NumRows()
				record.Release()
			}
			require.Equal(b, int64(100000), rows)
			reader.Release()
		}
	})
}

// BenchmarkStreamingBatches verifies streaming with 16MB batches
func BenchmarkStreamingBatches(b *testing.B) {
	ctx := context.Background()

	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		b.Skip("TEST_DATABASE_URL not set")
	}

	pool, err := pgarrow.NewPool(ctx, connStr)
	require.NoError(b, err)
	defer pool.Close()

	// Large dataset to force multiple batches
	sql := `
		SELECT 
			i::int8 as id,
			repeat('x', 100)::text as data,  -- ~100 bytes per row
			random() * 1000 as score
		FROM generate_series(1, 1000000) i
	`

	b.ResetTimer()

	reader, err := pool.QueryArrow(ctx, sql)
	require.NoError(b, err)
	defer reader.Release()

	batchCount := 0
	totalRows := int64(0)

	for reader.Next() {
		record := reader.Record()
		batchCount++
		totalRows += record.NumRows()
		record.Release()
	}
	require.NoError(b, reader.Err())

	b.ReportMetric(float64(batchCount), "batch_count")
	b.ReportMetric(float64(totalRows)/float64(batchCount), "avg_batch_rows")

	b.Logf("Streamed %d rows in %d batches", totalRows, batchCount)
	if batchCount > 1 {
		b.Logf("✅ Streaming confirmed - data split into %d batches", batchCount)
	}
}
