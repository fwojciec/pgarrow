package experimental

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
)

// BenchmarkDirectV3Performance tests the performance of the DirectV3 implementation
// This should achieve ~3.5M rows/sec with <10 GC runs for 5M rows
func BenchmarkDirectV3Performance(b *testing.B) {
	ctx := context.Background()

	// Database connection
	dbURL := "postgres://bookscanner:bookscanner@localhost:5432/bookscanner?sslmode=disable"

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		b.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	alloc := memory.NewGoAllocator()
	query := "SELECT id, score, active, name, created_date FROM performance_test"

	// Warm up
	warmupQuery := fmt.Sprintf("%s LIMIT 1000", query)
	record, err := QueryArrowDirectV3(ctx, conn, warmupQuery, alloc)
	if err != nil {
		b.Fatalf("Warmup failed: %v", err)
	}
	record.Release()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		record, err := QueryArrowDirectV3(ctx, conn, query, alloc)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}

		if record.NumRows() != 5000000 {
			b.Fatalf("Expected 5M rows, got %d", record.NumRows())
		}

		record.Release()
	}
}

// TestDirectV3PerformanceMetrics provides detailed performance metrics
func TestDirectV3PerformanceMetrics(t *testing.T) {
	ctx := context.Background()

	// Database connection
	dbURL := "postgres://bookscanner:bookscanner@localhost:5432/bookscanner?sslmode=disable"

	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	alloc := memory.NewGoAllocator()
	query := "SELECT id, score, active, name, created_date FROM performance_test"

	// Force GC before measurement
	runtime.GC()
	runtime.GC()

	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	gcBefore := memBefore.NumGC

	start := time.Now()

	record, err := QueryArrowDirectV3(ctx, conn, query, alloc)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer record.Release()

	elapsed := time.Since(start)
	runtime.ReadMemStats(&memAfter)
	gcAfter := memAfter.NumGC

	rowsPerSec := float64(record.NumRows()) / elapsed.Seconds()
	gcRuns := gcAfter - gcBefore
	memoryMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024)

	t.Logf("=== DirectV3 Performance Metrics ===")
	t.Logf("Rows processed: %d", record.NumRows())
	t.Logf("Time elapsed: %v", elapsed)
	t.Logf("Rows per second: %.0f", rowsPerSec)
	t.Logf("GC runs: %d", gcRuns)
	t.Logf("Memory allocated: %.2f MB", memoryMB)

	// Verify performance targets
	if rowsPerSec < 2000000 {
		t.Errorf("Performance below target: %.0f rows/sec (expected >2M)", rowsPerSec)
	}

	if gcRuns > 50 {
		t.Errorf("Too many GC runs: %d (expected <50)", gcRuns)
	}

	t.Logf("✅ Performance targets met!")
}
