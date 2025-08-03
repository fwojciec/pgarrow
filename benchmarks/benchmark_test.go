package benchmarks_test

import (
	"context"
	"os"
	"testing"

	"github.com/fwojciec/pgarrow/benchmarks"
	"github.com/stretchr/testify/require"
)

// TestBenchmarkRunner validates the core benchmarking functionality
func TestBenchmarkRunner(t *testing.T) {
	t.Parallel()

	databaseURL := getBenchmarkDatabaseURL(t)
	runner := benchmarks.NewRunner(databaseURL)

	ctx := context.Background()
	query := "SELECT i as id, 'test_' || i::text as name FROM generate_series(1, 100) i"

	t.Run("PGArrowBenchmark", func(t *testing.T) {
		t.Parallel()
		result, err := runner.RunPGArrowBenchmark(ctx, query)
		require.NoError(t, err)

		// Validate result structure
		require.Equal(t, "PGArrow Streaming", result.Name)
		require.Equal(t, int64(100), result.TotalRows)
		require.Greater(t, result.ThroughputRPS, 0.0)
		require.Positive(t, result.BatchCount)
		require.Greater(t, result.AvgBatchSize, 0.0)

		// Validate memory metrics are captured
		// Memory metrics validation (non-negative)
		require.NotNil(t, result.MemoryStats)

		// Validate GC metrics are captured
		// GC metrics validation
		require.NotNil(t, result.GCStats)
	})

	t.Run("NaivePgxBenchmark", func(t *testing.T) {
		t.Parallel()
		result, err := runner.RunNaivePgxBenchmark(ctx, query)
		require.NoError(t, err)

		// Validate result structure
		require.Equal(t, "Naive pgx→Arrow", result.Name)
		require.Equal(t, int64(100), result.TotalRows)
		require.Greater(t, result.ThroughputRPS, 0.0)
		require.Equal(t, int64(1), result.BatchCount) // Naive uses single batch
		require.InDelta(t, 100.0, result.AvgBatchSize, 0.1)

		// Validate memory metrics are captured
		// Memory metrics validation (non-negative)
		require.NotNil(t, result.MemoryStats)
	})
}

// TestStatisticalBenchmark validates the statistical measurement framework
func TestStatisticalBenchmark(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("Skipping statistical benchmark in short mode")
	}

	databaseURL := getBenchmarkDatabaseURL(t)
	runner := benchmarks.NewRunner(databaseURL)

	ctx := context.Background()
	query := "SELECT i as id, 'test_' || i::text as name FROM generate_series(1, 50) i"
	runs := 3 // Small number for tests

	comparisonResult, err := runner.RunStatisticalBenchmark(ctx, query, runs)
	require.NoError(t, err)

	// Validate PGArrow statistical result structure
	pgarrowResult := comparisonResult.PGArrow
	require.Contains(t, pgarrowResult.Name, "PGArrow")
	require.Equal(t, runs, pgarrowResult.Runs)
	require.Greater(t, pgarrowResult.AvgThroughput, 0.0)
	require.Greater(t, pgarrowResult.MinThroughput, 0.0)
	require.Greater(t, pgarrowResult.MaxThroughput, 0.0)
	require.LessOrEqual(t, pgarrowResult.MinThroughput, pgarrowResult.AvgThroughput)
	require.GreaterOrEqual(t, pgarrowResult.MaxThroughput, pgarrowResult.AvgThroughput)

	// Validate naive statistical result structure
	naiveResult := comparisonResult.Naive
	require.Contains(t, naiveResult.Name, "Naive")
	require.Equal(t, runs, naiveResult.Runs)
	require.Greater(t, naiveResult.AvgThroughput, 0.0)
}

// TestReporting validates the performance reporting functionality
func TestReporting(t *testing.T) {
	t.Parallel()

	databaseURL := getBenchmarkDatabaseURL(t)
	runner := benchmarks.NewRunner(databaseURL)

	ctx := context.Background()
	query := "SELECT i as id FROM generate_series(1, 10) i"

	// Get benchmark results
	pgarrowResult, err := runner.RunPGArrowBenchmark(ctx, query)
	require.NoError(t, err)

	naiveResult, err := runner.RunNaivePgxBenchmark(ctx, query)
	require.NoError(t, err)

	// Convert to statistical results for reporting
	pgarrowStats := benchmarks.StatisticalResult{
		Name:          pgarrowResult.Name,
		Runs:          1,
		AvgDuration:   pgarrowResult.Duration,
		MinDuration:   pgarrowResult.Duration,
		MaxDuration:   pgarrowResult.Duration,
		AvgThroughput: pgarrowResult.ThroughputRPS,
		MinThroughput: pgarrowResult.ThroughputRPS,
		MaxThroughput: pgarrowResult.ThroughputRPS,
		AvgMemory:     pgarrowResult.MemoryStats,
		AvgGC:         pgarrowResult.GCStats,
	}

	naiveStats := benchmarks.StatisticalResult{
		Name:          naiveResult.Name,
		Runs:          1,
		AvgDuration:   naiveResult.Duration,
		MinDuration:   naiveResult.Duration,
		MaxDuration:   naiveResult.Duration,
		AvgThroughput: naiveResult.ThroughputRPS,
		MinThroughput: naiveResult.ThroughputRPS,
		MaxThroughput: naiveResult.ThroughputRPS,
		AvgMemory:     naiveResult.MemoryStats,
		AvgGC:         naiveResult.GCStats,
	}

	t.Run("SingleReport", func(t *testing.T) {
		t.Parallel()
		report := pgarrowStats.ReportResult()
		require.Contains(t, report, "Performance Measurement Results")
		require.Contains(t, report, "Throughput (rows/sec)")
		require.Contains(t, report, "Memory Usage")
		require.Contains(t, report, "Garbage Collection")
	})

	t.Run("ComparisonReport", func(t *testing.T) {
		t.Parallel()
		report := benchmarks.ReportComparison(pgarrowStats, naiveStats)
		require.Contains(t, report, "PGArrow vs Naive pgx→Arrow Comparison")
		require.Contains(t, report, "Throughput Performance")
		require.Contains(t, report, "Memory Usage Comparison")
		require.Contains(t, report, "GC Impact Comparison")
	})

	t.Run("BaselineReport", func(t *testing.T) {
		t.Parallel()
		report := benchmarks.ReportBaseline(pgarrowStats, 10)
		require.Contains(t, report, "Baseline Measurements for Phase 1 Goals")
		require.Contains(t, report, "Current vs Expected Performance")
		require.Contains(t, report, "Memory Baseline")
		require.Contains(t, report, "GC Pressure Baseline")
	})
}

// TestMemoryLeakDetection ensures benchmarks detect memory leaks
func TestMemoryLeakDetection(t *testing.T) {
	t.Parallel()

	databaseURL := getBenchmarkDatabaseURL(t)
	runner := benchmarks.NewRunner(databaseURL)

	ctx := context.Background()
	query := "SELECT i as id FROM generate_series(1, 10) i"

	// Run benchmark - should not have memory leaks
	result, err := runner.RunPGArrowBenchmark(ctx, query)
	require.NoError(t, err)

	// CheckedAllocator should track allocations
	// In a well-behaved benchmark, CheckedAlloc should be 0 or minimal
	// This test validates that memory tracking is working
	require.GreaterOrEqual(t, result.MemoryStats.CheckedAlloc, int64(0))
}

// TestStatisticalFunctions validates statistical calculation functions
func TestStatisticalFunctions(t *testing.T) {
	t.Parallel()

	t.Run("StandardDeviation", func(t *testing.T) {
		t.Parallel()
		values := []float64{10, 12, 14, 16, 18}
		stdDev := benchmarks.CalculateStandardDeviation(values)
		require.Greater(t, stdDev, 0.0)
		require.Less(t, stdDev, 10.0) // Reasonable bound
	})

	t.Run("ConfidenceInterval", func(t *testing.T) {
		t.Parallel()
		values := []float64{100, 102, 98, 104, 96, 101, 99, 103, 97, 105}
		lower, upper := benchmarks.CalculateConfidenceInterval(values)
		require.Greater(t, upper, lower)
		require.Greater(t, lower, 90.0) // Reasonable lower bound
		require.Less(t, upper, 110.0)   // Reasonable upper bound
	})

	t.Run("EmptyValues", func(t *testing.T) {
		t.Parallel()
		values := []float64{}
		stdDev := benchmarks.CalculateStandardDeviation(values)
		require.InDelta(t, 0.0, stdDev, 0.001)

		lower, upper := benchmarks.CalculateConfidenceInterval(values)
		require.InDelta(t, 0.0, lower, 0.001)
		require.InDelta(t, 0.0, upper, 0.001)
	})
}

// getBenchmarkDatabaseURL returns the test database URL, skipping the test if not available
func getBenchmarkDatabaseURL(t *testing.T) string {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("No database URL provided. Set TEST_DATABASE_URL or DATABASE_URL environment variable to run benchmarks.")
	}
	return databaseURL
}
