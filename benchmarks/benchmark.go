package benchmarks

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/fwojciec/pgarrow"
	"github.com/jackc/pgx/v5"
)

// BenchmarkResult holds comprehensive performance metrics
type BenchmarkResult struct {
	Name          string
	Duration      time.Duration
	TotalRows     int64
	ThroughputRPS float64
	BatchCount    int64
	AvgBatchSize  float64
	MemoryStats   MemoryMetrics
	GCStats       GCMetrics
}

// MemoryMetrics tracks memory allocation patterns
type MemoryMetrics struct {
	AllocsBefore   uint64
	AllocsAfter    uint64
	TotalAllocs    uint64
	BytesAllocated uint64
	PeakAlloc      uint64
	HeapInUse      uint64
	HeapSys        uint64
	CheckedAlloc   int64 // From memory.CheckedAllocator
}

// GCMetrics tracks garbage collection impact
type GCMetrics struct {
	GCRunsBefore  uint32
	GCRunsAfter   uint32
	TotalGCRuns   uint32
	GCPauseNs     uint64
	GCCPUFraction float64
}

// StatisticalResult aggregates multiple benchmark runs
type StatisticalResult struct {
	Name          string
	Runs          int
	AvgDuration   time.Duration
	MinDuration   time.Duration
	MaxDuration   time.Duration
	AvgThroughput float64
	MinThroughput float64
	MaxThroughput float64
	AvgBatchSize  float64
	AvgBatchCount int64
	AvgMemory     MemoryMetrics
	AvgGC         GCMetrics
}

// ComparisonResult holds both PGArrow and naive benchmark statistics
type ComparisonResult struct {
	PGArrow StatisticalResult
	Naive   StatisticalResult
}

// Runner executes performance benchmarks with statistical rigor
type Runner struct {
	DatabaseURL string
	Allocator   memory.Allocator
}

// NewRunner creates a new benchmark runner
func NewRunner(databaseURL string) *Runner {
	return &Runner{
		DatabaseURL: databaseURL,
		Allocator:   memory.NewCheckedAllocator(memory.DefaultAllocator),
	}
}

// RunPGArrowBenchmark executes PGArrow streaming benchmark
func (r *Runner) RunPGArrowBenchmark(ctx context.Context, query string) (BenchmarkResult, error) {
	// Setup benchmark environment
	beforeMem, beforeGC, allocBefore, startTime, err := r.setupBenchmarkEnvironment()
	if err != nil {
		return BenchmarkResult{}, err
	}

	// Create PGArrow pool
	pool, err := pgarrow.NewPool(ctx, r.DatabaseURL, pgarrow.WithAllocator(r.Allocator))
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("failed to create PGArrow pool: %w", err)
	}
	defer pool.Close()

	// Execute streaming query
	totalRows, batchCount, err := r.streamArrowData(ctx, pool, query)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("failed to stream data: %w", err)
	}

	// Finalize metrics
	return r.finalizeBenchmarkMetrics("PGArrow Streaming", beforeMem, beforeGC, allocBefore, startTime, totalRows, batchCount)
}

// RunNaivePgxBenchmark executes naive pgx→Arrow comparison benchmark
func (r *Runner) RunNaivePgxBenchmark(ctx context.Context, query string) (BenchmarkResult, error) {
	// Setup benchmark environment (without CheckedAllocator requirement)
	runtime.GC()
	runtime.GC()
	beforeMem := getMemoryStats()
	beforeGC := getGCStats()
	startTime := time.Now()

	// Create pgx connection
	conn, err := pgx.Connect(ctx, r.DatabaseURL)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("failed to connect with pgx: %w", err)
	}
	defer conn.Close(ctx)

	// Execute naive query and convert to Arrow
	totalRows, err := r.queryAndConvertToArrow(ctx, conn, query)
	if err != nil {
		return BenchmarkResult{}, fmt.Errorf("failed to query and convert: %w", err)
	}

	// Calculate final metrics
	return r.calculateNaiveMetrics("Naive pgx→Arrow", beforeMem, beforeGC, startTime, totalRows), nil
}

// streamArrowData streams data using PGArrow and returns metrics
func (r *Runner) streamArrowData(ctx context.Context, pool *pgarrow.Pool, query string) (totalRows, batchCount int64, err error) {
	reader, err := pool.QueryArrow(ctx, query)
	if err != nil {
		return 0, 0, fmt.Errorf("QueryArrow failed: %w", err)
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()
		batchCount++
	}

	if err := reader.Err(); err != nil {
		return 0, 0, fmt.Errorf("reader error: %w", err)
	}

	return totalRows, batchCount, nil
}

// queryAndConvertToArrow performs naive pgx query with manual Arrow conversion
func (r *Runner) queryAndConvertToArrow(ctx context.Context, conn *pgx.Conn, query string) (int64, error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Create Arrow schema and builders
	schema := createArrowSchema(rows.FieldDescriptions())
	pool := memory.NewGoAllocator()
	builders := createStringBuilders(pool, len(schema.Fields()))
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()

	// Convert rows to Arrow format
	totalRows, err := convertRowsToArrow(rows, builders)
	if err != nil {
		return 0, fmt.Errorf("failed to convert rows: %w", err)
	}

	// Build final record (to simulate full conversion)
	record, err := buildArrowRecord(schema, builders, totalRows)
	if err != nil {
		return 0, fmt.Errorf("failed to build record: %w", err)
	}
	defer record.Release()

	return totalRows, nil
}

// getMemoryStats captures current memory statistics
func getMemoryStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}

// getGCStats captures current GC statistics
func getGCStats() runtime.MemStats {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m
}
