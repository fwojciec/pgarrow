package benchmarks

import (
	"fmt"
	"runtime"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// setupBenchmarkEnvironment prepares benchmark environment and captures initial state
func (r *Runner) setupBenchmarkEnvironment() (beforeMem runtime.MemStats, beforeGC runtime.MemStats, allocBefore int64, startTime time.Time, err error) {
	// Force GC to establish clean baseline
	runtime.GC()
	runtime.GC()

	// Capture initial state
	beforeMem = getMemoryStats()
	beforeGC = getGCStats()

	checkedAlloc, ok := r.Allocator.(*memory.CheckedAllocator)
	if !ok {
		return beforeMem, beforeGC, 0, time.Time{}, fmt.Errorf("allocator is not CheckedAllocator")
	}
	allocBefore = int64(checkedAlloc.CurrentAlloc())
	startTime = time.Now()

	return beforeMem, beforeGC, allocBefore, startTime, nil
}

// finalizeBenchmarkMetrics calculates final metrics after benchmark completion
func (r *Runner) finalizeBenchmarkMetrics(name string, beforeMem, beforeGC runtime.MemStats, allocBefore int64, startTime time.Time, totalRows, batchCount int64) (BenchmarkResult, error) {
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Capture final state
	afterMem := getMemoryStats()
	afterGC := getGCStats()

	checkedAlloc, ok := r.Allocator.(*memory.CheckedAllocator)
	if !ok {
		return BenchmarkResult{}, fmt.Errorf("allocator is not CheckedAllocator")
	}
	allocAfter := int64(checkedAlloc.CurrentAlloc())

	// Calculate metrics
	throughput := float64(totalRows) / duration.Seconds()
	avgBatchSize := float64(totalRows) / float64(batchCount)

	memMetrics := MemoryMetrics{
		AllocsBefore:   beforeMem.Mallocs,
		AllocsAfter:    afterMem.Mallocs,
		TotalAllocs:    afterMem.Mallocs - beforeMem.Mallocs,
		BytesAllocated: afterMem.TotalAlloc - beforeMem.TotalAlloc,
		PeakAlloc:      afterMem.HeapAlloc,
		HeapInUse:      afterMem.HeapInuse,
		HeapSys:        afterMem.HeapSys,
		CheckedAlloc:   allocAfter - allocBefore,
	}

	gcMetrics := GCMetrics{
		GCRunsBefore:  beforeGC.NumGC,
		GCRunsAfter:   afterGC.NumGC,
		TotalGCRuns:   afterGC.NumGC - beforeGC.NumGC,
		GCPauseNs:     afterGC.PauseTotalNs - beforeGC.PauseTotalNs,
		GCCPUFraction: afterGC.GCCPUFraction,
	}

	return BenchmarkResult{
		Name:          name,
		Duration:      duration,
		TotalRows:     totalRows,
		ThroughputRPS: throughput,
		BatchCount:    batchCount,
		AvgBatchSize:  avgBatchSize,
		MemoryStats:   memMetrics,
		GCStats:       gcMetrics,
	}, nil
}

// createArrowSchema builds Arrow schema from pgx field descriptions
func createArrowSchema(fieldDescriptions []pgconn.FieldDescription) *arrow.Schema {
	fields := make([]arrow.Field, len(fieldDescriptions))
	for i, fd := range fieldDescriptions {
		// Simplified type mapping - in practice this would be more complex
		var dataType arrow.DataType = arrow.BinaryTypes.String
		fields[i] = arrow.Field{Name: fd.Name, Type: dataType}
	}
	return arrow.NewSchema(fields, nil)
}

// createStringBuilders creates string builders for naive Arrow conversion
func createStringBuilders(pool memory.Allocator, fieldCount int) []array.Builder {
	builders := make([]array.Builder, fieldCount)
	for i := range builders {
		builders[i] = array.NewStringBuilder(pool)
	}
	return builders
}

// convertRowsToArrow processes pgx rows and converts to Arrow format
func convertRowsToArrow(rows pgx.Rows, builders []array.Builder) (int64, error) {
	var totalRows int64

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, fmt.Errorf("scan failed: %w", err)
		}

		// Convert values to string builders (simplified)
		for i, val := range values {
			strBuilder, ok := builders[i].(*array.StringBuilder)
			if !ok {
				return 0, fmt.Errorf("builder is not StringBuilder")
			}
			if val == nil {
				strBuilder.AppendNull()
			} else {
				strBuilder.Append(fmt.Sprintf("%v", val))
			}
		}
		totalRows++
	}

	return totalRows, nil
}

// buildArrowRecord creates final Arrow record from builders
func buildArrowRecord(schema *arrow.Schema, builders []array.Builder, totalRows int64) (arrow.Record, error) {
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	record := array.NewRecord(schema, arrays, totalRows)
	return record, nil
}

// calculateNaiveMetrics calculates metrics for naive benchmark (without CheckedAllocator)
func (r *Runner) calculateNaiveMetrics(name string, beforeMem, beforeGC runtime.MemStats, startTime time.Time, totalRows int64) BenchmarkResult {
	endTime := time.Now()
	duration := endTime.Sub(startTime)

	// Capture final state
	afterMem := getMemoryStats()
	afterGC := getGCStats()

	// Calculate metrics
	throughput := float64(totalRows) / duration.Seconds()

	memMetrics := MemoryMetrics{
		AllocsBefore:   beforeMem.Mallocs,
		AllocsAfter:    afterMem.Mallocs,
		TotalAllocs:    afterMem.Mallocs - beforeMem.Mallocs,
		BytesAllocated: afterMem.TotalAlloc - beforeMem.TotalAlloc,
		PeakAlloc:      afterMem.HeapAlloc,
		HeapInUse:      afterMem.HeapInuse,
		HeapSys:        afterMem.HeapSys,
	}

	gcMetrics := GCMetrics{
		GCRunsBefore:  beforeGC.NumGC,
		GCRunsAfter:   afterGC.NumGC,
		TotalGCRuns:   afterGC.NumGC - beforeGC.NumGC,
		GCPauseNs:     afterGC.PauseTotalNs - beforeGC.PauseTotalNs,
		GCCPUFraction: afterGC.GCCPUFraction,
	}

	return BenchmarkResult{
		Name:          name,
		Duration:      duration,
		TotalRows:     totalRows,
		ThroughputRPS: throughput,
		BatchCount:    1, // Naive approach uses single batch
		AvgBatchSize:  float64(totalRows),
		MemoryStats:   memMetrics,
		GCStats:       gcMetrics,
	}
}
