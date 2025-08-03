package benchmarks

import (
	"fmt"
	"strings"
	"time"
)

// ReportResult generates a comprehensive performance report
func (r StatisticalResult) ReportResult() string {
	var report strings.Builder

	report.WriteString("=== Performance Measurement Results ===\n\n")
	report.WriteString(fmt.Sprintf("Benchmark: %s\n", r.Name))
	report.WriteString(fmt.Sprintf("Sample Size: %d runs\n\n", r.Runs))

	// Throughput metrics
	report.WriteString("--- Throughput (rows/sec) ---\n")
	report.WriteString(fmt.Sprintf("Average: %s rows/sec\n", formatNumber(r.AvgThroughput)))
	report.WriteString(fmt.Sprintf("Range: %s - %s rows/sec\n",
		formatNumber(r.MinThroughput), formatNumber(r.MaxThroughput)))

	// Duration metrics
	report.WriteString("\n--- Duration ---\n")
	report.WriteString(fmt.Sprintf("Average: %v\n", r.AvgDuration))
	report.WriteString(fmt.Sprintf("Range: %v - %v\n", r.MinDuration, r.MaxDuration))

	// Memory metrics
	report.WriteString("\n--- Memory Usage ---\n")
	report.WriteString(fmt.Sprintf("Total Allocations: %s\n", formatBytes(r.AvgMemory.TotalAllocs)))
	report.WriteString(fmt.Sprintf("Bytes Allocated: %s\n", formatBytes(r.AvgMemory.BytesAllocated)))
	report.WriteString(fmt.Sprintf("Peak Allocation: %s\n", formatBytes(r.AvgMemory.PeakAlloc)))
	report.WriteString(fmt.Sprintf("Heap In Use: %s\n", formatBytes(r.AvgMemory.HeapInUse)))
	if r.AvgMemory.CheckedAlloc != 0 {
		report.WriteString(fmt.Sprintf("Checked Allocator: %s\n", formatBytes(uint64(r.AvgMemory.CheckedAlloc))))
	}

	// GC metrics
	report.WriteString("\n--- Garbage Collection ---\n")
	report.WriteString(fmt.Sprintf("GC Runs: %d\n", r.AvgGC.TotalGCRuns))
	report.WriteString(fmt.Sprintf("GC Pause Time: %v\n", time.Duration(r.AvgGC.GCPauseNs)))
	report.WriteString(fmt.Sprintf("GC CPU Fraction: %.4f%%\n", r.AvgGC.GCCPUFraction*100))

	return report.String()
}

// ReportComparison generates a side-by-side comparison report
func ReportComparison(pgarrow, naive StatisticalResult) string {
	var report strings.Builder

	report.WriteString("=== PGArrow vs Naive pgx→Arrow Comparison ===\n\n")

	// Throughput comparison
	throughputRatio := pgarrow.AvgThroughput / naive.AvgThroughput
	report.WriteString("--- Throughput Performance ---\n")
	report.WriteString(fmt.Sprintf("PGArrow:    %s rows/sec\n", formatNumber(pgarrow.AvgThroughput)))
	report.WriteString(fmt.Sprintf("Naive pgx:  %s rows/sec\n", formatNumber(naive.AvgThroughput)))
	if throughputRatio > 1 {
		report.WriteString(fmt.Sprintf("PGArrow is %.2fx faster\n", throughputRatio))
	} else {
		report.WriteString(fmt.Sprintf("PGArrow is %.2fx slower\n", 1/throughputRatio))
	}

	// Duration comparison
	durationRatio := float64(pgarrow.AvgDuration) / float64(naive.AvgDuration)
	report.WriteString("\n--- Duration Comparison ---\n")
	report.WriteString(fmt.Sprintf("PGArrow:   %v\n", pgarrow.AvgDuration))
	report.WriteString(fmt.Sprintf("Naive pgx: %v\n", naive.AvgDuration))
	if durationRatio < 1 {
		report.WriteString(fmt.Sprintf("PGArrow is %.1f%% faster\n", (1-durationRatio)*100))
	} else {
		report.WriteString(fmt.Sprintf("PGArrow is %.1f%% slower\n", (durationRatio-1)*100))
	}

	// Memory comparison
	memoryRatio := float64(pgarrow.AvgMemory.BytesAllocated) / float64(naive.AvgMemory.BytesAllocated)
	report.WriteString("\n--- Memory Usage Comparison ---\n")
	report.WriteString(fmt.Sprintf("PGArrow:   %s\n", formatBytes(pgarrow.AvgMemory.BytesAllocated)))
	report.WriteString(fmt.Sprintf("Naive pgx: %s\n", formatBytes(naive.AvgMemory.BytesAllocated)))
	if memoryRatio < 1 {
		report.WriteString(fmt.Sprintf("PGArrow uses %.1f%% less memory\n", (1-memoryRatio)*100))
	} else {
		report.WriteString(fmt.Sprintf("PGArrow uses %.1f%% more memory\n", (memoryRatio-1)*100))
	}

	// GC comparison
	report.WriteString("\n--- GC Impact Comparison ---\n")
	report.WriteString(fmt.Sprintf("PGArrow GC runs:   %d\n", pgarrow.AvgGC.TotalGCRuns))
	report.WriteString(fmt.Sprintf("Naive pgx GC runs: %d\n", naive.AvgGC.TotalGCRuns))

	// Handle division by zero gracefully
	if naive.AvgGC.TotalGCRuns == 0 && pgarrow.AvgGC.TotalGCRuns == 0 {
		report.WriteString("Both approaches had no GC runs\n")
	} else if naive.AvgGC.TotalGCRuns == 0 {
		report.WriteString(fmt.Sprintf("PGArrow triggered %d GC runs vs 0 for naive\n", pgarrow.AvgGC.TotalGCRuns))
	} else if pgarrow.AvgGC.TotalGCRuns == 0 {
		report.WriteString(fmt.Sprintf("PGArrow had no GC runs vs %d for naive\n", naive.AvgGC.TotalGCRuns))
	} else {
		gcRatio := float64(pgarrow.AvgGC.TotalGCRuns) / float64(naive.AvgGC.TotalGCRuns)
		if gcRatio < 1 {
			report.WriteString(fmt.Sprintf("PGArrow has %.1f%% fewer GC runs\n", (1-gcRatio)*100))
		} else {
			report.WriteString(fmt.Sprintf("PGArrow has %.1f%% more GC runs\n", (gcRatio-1)*100))
		}
	}

	return report.String()
}

// ReportBaseline generates baseline measurements for Phase 1 goals
func ReportBaseline(result StatisticalResult, expectedRows int64) string {
	var report strings.Builder

	report.WriteString("=== Baseline Measurements for Phase 1 Goals ===\n\n")
	report.WriteString(fmt.Sprintf("Dataset: %s rows\n", formatNumber(float64(expectedRows))))
	report.WriteString(fmt.Sprintf("Runs: %d\n\n", result.Runs))

	// Current performance vs issue context (28.2s, 177K rows/sec)
	expectedThroughput := 177000.0 // 177K rows/sec from issue
	expectedDuration := time.Duration(float64(expectedRows)/expectedThroughput) * time.Second

	report.WriteString("--- Current vs Expected Performance ---\n")
	report.WriteString(fmt.Sprintf("Current Throughput: %s rows/sec\n", formatNumber(result.AvgThroughput)))
	report.WriteString(fmt.Sprintf("Expected Throughput: %s rows/sec\n", formatNumber(expectedThroughput)))

	throughputRatio := result.AvgThroughput / expectedThroughput
	if throughputRatio > 1 {
		report.WriteString(fmt.Sprintf("Performance is %.2fx better than expected\n", throughputRatio))
	} else {
		report.WriteString(fmt.Sprintf("Performance is %.2fx slower than expected\n", 1/throughputRatio))
	}

	report.WriteString(fmt.Sprintf("\nCurrent Duration: %v\n", result.AvgDuration))
	report.WriteString(fmt.Sprintf("Expected Duration: %v\n", expectedDuration))

	// Batch analysis
	report.WriteString("\n--- Batch Analysis ---\n")
	report.WriteString(fmt.Sprintf("Average Batch Size: %.0f rows\n", result.AvgBatchSize))
	report.WriteString(fmt.Sprintf("Average Batch Count: %d batches\n", result.AvgBatchCount))

	// Memory baseline
	report.WriteString("\n--- Memory Baseline ---\n")
	report.WriteString(fmt.Sprintf("Peak Allocation: %s\n", formatBytes(result.AvgMemory.PeakAlloc)))
	report.WriteString(fmt.Sprintf("Total Allocations: %s\n", formatBytes(result.AvgMemory.TotalAllocs)))
	report.WriteString(fmt.Sprintf("Heap Usage: %s\n", formatBytes(result.AvgMemory.HeapInUse)))

	// GC baseline
	report.WriteString("\n--- GC Pressure Baseline ---\n")
	report.WriteString(fmt.Sprintf("Average GC Runs: %d\n", result.AvgGC.TotalGCRuns))
	report.WriteString(fmt.Sprintf("GC Pause Time: %v\n", time.Duration(result.AvgGC.GCPauseNs)))
	report.WriteString(fmt.Sprintf("GC CPU Impact: %.4f%%\n", result.AvgGC.GCCPUFraction*100))

	return report.String()
}

// formatNumber formats large numbers with appropriate units
func formatNumber(n float64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", n/1000000)
	} else if n >= 1000 {
		return fmt.Sprintf("%.1fK", n/1000)
	}
	return fmt.Sprintf("%.0f", n)
}

// formatBytes formats byte counts with appropriate units
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
