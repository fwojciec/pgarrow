package benchmarks

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"
)

// RunStatisticalBenchmark executes multiple benchmark runs and provides statistical analysis
func (r *Runner) RunStatisticalBenchmark(ctx context.Context, query string, runs int) (StatisticalResult, error) {
	if runs < 1 {
		return StatisticalResult{}, fmt.Errorf("runs must be at least 1, got %d", runs)
	}

	pgarrowResults := make([]BenchmarkResult, runs)
	naiveResults := make([]BenchmarkResult, runs)

	// Execute PGArrow benchmarks
	for i := 0; i < runs; i++ {
		result, err := r.RunPGArrowBenchmark(ctx, query)
		if err != nil {
			return StatisticalResult{}, fmt.Errorf("PGArrow run %d failed: %w", i+1, err)
		}
		pgarrowResults[i] = result
	}

	// Execute naive benchmarks
	for i := 0; i < runs; i++ {
		result, err := r.RunNaivePgxBenchmark(ctx, query)
		if err != nil {
			return StatisticalResult{}, fmt.Errorf("naive run %d failed: %w", i+1, err)
		}
		naiveResults[i] = result
	}

	// Calculate statistics
	pgarrowStats := calculateStatistics("PGArrow", pgarrowResults)
	_ = calculateStatistics("Naive pgx→Arrow", naiveResults)

	// Return the primary measurement (PGArrow) with comparison data
	comparison := StatisticalResult{
		Name:          fmt.Sprintf("PGArrow vs Naive (n=%d)", runs),
		Runs:          runs,
		AvgDuration:   pgarrowStats.AvgDuration,
		MinDuration:   pgarrowStats.MinDuration,
		MaxDuration:   pgarrowStats.MaxDuration,
		AvgThroughput: pgarrowStats.AvgThroughput,
		MinThroughput: pgarrowStats.MinThroughput,
		MaxThroughput: pgarrowStats.MaxThroughput,
		AvgMemory:     pgarrowStats.AvgMemory,
		AvgGC:         pgarrowStats.AvgGC,
	}

	// Add comparison context in formatted output later
	return comparison, nil
}

// calculateStatistics computes comprehensive statistics from multiple benchmark runs
func calculateStatistics(name string, results []BenchmarkResult) StatisticalResult {
	if len(results) == 0 {
		return StatisticalResult{}
	}

	// Sort by duration for percentile calculations
	sortedResults := make([]BenchmarkResult, len(results))
	copy(sortedResults, results)
	sort.Slice(sortedResults, func(i, j int) bool {
		return sortedResults[i].Duration < sortedResults[j].Duration
	})

	// Duration statistics
	minDuration := sortedResults[0].Duration
	maxDuration := sortedResults[len(sortedResults)-1].Duration

	var totalDuration time.Duration
	for _, r := range results {
		totalDuration += r.Duration
	}
	avgDuration := totalDuration / time.Duration(len(results))

	// Throughput statistics
	throughputs := make([]float64, len(results))
	for i, r := range results {
		throughputs[i] = r.ThroughputRPS
	}
	sort.Float64s(throughputs)

	minThroughput := throughputs[0]
	maxThroughput := throughputs[len(throughputs)-1]

	var totalThroughput float64
	for _, t := range throughputs {
		totalThroughput += t
	}
	avgThroughput := totalThroughput / float64(len(throughputs))

	// Memory statistics (averages)
	avgMemory := calculateAverageMemoryMetrics(results)
	avgGC := calculateAverageGCMetrics(results)

	return StatisticalResult{
		Name:          name,
		Runs:          len(results),
		AvgDuration:   avgDuration,
		MinDuration:   minDuration,
		MaxDuration:   maxDuration,
		AvgThroughput: avgThroughput,
		MinThroughput: minThroughput,
		MaxThroughput: maxThroughput,
		AvgMemory:     avgMemory,
		AvgGC:         avgGC,
	}
}

// calculateAverageMemoryMetrics computes average memory metrics across runs
func calculateAverageMemoryMetrics(results []BenchmarkResult) MemoryMetrics {
	if len(results) == 0 {
		return MemoryMetrics{}
	}

	var (
		totalAllocsBefore   uint64
		totalAllocsAfter    uint64
		totalAllocs         uint64
		totalBytesAllocated uint64
		totalPeakAlloc      uint64
		totalHeapInUse      uint64
		totalHeapSys        uint64
		totalCheckedAlloc   int64
	)

	for _, r := range results {
		totalAllocsBefore += r.MemoryStats.AllocsBefore
		totalAllocsAfter += r.MemoryStats.AllocsAfter
		totalAllocs += r.MemoryStats.TotalAllocs
		totalBytesAllocated += r.MemoryStats.BytesAllocated
		totalPeakAlloc += r.MemoryStats.PeakAlloc
		totalHeapInUse += r.MemoryStats.HeapInUse
		totalHeapSys += r.MemoryStats.HeapSys
		totalCheckedAlloc += r.MemoryStats.CheckedAlloc
	}

	count := uint64(len(results))
	return MemoryMetrics{
		AllocsBefore:   totalAllocsBefore / count,
		AllocsAfter:    totalAllocsAfter / count,
		TotalAllocs:    totalAllocs / count,
		BytesAllocated: totalBytesAllocated / count,
		PeakAlloc:      totalPeakAlloc / count,
		HeapInUse:      totalHeapInUse / count,
		HeapSys:        totalHeapSys / count,
		CheckedAlloc:   totalCheckedAlloc / int64(count),
	}
}

// calculateAverageGCMetrics computes average GC metrics across runs
func calculateAverageGCMetrics(results []BenchmarkResult) GCMetrics {
	if len(results) == 0 {
		return GCMetrics{}
	}

	var (
		totalGCRunsBefore  uint32
		totalGCRunsAfter   uint32
		totalGCRuns        uint32
		totalGCPauseNs     uint64
		totalGCCPUFraction float64
	)

	for _, r := range results {
		totalGCRunsBefore += r.GCStats.GCRunsBefore
		totalGCRunsAfter += r.GCStats.GCRunsAfter
		totalGCRuns += r.GCStats.TotalGCRuns
		totalGCPauseNs += r.GCStats.GCPauseNs
		totalGCCPUFraction += r.GCStats.GCCPUFraction
	}

	count := float64(len(results))
	return GCMetrics{
		GCRunsBefore:  uint32(float64(totalGCRunsBefore) / count),
		GCRunsAfter:   uint32(float64(totalGCRunsAfter) / count),
		TotalGCRuns:   uint32(float64(totalGCRuns) / count),
		GCPauseNs:     uint64(float64(totalGCPauseNs) / count),
		GCCPUFraction: totalGCCPUFraction / count,
	}
}

// CalculateStandardDeviation computes standard deviation for throughput measurements
func CalculateStandardDeviation(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	// Calculate mean
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))

	// Calculate variance
	var varianceSum float64
	for _, v := range values {
		diff := v - mean
		varianceSum += diff * diff
	}
	variance := varianceSum / float64(len(values)-1)

	return math.Sqrt(variance)
}

// CalculateConfidenceInterval computes 95% confidence interval for throughput
func CalculateConfidenceInterval(values []float64) (lower, upper float64) {
	if len(values) < 2 {
		return 0, 0
	}

	sort.Float64s(values)
	n := len(values)

	// For small samples, use simple percentile approach
	if n < 30 {
		lowerIdx := int(0.025 * float64(n))
		upperIdx := int(0.975 * float64(n))
		if upperIdx >= n {
			upperIdx = n - 1
		}
		return values[lowerIdx], values[upperIdx]
	}

	// For larger samples, use standard error approach
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(n)

	stdDev := CalculateStandardDeviation(values)
	standardError := stdDev / math.Sqrt(float64(n))

	// 95% confidence interval (z = 1.96)
	margin := 1.96 * standardError
	return mean - margin, mean + margin
}
