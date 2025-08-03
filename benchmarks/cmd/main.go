package main

import (
	"context"
	"fmt"
	"log"

	"github.com/fwojciec/pgarrow/benchmarks"
)

func main() {
	config := parseConfig()
	config.printIntro()

	// Create benchmark runner
	runner := benchmarks.NewRunner(config.DatabaseURL)
	ctx := context.Background()

	// Run statistical benchmark
	comparisonResult, err := runner.RunStatisticalBenchmark(ctx, config.generateQuery(), config.Runs)
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	// Generate appropriate report
	if config.Baseline {
		fmt.Print(benchmarks.ReportBaseline(comparisonResult.PGArrow, config.Rows))
	} else if config.Verbose {
		// Show detailed comparison in verbose mode
		fmt.Print(benchmarks.ReportComparison(comparisonResult.PGArrow, comparisonResult.Naive))
	} else {
		// Show primary PGArrow results
		fmt.Print(comparisonResult.PGArrow.ReportResult())
	}

	// Performance validation and summary
	validatePerformance(comparisonResult.PGArrow.AvgThroughput)
	config.printSummary(comparisonResult.PGArrow.AvgThroughput)
}
