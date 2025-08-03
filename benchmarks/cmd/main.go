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
	result, err := runner.RunStatisticalBenchmark(ctx, config.generateQuery(), config.Runs)
	if err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	// Generate appropriate report
	if config.Baseline {
		fmt.Print(benchmarks.ReportBaseline(result, config.Rows))
	} else {
		fmt.Print(result.ReportResult())
	}

	// Performance validation and summary
	validatePerformance(result.AvgThroughput)
	config.printSummary(result.AvgThroughput)
}
