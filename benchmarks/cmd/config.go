package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

type Config struct {
	DatabaseURL string
	Runs        int
	Rows        int64
	Verbose     bool
	Baseline    bool
}

func parseConfig() *Config {
	var (
		databaseURL = flag.String("db", "", "Database URL (or set DATABASE_URL env var)")
		runs        = flag.Int("runs", 5, "Number of benchmark runs for statistical analysis")
		rows        = flag.Int64("rows", 100000, "Number of rows to benchmark (using generate_series)")
		verbose     = flag.Bool("v", false, "Verbose output")
		baseline    = flag.Bool("baseline", false, "Generate baseline measurements for Phase 1 goals")
	)
	flag.Parse()

	// Get database URL
	dbURL := *databaseURL
	if dbURL == "" {
		dbURL = os.Getenv("DATABASE_URL")
	}
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		log.Fatal("Database URL required. Use -db flag or set DATABASE_URL environment variable.")
	}

	return &Config{
		DatabaseURL: dbURL,
		Runs:        *runs,
		Rows:        *rows,
		Verbose:     *verbose,
		Baseline:    *baseline,
	}
}

func (c *Config) generateQuery() string {
	return fmt.Sprintf(`
		SELECT 
			id::int8 as id,
			(random() * 100)::float8 as score,
			(random() > 0.5)::bool as active,
			'User_' || id::text as name,
			CURRENT_DATE + (random() * 365)::int as created_date
		FROM generate_series(1, %d) as id
		ORDER BY id
	`, c.Rows)
}

func (c *Config) printIntro() {
	if c.Verbose {
		fmt.Printf("Running benchmark with %d rows, %d statistical runs\n", c.Rows, c.Runs)
		fmt.Printf("Database: %s\n", c.DatabaseURL)
		fmt.Println()
	}
}

func (c *Config) printSummary(avgThroughput float64) {
	expectedThroughput := 177000.0 // 177K rows/sec from issue #65

	if c.Verbose {
		fmt.Printf("\n✅ Benchmark completed successfully\n")
		fmt.Printf("Throughput: %.1fK rows/sec (%.2fx vs expected %.1fK)\n",
			avgThroughput/1000,
			avgThroughput/expectedThroughput,
			expectedThroughput/1000)
	}
}

func validatePerformance(avgThroughput float64) {
	expectedThroughput := 177000.0 // 177K rows/sec from issue #65
	if avgThroughput < expectedThroughput*0.5 {
		fmt.Printf("\n⚠️  WARNING: Performance significantly below expected baseline (%.1fK rows/sec vs %.1fK expected)\n",
			avgThroughput/1000, expectedThroughput/1000)
		os.Exit(1)
	}
}
