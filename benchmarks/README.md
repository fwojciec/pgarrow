# PGArrow Performance Measurement Framework

Standardized performance measurement framework for tracking progress during the High-Performance Architecture Overhaul (#63) and validating performance improvements.

## Quick Start

```bash
# Set database connection
export DATABASE_URL="postgres://user:pass@localhost:5432/dbname"

# Run benchmarks with default settings (5 runs, 100K rows)
cd benchmarks/cmd
go run main.go

# Generate baseline measurements for Phase 1 goals
go run main.go -baseline -rows 5000000 -runs 5

# Custom benchmark parameters with verbose comparison output
go run main.go -runs 10 -rows 1000000 -v
```

## Framework Architecture

### Core Components

- **`benchmark.go`** - Core benchmarking engine with PGArrow vs naive pgx→Arrow comparison
- **`statistics.go`** - Statistical analysis framework for multiple runs and confidence intervals  
- **`reporting.go`** - Comprehensive performance reporting with baseline comparisons
- **`benchmark_test.go`** - Validation tests ensuring measurement accuracy
- **`cmd/main.go`** - Command-line interface for running benchmarks

### Key Metrics Tracked

**Throughput & Duration:**
- Rows processed per second (target: >177K rows/sec from issue #65)
- Total execution time
- Batch count and average batch size

**Memory Analysis:**
- Total allocations and bytes allocated  
- Peak memory usage and heap utilization
- `memory.CheckedAllocator` tracking for leak detection

**GC Pressure:**
- Garbage collection run count
- GC pause time impact
- CPU fraction consumed by GC

## Usage Examples

### Statistical Benchmarking

```go
runner := benchmarks.NewRunner(databaseURL)
comparison, err := runner.RunStatisticalBenchmark(ctx, query, 5)
if err != nil {
    log.Fatal(err)
}

// Show PGArrow results
fmt.Print(comparison.PGArrow.ReportResult())

// Show detailed comparison
fmt.Print(benchmarks.ReportComparison(comparison.PGArrow, comparison.Naive))
```

### Individual Benchmark Runs

```go
// Run individual benchmarks
pgarrowResult, _ := runner.RunPGArrowBenchmark(ctx, query)
naiveResult, _ := runner.RunNaivePgxBenchmark(ctx, query)

// Manual comparison reporting
comparison := benchmarks.ReportComparison(pgarrowStats, naiveStats)
fmt.Print(comparison)
```

### Baseline Measurement

```go
baseline := benchmarks.ReportBaseline(comparison.PGArrow, 5000000)
fmt.Print(baseline) // Compare against 28.2s, 177K rows/sec from issue context
```

## Integration with Test Database

Uses the same database setup pattern as `pgarrow-test`:

```sql
-- Test data generation (compatible with pgarrow-test schema)
SELECT 
    id::int8 as id,
    (random() * 100)::float8 as score,
    (random() > 0.5)::bool as active,
    'User_' || id::text as name,
    CURRENT_DATE + (random() * 365)::int as created_date
FROM generate_series(1, 5000000) as id
ORDER BY id
```

## GitHub Actions Integration

Performance regression detection runs automatically:

```yaml
# Triggered on: push to main, PRs, manual dispatch
- name: Performance Regression Check
  run: |
    OUTPUT=$(go run main.go -runs 3 -rows 1000)
    if [ $? -eq 1 ]; then
      echo "❌ Performance regression detected!"
      exit 1
    fi
```

**Regression Thresholds:**
- Fails if throughput drops below 50% of expected baseline (88.5K rows/sec)
- Uploads performance artifacts for analysis
- Supports manual baseline generation workflow

## Statistical Rigor

**Multiple Run Analysis:**
- Configurable number of benchmark runs (default: 5)
- Mean, min, max calculations for all metrics
- Standard deviation and 95% confidence intervals
- Outlier detection and handling

**Memory Safety:**
- `memory.CheckedAllocator` integration detects leaks
- Runtime GC forcing for clean measurement baselines
- Comprehensive memory metrics across benchmark lifecycle

## Performance Baselines

### Current Performance (Issue #65 Context)
- **Duration**: 28.2s for 5M rows
- **Throughput**: 177K rows/sec  
- **GC Runs**: 435
- **Batch Count**: 39
- **Comparison**: 10.3x slower than naive pgx→Arrow

### Target Improvements
Framework enables tracking progress toward:
- Reduced GC pressure through optimized memory layouts
- Higher throughput via streaming optimizations
- Lower memory usage through zero-copy techniques
- Better batch size optimization

## Command Line Options

```bash
Usage: go run main.go [options]

-db string
    Database URL (or set DATABASE_URL env var)
-runs int
    Number of benchmark runs for statistical analysis (default 5)
-rows int
    Number of rows to benchmark using generate_series (default 100000)
-v  Verbose output with detailed PGArrow vs Naive comparison
-baseline
    Generate baseline measurements for Phase 1 goals
```

## Testing

```bash
# Run benchmark validation tests
go test -v ./benchmarks/...

# Run with database (requires TEST_DATABASE_URL)
export TEST_DATABASE_URL="postgres://..."
go test -v ./benchmarks/...

# Skip long-running statistical tests
go test -short ./benchmarks/...
```

## Integration with Existing Benchmarks

Complements existing benchmarks in the root package:
- **Root benchmarks** (`pgarrow_bench_test.go`) - Go's built-in benchmarking for micro-optimizations
- **This framework** - End-to-end performance measurement with statistical analysis
- **pgarrow-test** - Large-scale performance validation (5M+ rows)

Use this framework for:
- ✅ Architecture overhaul progress tracking
- ✅ Performance regression detection in CI
- ✅ Statistical analysis of improvements
- ✅ Baseline measurement documentation

Use root benchmarks for:
- ✅ Column writer micro-optimizations  
- ✅ Memory layout optimization validation
- ✅ Individual component performance tuning