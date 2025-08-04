# Experimental DirectV3 Implementation

This directory contains the DirectV3 parser that achieved **19x performance improvement** over the original implementation.

## Performance Metrics

- **Speed**: 3.5M rows/sec (vs 183K original)
- **GC Runs**: 9 for 5M rows (vs 170 original)
- **Memory**: 522MB for 5M rows

## Key Innovation

The breakthrough came from eliminating goroutine synchronization overhead by directly processing COPY protocol messages instead of using `io.Pipe()`.

### The Problem
The original implementation used:
```go
reader, writer := io.Pipe()
go func() {
    conn.CopyTo(ctx, writer, copyQuery)
}()
// Parse from reader in main goroutine
```

This caused **74% of CPU time** to be spent in thread synchronization (`pthread_cond_signal`, `usleep`, `pthread_cond_wait`).

### The Solution
DirectV3 processes COPY messages directly:
```go
conn.Frontend().SendQuery(&pgproto3.Query{String: copyQuery})
for {
    msg := conn.ReceiveMessage(ctx)
    switch msg := msg.(type) {
    case *pgproto3.CopyData:
        // Process data directly, no goroutines!
        processCopyData(msg.Data)
    }
}
```

## Running the Benchmark

### Prerequisites

Set the database connection URL via environment variable:
```bash
export PGARROW_TEST_DB_URL="postgres://bookscanner:bookscanner@localhost:5432/bookscanner?sslmode=disable"
```

If not set, tests will use the default connection string above and log a warning.

### Running Tests

```bash
# Ensure the test database is set up
psql -U bookscanner -d bookscanner -f /Users/filip/code/go/pgarrow-test/setup.sql

# Run the performance test (requires integration tag)
go test -v -tags integration -run TestDirectV3PerformanceMetrics ./experimental

# Run the benchmark (requires integration tag)
go test -tags integration -bench BenchmarkDirectV3Performance ./experimental
```

## Implementation Details

See `direct_copy_parser_v3.go` for the complete implementation.

Key components:
- `CopyScanPlan` interface for type-specific parsing
- Direct binary to Arrow parsing (single pass)
- Pre-allocated builders with 100k capacity
- Buffer management for partial COPY messages

## Status

This is a reference implementation for issue #73. The production implementation will integrate this approach into the main codebase while maintaining API compatibility.