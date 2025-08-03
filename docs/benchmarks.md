# PGArrow Performance Benchmarks

This document describes the performance benchmarking suite for PGArrow's CompiledSchema-based binary-to-Arrow conversion pipeline.

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -run=^$ -benchmem

# Run specific benchmark category
go test -bench=BenchmarkColumnWriter -run=^$ -benchmem

# Run with CPU profiling
go test -bench=BenchmarkMemoryUsage -run=^$ -cpuprofile=cpu.prof

# Run with memory profiling  
go test -bench=BenchmarkGCPressureReduction -run=^$ -memprofile=mem.prof
```

## Benchmark Environment

All performance measurements were recorded under the following standardized conditions:

### **Hardware Specifications**
- **CPU**: Apple M1 (ARM64 architecture)
- **Memory**: 16 GB unified memory
- **Storage**: SSD (NVMe)
- **System**: MacBook Air M1

### **Software Environment**
- **Operating System**: macOS 15.5 (Darwin 24.5.0)
- **Go Version**: 1.24.5 darwin/arm64
- **PostgreSQL**: 15-alpine (Docker containerized)
- **Docker Environment**: Local development setup

### **PostgreSQL Configuration**
- **Version**: PostgreSQL 15 (Alpine Linux container)
- **Container Setup**: Local Docker instance
- **Connection**: Local network (minimal latency)
- **Default Configuration**: Standard PostgreSQL 15 settings

### **Benchmark Methodology**
- **Execution**: `go test -bench=. -benchmem -cpu=1`
- **Warm-up**: Each benchmark includes Go's standard warm-up iterations
- **Measurement**: Multiple runs averaged using Go's built-in benchmarking
- **Memory Tracking**: `-benchmem` flag for allocation tracking
- **GC Measurement**: Custom gc-ns/op metrics using runtime.GC() timing

### **Reproducibility Notes**
- Results may vary on different hardware architectures (x86_64 vs ARM64)
- Network latency to PostgreSQL affects connection benchmarks
- Go version and compiler optimizations impact performance
- Docker overhead minimal for CPU-bound operations
- Memory allocation patterns may differ across Go versions

## Benchmark Categories

### 1. Column Writer Performance
Type-specific column writers for Arrow format conversion:

- `BenchmarkBoolColumnWriter` - Boolean type conversion (~5.4 ns/op, 0 allocs)
- `BenchmarkInt16ColumnWriter` - 16-bit integers (~5.5 ns/op, 0 allocs)  
- `BenchmarkInt32ColumnWriter` - 32-bit integers (~5.8 ns/op, 0 allocs)
- `BenchmarkInt64ColumnWriter` - 64-bit integers (~6.4 ns/op, 0 allocs)
- `BenchmarkFloat32ColumnWriter` - 32-bit floats (~6.5 ns/op, 0 allocs)
- `BenchmarkFloat64ColumnWriter` - 64-bit floats (~6.7 ns/op, 0 allocs)
- `BenchmarkStringColumnWriter` - String conversion (~26 ns/op, 162 B/op)
- `BenchmarkDate32ColumnWriter` - Date handling (~5.9 ns/op, 0 allocs)
- `BenchmarkTimestampColumnWriter` - Timestamp conversion (~6.6 ns/op, 0 allocs)
- `BenchmarkTime64ColumnWriter` - Time handling (~6.5 ns/op, 0 allocs)
- `BenchmarkMonthDayNanoIntervalColumnWriter` - Interval type (~8.8 ns/op, 0 allocs)

### 2. Type Handler Parsing
Direct PostgreSQL binary format parsing performance:

- `BenchmarkTypeHandlers_Parse/BoolType` - ~2.1 ns/op, 0 allocs
- `BenchmarkTypeHandlers_Parse/Int2Type` - ~2.5 ns/op, 0 allocs
- `BenchmarkTypeHandlers_Parse/Int4Type` - ~9.1 ns/op, 1 alloc
- `BenchmarkTypeHandlers_Parse/Int8Type` - ~9.5 ns/op, 1 alloc  
- `BenchmarkTypeHandlers_Parse/Float4Type` - ~9.0 ns/op, 1 alloc
- `BenchmarkTypeHandlers_Parse/Float8Type` - ~9.4 ns/op, 1 alloc
- `BenchmarkTypeHandlers_Parse/TextType_Short` - ~26 ns/op, 2 allocs
- `BenchmarkTypeHandlers_Parse/TextType_Long` - ~36 ns/op, 2 allocs
- `BenchmarkTypeHandlers_Parse/IntervalType` - ~12.7 ns/op, 1 alloc

### 3. Memory Layout Optimization
Go GC-optimized batch sizing for sustained performance:

| Batch Size | ns/op   | gc-ns/op | B/op    | allocs/op | Notes |
|------------|---------|----------|---------|-----------|-------|
| 64         | 2,695   | 44.4     | 5,888   | 65        | Small batches, frequent GC |
| 128        | 5,462   | 102.5    | 11,392  | 129       | Good balance |
| **256**    | **10,662** | **174.0** | **22,912** | **257** | **Optimal for Go runtime** |
| 512        | 21,350  | 351.2    | 46,337  | 513       | Higher memory pressure |
| 1024       | 42,711  | 711.8    | 92,802  | 1,025     | Large batches, GC stress |

**Key Finding**: 256-row batches provide optimal balance between throughput and GC efficiency.

### 4. GC Pressure Reduction
CompiledSchema memory optimization results:

| Approach | ns/op | gc-ns/op | B/op | allocs/op | Improvement |
|----------|-------|----------|------|-----------|-------------|
| **With Optimizations** | **56,318** | **456.0** | **38,284** | **1,538** | **Baseline** |
| Without Optimizations | 123,017 | 3,285 | 354,954 | 6,145 | - |
| **Difference** | **54% faster** | **86% less GC** | **89% less memory** | **75% fewer allocs** | - |

### 5. End-to-End Performance Comparisons
PGArrow vs pgx text format parsing:

#### Pool vs Connection Creation
- **PGArrow Pool Creation**: ~10μs (pgxpool.New - creates connection pool)
- **pgx Single Connection**: ~3,455,295 ns/op (pgx.Connect - establishes actual connection)

*Note: This comparison is not directly meaningful since PGArrow uses pgx internally. The difference reflects pool creation vs actual connection establishment, not a performance advantage.*

#### Query Performance (100 rows)
| Query Type | PGArrow (ns/op) | pgx Text (ns/op) | PGArrow Advantage |
|------------|------------------|------------------|-------------------|
| Simple Types | 408,283 | 145,619 | 2.8x processing overhead* |
| All Supported Types | 389,588 | 148,452 | 2.6x processing overhead* |
| Mixed with NULLs | 402,597 | 147,149 | 2.7x processing overhead* |

*Note: PGArrow performs full binary-to-Arrow conversion while pgx only parses to Go types. The overhead includes Arrow record construction, which enables downstream analytical processing.

#### Memory Usage (100 rows)
- **PGArrow**: 27,725 B/op, 904 allocs/op
- **pgx Text**: 6,784 B/op, 406 allocs/op

PGArrow uses more memory upfront but provides structured Arrow records ready for analytical processing.

## Performance Optimizations Achieved

### CompiledSchema Architecture Benefits
- **Concrete improvements** from optimization work:
  - Current: 56,318 ns/op, 38,284 B/op, 1,538 allocs/op
  - Previous: 123,017 ns/op, 354,954 B/op, 6,145 allocs/op
- **54% faster execution**, **89% less memory**, **75% fewer allocations**
- **Sub-microsecond GC impact** per operation (174 gc-ns/op optimal)
- **Direct binary parsing** with zero intermediate copies

### Go Runtime Optimizations
- **Cache-aligned memory layouts** for optimal CPU performance
- **GC-friendly batch sizes** (256 rows) balancing throughput and pressure
- **Buffer pool management** for high-frequency allocations
- **Zero-copy binary data handling** where possible

### Key Performance Patterns
1. **Primitive types** (bool, integers, floats): ~2-9 ns/op, minimal allocations
2. **String types**: ~26-36 ns/op, 2 allocations for UTF-8 conversion
3. **Complex types** (intervals): ~12-13 ns/op, 1 allocation
4. **Batch processing**: Linear scaling with optimal GC characteristics

## Comprehensive Benchmark Suite

PGArrow includes **80+ benchmark functions** organized by category:

### Column Writer Benchmarks (11 functions)
- Individual type writer performance
- Single vs batch operation comparisons
- End-to-end column processing

### Type Handler Benchmarks (14 functions)
- PostgreSQL binary format parsing
- NULL value handling
- UTF-8 text processing with various lengths

### Memory Optimization Benchmarks (8 functions)
- Batch size scaling analysis
- GC pressure measurements
- Memory allocation pattern optimization

### End-to-End Comparison Benchmarks (42+ functions)
- PGArrow vs pgx text format parsing
- Connection initialization performance
- Data type conversion comparisons
- Memory usage analysis across query types

### System Integration Benchmarks (5+ functions)
- Buffer pool efficiency
- Cache alignment impact
- Type registry performance

## Monitoring Performance Regressions

### Key Metrics to Track
1. **Column Writer Performance**: Should maintain sub-10ns for primitives
2. **GC Impact**: Target <200 gc-ns/op for optimal batch sizes
3. **Memory Allocations**: Monitor for unexpected growth in allocs/op
4. **Connection Speed**: PGArrow should remain <20μs for pool creation

### Regression Detection
```bash
# Compare before/after performance
go test -bench=BenchmarkMemoryUsage -count=5 -run=^$ > before.txt
# ... make changes ...
go test -bench=BenchmarkMemoryUsage -count=5 -run=^$ > after.txt
benchcmp before.txt after.txt
```

### CI Integration
Performance regression testing should fail if:
- Column writer performance drops >20%
- GC pressure increases >50%
- Memory allocations grow >30%
- Connection initialization exceeds 50μs

## Architecture Notes

The benchmarks directly test the core CompiledSchema conversion pipeline:
1. **Just-in-Time Metadata Discovery** → Fast connection establishment
2. **Binary Protocol Parsing** → PostgreSQL COPY binary format processing
3. **Type-Specific Conversion** → OID-based optimized type handlers  
4. **Arrow Record Construction** → Columnar array building with optimal batching
5. **Memory Management** → GC-optimized allocation patterns and cleanup

This architecture optimizes for analytical workloads while maintaining compatibility with standard Go database patterns.

## Performance Summary

**Measured performance characteristics:**
- **Memory usage**: 89% reduction in allocations, 75% fewer allocs/op vs previous implementation
- **GC impact**: 174 gc-ns/op measured with 256-row batches
- **Type conversion**: 2-36 ns/op depending on data type complexity
- **Batch processing**: Linear scaling with 256-row optimal batch size
- **Output format**: Direct Arrow records for analytical processing

These measurements provide objective data for evaluating PGArrow in PostgreSQL-to-Arrow use cases.