# PGArrow Performance Benchmarks

This document describes the performance benchmarking suite for PGArrow's binary-to-Arrow conversion pipeline.

## Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. -run=^$ -benchmem

# Run specific benchmark category
go test -bench=BenchmarkBinaryToArrow -run=^$ -benchmem

# Run with CPU profiling
go test -bench=BenchmarkBinaryToArrow_MixedTypes -run=^$ -cpuprofile=cpu.prof

# Run with memory profiling  
go test -bench=BenchmarkBinaryToArrow_ByteaMemory -run=^$ -memprofile=mem.prof
```

## Benchmark Categories

### 1. Data Type Benchmarks
Test conversion performance for different PostgreSQL data types:

- `BenchmarkBinaryToArrow_Int4` - 32-bit integers
- `BenchmarkBinaryToArrow_Int8` - 64-bit integers  
- `BenchmarkBinaryToArrow_Float8` - 64-bit floats
- `BenchmarkBinaryToArrow_Text` - Text/string data
- `BenchmarkBinaryToArrow_Bytea` - Binary data (zero-copy optimized)

### 2. Batch Size Scaling
Test how performance scales with different row counts:

- `BenchmarkBinaryToArrow_SmallBatch` - 100 rows
- `BenchmarkBinaryToArrow_MediumBatch` - 1,000 rows  
- `BenchmarkBinaryToArrow_LargeBatch` - 10,000 rows

### 3. Mixed Type Scenarios
Test realistic workloads with multiple data types:

- `BenchmarkBinaryToArrow_MixedTypes` - Int4, Text, Float8, Bytea, Bool

### 4. Memory Allocation Tracking
Special benchmarks that report detailed allocation metrics:

- `BenchmarkBinaryToArrow_ByteaMemory` - Track binary data allocations
- `BenchmarkBinaryToArrow_TextMemory` - Track string conversion allocations

## Baseline Performance Metrics

*Recorded on Apple M1, Go 1.24.5, after zero-copy optimizations*

### Single Data Type Performance (1,000 rows)

| Data Type | ns/op     | B/op    | allocs/op | Notes |
|-----------|-----------|---------|-----------|-------|
| Int4      | ~110,000  | 62,353  | 4,781     | Primitive, minimal copying |
| Int8      | ~112,000  | 76,657  | 5,036     | Primitive, 8-byte values |
| Float8    | ~112,000  | 76,657  | 5,036     | Primitive, same as Int8 |
| Text      | ~159,000  | 153,726 | 8,055     | String conversion overhead |
| Bytea     | ~146,000  | 239,865 | 5,119     | Zero-copy optimization |

### Batch Size Scaling

| Batch Size | ns/op        | B/op     | allocs/op | Rows/sec   |
|------------|--------------|----------|-----------|------------|
| 100        | ~11,200      | 7,664    | 433       | ~8.9M      |
| 1,000      | ~110,000     | 62,353   | 4,781     | ~9.1M      |
| 10,000     | ~1,118,000   | 677,313  | 49,789    | ~8.9M      |

**Key Insight**: Performance scales linearly with batch size (~9M rows/sec sustained).

### Mixed Types Performance
- **5 columns (Int4, Text, Float8, Bytea, Bool)**: ~530,000 ns/op for 1,000 rows
- **Throughput**: ~1.9M rows/sec for realistic mixed workloads

## Performance Optimizations Achieved

### Zero-Copy Binary Data
- **Bytea fields** use field-specific buffers to avoid copying
- Raw bytes passed directly from parser to Arrow builder
- **Memory Impact**: Eliminates one copy operation per binary field

### Reduced String Copying  
- **Text types** reduced from 3 copies to 1 copy:
  1. ~~Binary → parseFieldData (eliminated)~~
  2. parseFieldData → string conversion (retained)
  3. ~~String → TypeHandler (eliminated)~~
  4. TypeHandler → Arrow builder (retained)

### Streamlined Type Conversion
- Parser does minimal conversion (raw bytes or strings only)
- TypeHandlers handle primary conversion logic
- Eliminated redundant processing layers

## Monitoring Performance Regressions

### Key Metrics to Track
1. **Throughput** (rows/sec) - should remain ~9M for primitives, ~2M for mixed
2. **Memory allocations** (allocs/op) - watch for unexpected growth
3. **Memory usage** (B/op) - ensure zero-copy optimizations maintained

### Regression Detection
```bash
# Compare before/after performance
go test -bench=BenchmarkBinaryToArrow_Bytea -count=5 -run=^$ > before.txt
# ... make changes ...
go test -bench=BenchmarkBinaryToArrow_Bytea -count=5 -run=^$ > after.txt
benchcmp before.txt after.txt
```

### CI Integration
Consider adding performance regression testing:
```bash
# Fail if performance drops more than 20%
go test -bench=. -run=^$ -benchmem | tee benchmark.txt
# Compare against historical baseline
```

## Architecture Notes

The benchmarks directly test the core conversion pipeline:
1. **Binary Parser** → PostgreSQL COPY binary format parsing
2. **Type Conversion** → OID-based type handler dispatch  
3. **Arrow Building** → Arrow columnar array construction
4. **Memory Management** → Allocation tracking and cleanup

This matches the real-world usage pattern and provides realistic performance metrics for optimization decisions.