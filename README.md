# PGArrow

[![CI](https://github.com/fwojciec/pgarrow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fwojciec/pgarrow/actions/workflows/ci.yml)

**PostgreSQL → Apache Arrow conversion in pure Go**


Pure Go library that streams PostgreSQL query results directly to Arrow format using binary protocol. Designed for analytical workloads, data pipelines, and Arrow ecosystem integration.

```go
// One pool, many queries - with safe parameterization
reader, err := pool.QueryArrow(ctx, 
    "SELECT * FROM large_table WHERE created_at > $1",
    time.Now().AddDate(0, -1, 0)) // Last month
defer reader.Release()

for reader.Next() {
    batch := reader.Record() // Arrow record batch
    // Process with Arrow ecosystem (DuckDB, DataFusion, etc.)
}
```

## Why PGArrow?

| What You Get | How It Helps |
|--------------|---------------|
| 🐹 **Pure Go** | Easy deployment, no CGO complexity |
| ⚡ **Just-in-Time Metadata** | Schema discovered at query time, not at connection time |
| 📊 **Streaming** | Constant memory usage, handles any result size |
| 🎯 **Arrow Native** | Drop-in `array.RecordReader`, ecosystem ready |
| 🔒 **Safe Queries** | Full parameterization support prevents SQL injection |

### Performance-First Design

PGArrow uses PostgreSQL's SELECT protocol with binary format optimization, achieving performance comparable to Apache Arrow ADBC - the gold standard for database-to-Arrow conversion. Our implementation achieves **2.44M rows/sec** throughput (measured on 5-column table, see [benchmarks](docs/benchmarks.md) for full methodology), matching ADBC's excellent performance through:

- **SELECT over COPY**: 86% faster than COPY protocol for read workflows ([detailed investigation](docs/performance-investigation-2025.md))
- **Binary wire format**: Direct access via pgx's RawValues() API
- **Optimized batching**: 200K rows per batch based on empirical testing
- **Zero-allocation conversions**: All 17 supported types achieve zero heap allocations

## Quick Start

### Install
```bash
go get github.com/fwojciec/pgarrow
```

### 30-Second Example
```go
pool, _ := pgarrow.NewPool(ctx, "postgres://...")

// Safe parameterized queries - no SQL injection
reader, _ := pool.QueryArrow(ctx, 
    "SELECT id, name FROM users WHERE active = $1 AND age > $2",
    true, 18)
defer reader.Release()

for reader.Next() {
    record := reader.Record()
    // Standard Arrow record - works with any Arrow tool
}
```

[▶️ **Run Complete Examples**](examples/) | [📊 **See All Data Types**](#supported-types) | [🚀 **Performance Details**](#performance)

---

## Supported Types <a id="supported-types"></a>

**17 PostgreSQL types** → **Arrow native format**

<details>
<summary>📋 <strong>Full Type Mapping Table</strong></summary>

| PostgreSQL Type | PostgreSQL OID | Arrow Type | Go Type |
|----------------|---------------|------------|---------|
| `bool` | 16 | `Boolean` | `bool` |
| `bytea` | 17 | `Binary` | `[]byte` |
| `int2` / `smallint` | 21 | `Int16` | `int16` |
| `int4` / `integer` | 23 | `Int32` | `int32` |
| `int8` / `bigint` | 20 | `Int64` | `int64` |
| `float4` / `real` | 700 | `Float32` | `float32` |
| `float8` / `double precision` | 701 | `Float64` | `float64` |
| `text` | 25 | `String` | `string` |
| `varchar` | 1043 | `String` | `string` |
| `bpchar` / `char(n)` | 1042 | `String` | `string` |
| `name` | 19 | `String` | `string` |
| `"char"` | 18 | `String` | `string` |
| `date` | 1082 | `Date32` | `int32` |
| `time` | 1083 | `Time64[μs]` | `int64` |
| `timestamp` | 1114 | `Timestamp[μs]` | `int64` |
| `timestamptz` | 1184 | `Timestamp[μs, UTC]` | `int64` |
| `interval` | 1186 | `MonthDayNanoInterval` | `MonthDayNanoInterval` |

</details>

✅ Full NULL handling • ✅ Microsecond precision • ✅ Binary data • ✅ Temporal types • ✅ Interval support

---

## How It Works

```
PostgreSQL → SELECT (Binary) → Direct Wire Parsing → Arrow Batches
                ↳ No schema preloading
                ↳ Constant memory usage  
                ↳ Zero intermediate copies
```

**Core Philosophy**: Just-in-time metadata, stream everything, copy nothing.

Unlike libraries that use COPY protocol or require extensive metadata preloading, PGArrow leverages PostgreSQL's SELECT protocol with binary format optimization. This approach, validated through [comprehensive benchmarking](docs/technical-deep-dive-select-protocol.md), delivers superior performance for read-focused analytical workloads.

---

## Performance <a id="performance"></a>


**Performance characteristics:**

- **Throughput**: 2.44M rows/sec average (5-column table: int64, float64, bool, text, date)
- **Protocol**: SELECT with binary format - 86% faster than COPY protocol
- **Comparison**: Performance comparable to Apache Arrow ADBC (the reference standard)
- **Memory Usage**: 89% reduction in allocations vs previous implementation  
- **ColumnWriter Performance**: 5-26 ns/op with zero allocations for most types
- **GC Impact**: 174 gc-ns/op measured with 256-row batches

**Architecture:**
- **Built on proven foundations**: [pgx](https://github.com/jackc/pgx) for PostgreSQL connectivity + [Apache Arrow Go](https://github.com/apache/arrow-go) for columnar format
- **SELECT protocol optimization**: QueryExecMode.CacheDescribe for automatic binary format
- **Direct wire access**: pgx RawValues() API eliminates intermediate conversions
- **CompiledSchema optimization**: Direct binary-to-Arrow conversion pipeline

**ColumnWriter Performance:**
- **Primitive types** (bool, integers, floats): 5-7 ns/op, zero allocations
- **String types**: ~12.7 ns/op, zero allocations with memory-safe buffer management  
- **Complex types** (intervals, timestamps): 6-9 ns/op, zero allocations

All 17 supported PostgreSQL data types achieve zero heap allocations during Arrow conversion.

**Memory Efficiency:**
- **Current implementation**: 38,284 B/op, 1,538 allocs/op (optimized)
- **Previous implementation**: 354,954 B/op, 6,145 allocs/op (unoptimized)  
- **89% memory reduction**, **75% fewer allocations**, **86% less GC pressure**
- **Zero-copy binary data** handling where possible

**Implementation approach:**
- **Direct binary parsing**: PostgreSQL wire format to Arrow without intermediate representations
- **Optimal batching**: 200K rows per batch based on empirical testing
- **Memory layout optimization**: Cache-aligned structures for Go runtime
- **Zero-copy operations**: Direct access to pgx's wire buffers where possible

```bash
go test -bench=. -benchmem  # Run comprehensive 80+ benchmark suite
```

See [docs/benchmarks.md](docs/benchmarks.md) for full benchmark methodology and test dataset details.

---

## AI-First Development with Production Quality

This codebase is written by Claude with both Copilot and Gemini performing code reviews, using a human-supervised quality-first approach. Rather than viewing AI as a productivity tool that requires extensive human cleanup, we treat it as a capable development partner that produces production-ready code when guided by robust processes.

### Our Quality Philosophy

**Process over Polish**: Quality comes from systematic validation and constant iteration within established feedback loops, not post-hoc cleanup.

- **Test-Driven Development**: Tests written first, implementation follows (documented in `CLAUDE.md`)
- **Specification Compliance**: Real PostgreSQL binary format validation with comprehensive boundary testing
- **Performance Validation**: Benchmarks ensure efficient type conversions and memory usage
- **Multi-Layer Validation**: Race detection (`t.Parallel()` + `-race`), linting, integration testing via `make validate`
- **AI-AI Collaboration**: Copilot and Gemini reviews trigger investigation, not blind acceptance
- **Reference Architecture**: Apache Arrow ADBC serves as our gold standard for quality and performance

### Production Readiness

While developed with rigorous quality processes, this software should be considered **alpha quality** until battle-tested in production environments. The systematic approach demonstrates AI's potential for quality code generation, but real-world validation remains essential.

---

## Documentation

- [📖 **Complete Examples**](examples/) - Working code for all features
- [🏗️ **Architecture Guide**](CLAUDE.md) - How it works internally  
- [⚡ **Performance Analysis**](docs/benchmarks.md) - CompiledSchema benchmarks (80+ functions)
- [🧪 **Testing Strategy**](docs/testing.md) - Our quality approach

## Status & Limitations

**✅ Ready for experimentation:**
- Core functionality complete
- 17 data types supported
- Comprehensive test suite
- Performance benchmarks

**⚠️ Known limits:**
- Alpha quality - production validation needed
- Limited to 17 PostgreSQL types currently
- Read-only operations (no write support)

**🎯 Design decisions:**
- **All columns marked nullable**: Arrow schema always shows `nullable=true` regardless of PostgreSQL `NOT NULL` constraints, optimizing for performance and compatibility with major Arrow engines (DuckDB, DataFusion, Polars) that ignore nullability metadata anyway ([research details](docs/nullability-tradeoff-research.md))
- **SELECT protocol focus**: Optimized for read workflows, deliberately choosing SELECT over COPY based on measured 86% performance improvement for analytical queries
- **Pure Go implementation**: No CGO dependencies, prioritizing deployment simplicity and maintainability over potential micro-optimizations

---

## Credits

Built on top of exceptional open source projects:
- **[pgx](https://github.com/jackc/pgx)** - PostgreSQL driver and toolkit for Go
- **[Apache Arrow Go](https://github.com/apache/arrow-go)** - Go implementation of Apache Arrow columnar format

---

**Questions?** [Open an issue](../../issues) • **Contributing:** See [CLAUDE.md](CLAUDE.md)