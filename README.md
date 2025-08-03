# PGArrow

[![CI](https://github.com/fwojciec/pgarrow/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/fwojciec/pgarrow/actions/workflows/ci.yml)

**PostgreSQL → Apache Arrow conversion in pure Go**

Pure Go library that streams PostgreSQL query results directly to Arrow format using binary protocol. Designed for analytical workloads, data pipelines, and Arrow ecosystem integration.

```go
// One pool, many queries - streaming results
reader, err := pool.QueryArrow(ctx, "SELECT * FROM large_table")
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

## Quick Start

### Install
```bash
go get github.com/fwojciec/pgarrow
```

### 30-Second Example
```go
pool, _ := pgarrow.NewPool(ctx, "postgres://...")
reader, _ := pool.QueryArrow(ctx, "SELECT id, name FROM users")
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
PostgreSQL → COPY BINARY → Stream Parser → Arrow Batches
                ↳ No schema preloading
                ↳ Constant memory usage  
                ↳ Zero intermediate copies
```

**Core Philosophy**: Just-in-time metadata, stream everything, copy nothing.

---

## Performance <a id="performance"></a>

**Performance characteristics:**

- **Just-in-Time Metadata**: Schema discovered at query time, not at connection time
- **Memory Usage**: 89% reduction in allocations vs previous implementation  
- **Type Conversion**: 2-9 ns/op with optimized string handling
- **GC Impact**: 174 gc-ns/op measured with 256-row batches

**Architecture:**
- **Uses pgx internally**: Built on proven PostgreSQL driver foundation
- **Just-in-time metadata discovery**: No expensive upfront schema queries
- **CompiledSchema optimization**: Direct binary-to-Arrow conversion pipeline

**Type Conversion Speed:**
- **Primitive types** (bool, integers, floats): 2-9 ns/op, zero allocations
- **String types**: 8.5 ns/op, zero allocations with optimized buffer management
- **Complex types** (intervals, timestamps): 12-13 ns/op

**Memory Efficiency:**
- **Current implementation**: 38,284 B/op, 1,538 allocs/op (optimized)
- **Previous implementation**: 354,954 B/op, 6,145 allocs/op (unoptimized)  
- **89% memory reduction**, **75% fewer allocations**, **86% less GC pressure**
- **Zero-copy binary data** handling where possible

**Implementation approach:**
- **Direct binary parsing**: PostgreSQL COPY protocol to Arrow format
- **Memory layout optimization**: Cache-aligned structures for Go runtime
- **Batch size tuning**: 256 rows balances throughput and GC pressure  
- **Zero-copy operations**: Minimize data movement where possible

```bash
go test -bench=. -benchmem  # Run comprehensive 80+ benchmark suite
```

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
- **Reference Architecture**: C++ ADBC implementation provides invaluable implementation guidance

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

**🎯 Design decisions:**
- **All columns marked nullable**: Arrow schema always shows `nullable=true` regardless of PostgreSQL `NOT NULL` constraints, optimizing for performance and compatibility with major Arrow engines (DuckDB, DataFusion, Polars) that ignore nullability metadata anyway ([research details](docs/nullability-tradeoff-research.md))

---

**Questions?** [Open an issue](../../issues) • **Contributing:** See [CLAUDE.md](CLAUDE.md)