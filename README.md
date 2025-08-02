# PGArrow

**Fast PostgreSQL → Apache Arrow conversion in pure Go**

Zero-CGO library that streams PostgreSQL query results directly to Arrow format using binary protocol. Perfect for analytical workloads, data pipelines, and Arrow ecosystem integration.

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
| ⚡ **Just-in-Time Metadata** | Fast connections, discover types on-demand |
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

**11 PostgreSQL types** → **Arrow native format**

<details>
<summary>📋 <strong>Full Type Mapping Table</strong></summary>

| PostgreSQL Type | PostgreSQL OID | Arrow Type | Go Type |
|----------------|---------------|------------|---------|
| `bool` | 16 | `Boolean` | `bool` |
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

</details>

✅ Full NULL handling • ✅ Microsecond timestamps • ✅ Binary data support

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

- 🏃 **Fast Connections**: Just-in-time metadata discovery, no upfront schema queries
- 🧠 **Memory Efficient**: Constant usage via streaming batches  
- ⚡ **High Throughput**: Direct binary conversion, no JSON overhead
- 📊 **Optimized Batching**: DuckDB-optimized 128K row batches

```bash
go test -bench=. -benchmem  # Run benchmarks
```

---

## AI-First Development with Production Quality

This codebase is written by Claude with Copilot performing code reviews, using a human-supervised quality-first approach. Rather than viewing AI as a productivity tool that requires extensive human cleanup, we treat it as a capable development partner that produces production-ready code when guided by robust processes.

### Our Quality Philosophy

**Process over Polish**: Quality comes from systematic validation and constant iteration within established feedback loops, not post-hoc cleanup.

- **Test-Driven Development**: Tests written first, implementation follows (documented in `CLAUDE.md`)
- **Specification Compliance**: Real PostgreSQL binary format validation with comprehensive boundary testing
- **Performance Validation**: Benchmarks ensure efficient type conversions and memory usage
- **Multi-Layer Validation**: Race detection (`t.Parallel()` + `-race`), linting, integration testing via `make validate`
- **AI-AI Collaboration**: Copilot reviews trigger investigation, not blind acceptance
- **Reference Architecture**: C++ ADBC implementation provides invaluable implementation guidance

### Production Readiness

While developed with rigorous quality processes, this software should be considered **alpha quality** until battle-tested in production environments. The systematic approach demonstrates AI's potential for quality code generation, but real-world validation remains essential.

---

## Documentation

- [📖 **Complete Examples**](examples/) - Working code for all features
- [🏗️ **Architecture Guide**](CLAUDE.md) - How it works internally  
- [⚡ **Performance Analysis**](docs/benchmarks.md) - Detailed metrics
- [🧪 **Testing Strategy**](docs/testing.md) - Our quality approach

## Status & Limitations

**✅ Ready for experimentation:**
- Core functionality complete
- 11 data types supported
- Comprehensive test suite
- Performance benchmarks

**⚠️ Known limits:**
- No parameterized queries (COPY protocol limitation)
- Alpha quality - production validation needed
- Limited to 11 PostgreSQL types (more coming)

---

**Questions?** [Open an issue](../../issues) • **Contributing:** See [CLAUDE.md](CLAUDE.md)