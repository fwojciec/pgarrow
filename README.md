# PGArrow

PGArrow is a pure Go library that converts PostgreSQL query results directly to Apache Arrow format, without CGO dependencies. It uses a streaming RecordReader architecture for optimal memory efficiency and performance.

**Key Benefits:**
- ✅ **Lazy metadata loading** (no upfront column type preloading)  
- ✅ **Zero CGO dependencies** (pure Go)
- ✅ **Streaming architecture** (constant memory usage, scalable to any result set size)
- ✅ **High performance** (direct binary format conversion, DuckDB-optimized batching)
- ✅ **pgx compatibility** (uses pgxpool for connection management)
- ✅ **Arrow ecosystem ready** (implements standard array.RecordReader interface)

## Quick Start

### Installation

```bash
go get github.com/fwojciec/pgarrow
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/fwojciec/pgarrow"
)

func main() {
    // Create pool
    pool, err := pgarrow.NewPool(context.Background(), 
        "postgres://user:password@localhost/dbname")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    // Execute query and get Arrow RecordReader (streaming)
    reader, err := pool.QueryArrow(context.Background(), 
        "SELECT id, name, active FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Release()

    // Stream through batches of data
    for reader.Next() {
        record := reader.Record()
        
        // Access data using Arrow arrays
        idCol := record.Column(0).(*array.Int32)
        nameCol := record.Column(1).(*array.String)
        activeCol := record.Column(2).(*array.Boolean)

        for i := 0; i < int(record.NumRows()); i++ {
            fmt.Printf("ID: %d, Name: %s, Active: %t\n",
                idCol.Value(i), nameCol.Value(i), activeCol.Value(i))
        }
    }
    
    // Check for errors
    if err := reader.Err(); err != nil {
        log.Fatal(err)
    }
}
```

## Supported Data Types

PGArrow supports 11 PostgreSQL data types with direct Arrow format conversion:

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

### NULL Value Handling

PGArrow properly handles PostgreSQL NULL values using Arrow's null bitmap:

```go
reader, err := pool.QueryArrow(ctx, "SELECT id, name FROM users")
if err != nil {
    log.Fatal(err)
}
defer reader.Release()

for reader.Next() {
    record := reader.Record()
    nameCol := record.Column(1).(*array.String)
    
    for i := 0; i < int(record.NumRows()); i++ {
        if nameCol.IsNull(i) {
            fmt.Printf("Row %d: name is NULL\n", i)
        } else {
            fmt.Printf("Row %d: name is %s\n", i, nameCol.Value(i))
        }
    }
}

if err := reader.Err(); err != nil {
    log.Fatal(err)
}
```

## Examples

See the [`examples/`](examples/) directory for complete working examples:

- [`examples/simple/`](examples/simple/) - Basic QueryArrow usage
- [`examples/types/`](examples/types/) - All supported data types demonstration  
- [`examples/README.md`](examples/README.md) - Setup and run instructions

## Performance

PGArrow uses lazy metadata loading, avoiding upfront preloading of all database schema information. Column types are discovered only when needed during query execution.

**Memory Efficiency:**
- Direct binary format parsing (PostgreSQL COPY BINARY)
- Streaming architecture with configurable batch sizes
- Zero-copy data access where possible
- Proper memory management with Arrow's reference counting

Run benchmarks:
```bash
go test -bench=. -benchmem ./...
```

## Architecture

PGArrow uses PostgreSQL's `COPY TO BINARY` format with streaming RecordReader architecture for optimal data transfer and constant memory usage:

```
PostgreSQL → COPY TO BINARY → Binary Parser → Type Handlers → Arrow RecordReader → Arrow Record Batches
```

### Core Components

1. **Pool**: Connection management using pgxpool with lazy metadata loading
2. **RecordReader**: Streaming interface implementing `array.RecordReader` with proper reference counting
3. **Binary Parser**: PostgreSQL binary format decoder with support for all 11 data types  
4. **Type System**: OID-based type handlers with direct Arrow conversion
5. **Record Builder**: Arrow record construction with proper memory management and DuckDB-optimized batching (128,800 rows)

### Streaming Benefits

- **Constant Memory Usage**: Processes data in configurable batches regardless of result set size
- **Connection Lifecycle**: Proper connection management tied to RecordReader lifecycle
- **Reference Counting**: Atomic reference counting following Apache Arrow patterns
- **Zero-Copy Access**: Direct access to Arrow data without intermediate copies

## Status

PGArrow is currently in active development. The core functionality is implemented and working:

- ✅ All 11 supported data types
- ✅ NULL value handling  
- ✅ Connection pooling with lazy metadata loading
- ✅ Binary format parsing
- ✅ Streaming RecordReader architecture
- ✅ Arrow record building with batching
- ✅ Memory safety and leak prevention
- ✅ Comprehensive test suite with parallel execution
- ✅ Performance benchmarks
- ✅ DuckDB-optimized batch sizes

## Known Limitations

- **Parameterized Queries**: PGArrow does not support parameterized queries ($1, $2, etc.) due to PostgreSQL's COPY TO BINARY protocol limitations. Use literal values in your SQL queries instead.
- **Limited Data Types**: Currently supports 11 PostgreSQL data types. Additional types can be added as needed.