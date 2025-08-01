# PGArrow

PGArrow is a pure Go library that provides ADBC-like functionality for converting PostgreSQL query results directly to Apache Arrow format, without CGO dependencies.

**Key Benefits:**
- ✅ **Instant connections** (no metadata preloading)  
- ✅ **Zero CGO dependencies** (pure Go)
- ✅ **High performance** (direct binary format conversion)
- ✅ **pgx compatibility** (uses pgxpool for connection management)

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

    // Execute query and get Arrow record
    record, err := pool.QueryArrow(context.Background(), 
        "SELECT id, name, active FROM users WHERE id = $1", 123)
    if err != nil {
        log.Fatal(err)
    }
    defer record.Release()

    // Access data using Arrow arrays
    idCol := record.Column(0).(*array.Int32)
    nameCol := record.Column(1).(*array.String)
    activeCol := record.Column(2).(*array.Boolean)

    for i := 0; i < int(record.NumRows()); i++ {
        fmt.Printf("ID: %d, Name: %s, Active: %t\n",
            idCol.Value(i), nameCol.Value(i), activeCol.Value(i))
    }
}
```

## Supported Data Types

PGArrow supports 7 core PostgreSQL data types with direct Arrow format conversion:

| PostgreSQL Type | PostgreSQL OID | Arrow Type | Go Type |
|----------------|---------------|------------|---------|
| `bool` | 16 | `Boolean` | `bool` |
| `int2` / `smallint` | 21 | `Int16` | `int16` |
| `int4` / `integer` | 23 | `Int32` | `int32` |
| `int8` / `bigint` | 20 | `Int64` | `int64` |
| `float4` / `real` | 700 | `Float32` | `float32` |
| `float8` / `double precision` | 701 | `Float64` | `float64` |
| `text` | 25 | `String` | `string` |

### NULL Value Handling

PGArrow properly handles PostgreSQL NULL values using Arrow's null bitmap:

```go
record, err := pool.QueryArrow(ctx, "SELECT id, name FROM users")
if err != nil {
    log.Fatal(err)
}
defer record.Release()

nameCol := record.Column(1).(*array.String)
for i := 0; i < int(record.NumRows()); i++ {
    if nameCol.IsNull(i) {
        fmt.Printf("Row %d: name is NULL\n", i)
    } else {
        fmt.Printf("Row %d: name is %s\n", i, nameCol.Value(i))
    }
}
```

## Examples

See the [`examples/`](examples/) directory for complete working examples:

- [`examples/simple/`](examples/simple/) - Basic QueryArrow usage
- [`examples/types/`](examples/types/) - All supported data types demonstration  
- [`examples/README.md`](examples/README.md) - Setup and run instructions

## Performance

PGArrow addresses the key scalability issue in ADBC where connection initialization takes 6-8 seconds for databases with 20k+ tables due to upfront metadata loading.

**Connection Speed:**
- **PGArrow**: <100ms (instant connections)
- **ADBC**: 6-8 seconds (with 20k+ tables)

**Memory Efficiency:**
- Direct binary format parsing (PostgreSQL COPY BINARY)
- Zero-copy data access where possible
- Proper memory management with Arrow's reference counting

Run benchmarks:
```bash
go test -bench=. -benchmem ./...
```

## Architecture

PGArrow uses PostgreSQL's `COPY TO BINARY` format for optimal data transfer:

```
PostgreSQL → COPY TO BINARY → Binary Parser → Type Handlers → Arrow Record
```

### Core Components

1. **Pool**: Connection management using pgxpool
2. **Binary Parser**: PostgreSQL binary format decoder  
3. **Type System**: OID-based type handlers with Arrow conversion
4. **Record Builder**: Arrow record construction with proper memory management

## Status

PGArrow is currently in active development. The core functionality is implemented and working:

- ✅ All 7 supported data types
- ✅ NULL value handling  
- ✅ Connection pooling
- ✅ Binary format parsing
- ✅ Arrow record building
- ✅ Memory safety and leak prevention
- ✅ Comprehensive test suite
- ✅ Performance benchmarks

## Known Limitations

- **Parameterized Queries**: PGArrow does not support parameterized queries ($1, $2, etc.) due to PostgreSQL's COPY TO BINARY protocol limitations. Use literal values in your SQL queries instead.
- **Limited Data Types**: Currently supports 7 PostgreSQL data types. Additional types can be added as needed.

See [docs/implementation-plan.md](docs/implementation-plan.md) for detailed technical specifications.