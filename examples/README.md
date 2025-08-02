# PGArrow Examples

This directory contains example applications demonstrating how to use PGArrow to convert PostgreSQL query results directly to Apache Arrow format.

## Prerequisites

- Go 1.24.5 or later
- PostgreSQL database instance
- `DATABASE_URL` environment variable set

## Examples

### 1. Simple Example (`simple/`)

Basic PGArrow usage demonstrating:
- Pool creation and management
- Simple queries with `QueryArrow()`
- Accessing Arrow record data
- Memory management with `defer record.Release()`

**Run:**
```bash
cd simple
export DATABASE_URL="postgres://user:password@localhost/dbname"
go run main.go
```

### 2. Types Example (`types/`)

Comprehensive demonstration of all 17 supported PostgreSQL data types including:
- `bool`, `bytea`
- `int2/smallint`, `int4/integer`, `int8/bigint` 
- `float4/real`, `float8/double precision`
- `text`, `varchar`, `bpchar/char(n)`, `name`, `"char"`
- `date`, `time`, `timestamp`, `timestamptz`, `interval`

Also demonstrates:
- NULL value handling
- Mixed data queries with some NULL values
- Memory tracking with `memory.CheckedAllocator`

**Run:**
```bash
cd types
export DATABASE_URL="postgres://user:password@localhost/dbname"
go run main.go
```

## Database Setup

These examples work with any PostgreSQL database and don't require specific tables. They use:
- Simple `SELECT` statements with literal values
- `VALUES` clauses to create inline data
- PostgreSQL type casting (e.g., `123::int2`)

## ⚠️ Important Limitation

**Parameterized queries are NOT supported** due to PostgreSQL's COPY TO BINARY protocol limitations. 

❌ **Don't do this:**
```go
// This will fail - parameterized queries not supported
record, err := pool.QueryArrow(ctx, "SELECT * FROM my_table WHERE id = $1", 123)
```

✅ **Do this instead:**
```go  
// Use literal values in your SQL
record, err := pool.QueryArrow(ctx, "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS my_table(id, name) WHERE id = 123")
```

### Example DATABASE_URL formats:

**Local PostgreSQL:**
```bash
export DATABASE_URL="postgres://postgres:password@localhost:5432/postgres"
```

**PostgreSQL with SSL:**
```bash
export DATABASE_URL="postgres://user:pass@host:5432/db?sslmode=require"
```

**Connection pooling options:**
```bash
export DATABASE_URL="postgres://user:pass@host:5432/db?pool_max_conns=10"
```

## Expected Output

### Simple Example
```
Query returned 1 rows, 3 columns
Schema: schema:
  fields: 3
    - id: type=int32
    - message: type=utf8
    - active: type=bool

Row 0: id=1, message=Hello World, active=true

--- Table Query Example ---
Table query returned 3 rows

PGArrow simple example completed successfully!
```

### Types Example  
```
=== PGArrow Supported Data Types Demo ===

Query returned 1 rows, 7 columns
Schema: schema:
  fields: 7
    - bool_col: type=bool
    - int2_col: type=int16
    - int4_col: type=int32
    - int8_col: type=int64
    - float4_col: type=float32
    - float8_col: type=float64
    - text_col: type=utf8

Row 0:
  bool:   true
  int2:   123
  int4:   456789
  int8:   123456789012
  float4: 3.140000
  float8: 2.718281828
  text:   Hello PGArrow!

=== NULL Value Handling ===
...

PGArrow types example completed successfully!
```

## Troubleshooting

**Connection Issues:**
- Verify PostgreSQL is running and accessible
- Check `DATABASE_URL` format and credentials
- Ensure network connectivity to database host

**Build Issues:**
- Verify Go 1.24.5+ is installed: `go version`
- Run `go mod tidy` in the project root
- Check for missing dependencies: `go mod verify`

**Runtime Issues:**
- Enable verbose logging by modifying examples
- Check PostgreSQL logs for connection/query errors
- Verify supported PostgreSQL version (9.5+)

## Performance Notes

PGArrow provides significant performance benefits over text-based parsing:
- **Connection Speed**: <100ms vs 6-8s for ADBC with large schemas
- **Memory Efficiency**: Direct binary format parsing
- **Zero CGO**: Pure Go implementation
- **Type Safety**: Strong typing with Arrow format

For performance comparisons, see the benchmarks in the main test suite.