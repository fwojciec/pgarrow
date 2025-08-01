# PGArrow Testing Strategy

## Overview

This document outlines the comprehensive testing strategy for PGArrow, emphasizing test-driven development (TDD) using pgxmock for database interactions and binary format validation.

## Testing Framework Setup

### Core Dependencies
```go
// go.mod
module github.com/example/pgarrow

go 1.21

require (
    github.com/apache/arrow-go/v18 v18.0.0
    github.com/jackc/pgx/v5 v5.5.0
    github.com/pashagolub/pgxmock/v4 v4.2.0
    github.com/stretchr/testify v1.8.4
)
```

### Test Structure
```
tests/
├── unit/
│   ├── binary_test.go         # Binary format parsing
│   ├── types_test.go          # Type conversion
│   └── arrow_test.go          # Arrow record building
├── integration/
│   ├── connection_test.go     # Database connectivity
│   ├── query_test.go          # End-to-end queries
│   └── pool_test.go           # Connection pooling
├── performance/
│   ├── benchmark_test.go      # Performance comparisons
│   └── memory_test.go         # Memory usage validation
├── fixtures/
│   ├── binary_samples/        # Binary COPY format samples
│   └── test_data.sql          # Test data generation
└── helpers/
    ├── mock_data.go           # Mock data generation
    └── binary_builder.go      # Binary format construction
```

## Unit Testing Strategy

### 1. Binary Format Parser Tests

```go
package binary

import (
    "bytes"
    "encoding/binary"
    "testing"
    
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestParseHeader(t *testing.T) {
    tests := []struct {
        name    string
        data    []byte
        want    *Header
        wantErr bool
    }{
        {
            name: "valid header",
            data: buildHeaderBytes(t, 0, 0),
            want: &Header{
                Signature:    [11]byte{'P','G','C','O','P','Y','\n',0xff,'\r','\n',0},
                Flags:        0,
                ExtensionLen: 0,
            },
            wantErr: false,
        },
        {
            name:    "invalid signature",
            data:    []byte("INVALID_SIG\x00\x00\x00\x00"),
            want:    nil,
            wantErr: true,
        },
        {
            name:    "truncated header",
            data:    []byte("PGCOPY\n\xff\r\n"),
            want:    nil,
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            parser := NewParser(bytes.NewReader(tt.data))
            header, err := parser.ParseHeader()
            
            if tt.wantErr {
                assert.Error(t, err)
                assert.Nil(t, header)
            } else {
                require.NoError(t, err)
                assert.Equal(t, tt.want, header)
            }
        })
    }
}

func TestParseTuple(t *testing.T) {
    tests := []struct {
        name    string
        data    []byte
        want    *Tuple
        wantErr bool
    }{
        {
            name: "single int4 field",
            data: buildTupleBytes(t, []Field{
                {Length: 4, Data: []byte{0x00, 0x00, 0x00, 0x2A}}, // 42
            }),
            want: &Tuple{
                FieldCount: 1,
                Fields: []Field{
                    {Length: 4, Data: []byte{0x00, 0x00, 0x00, 0x2A}},
                },
            },
            wantErr: false,
        },
        {
            name: "null field",
            data: buildTupleBytes(t, []Field{
                {Length: -1, Data: nil},
            }),
            want: &Tuple{
                FieldCount: 1,
                Fields: []Field{
                    {Length: -1, Data: nil},
                },
            },
            wantErr: false,
        },
        {
            name: "mixed fields",
            data: buildTupleBytes(t, []Field{
                {Length: 4, Data: []byte{0x00, 0x00, 0x00, 0x2A}},    // int4: 42
                {Length: 5, Data: []byte("Hello")},                    // text: "Hello"
                {Length: -1, Data: nil},                               // NULL
                {Length: 1, Data: []byte{0x01}},                       // bool: true
            }),
            want: &Tuple{
                FieldCount: 4,
                Fields: []Field{
                    {Length: 4, Data: []byte{0x00, 0x00, 0x00, 0x2A}},
                    {Length: 5, Data: []byte("Hello")},
                    {Length: -1, Data: nil},
                    {Length: 1, Data: []byte{0x01}},
                },
            },
            wantErr: false,
        },
        {
            name: "trailer (EOF)",
            data: []byte{0xFF, 0xFF}, // -1 as uint16
            want: nil,
            wantErr: true, // Should return io.EOF
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            parser := NewParser(bytes.NewReader(tt.data))
            tuple, err := parser.ParseTuple()
            
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                require.NoError(t, err)
                assert.Equal(t, tt.want, tuple)
            }
        })
    }
}

// Helper functions for test data generation
func buildHeaderBytes(t *testing.T, flags, extLen uint32) []byte {
    var buf bytes.Buffer
    buf.WriteString("PGCOPY\n\377\r\n\000")
    binary.Write(&buf, binary.BigEndian, flags)
    binary.Write(&buf, binary.BigEndian, extLen)
    return buf.Bytes()
}

func buildTupleBytes(t *testing.T, fields []Field) []byte {
    var buf bytes.Buffer
    binary.Write(&buf, binary.BigEndian, uint16(len(fields)))
    
    for _, field := range fields {
        binary.Write(&buf, binary.BigEndian, field.Length)
        if field.Length > 0 {
            buf.Write(field.Data)
        }
    }
    
    return buf.Bytes()
}
```

### 2. Type System Tests

```go
package types

import (
    "math"
    "testing"
    
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestInt4Type(t *testing.T) {
    handler := &Int4Type{}
    
    tests := []struct {
        name    string
        data    []byte
        want    interface{}
        wantErr bool
    }{
        {
            name: "positive int",
            data: []byte{0x00, 0x00, 0x00, 0x2A}, // 42
            want: int32(42),
            wantErr: false,
        },
        {
            name: "negative int", 
            data: []byte{0xFF, 0xFF, 0xFF, 0xD6}, // -42
            want: int32(-42),
            wantErr: false,
        },
        {
            name: "max int32",
            data: []byte{0x7F, 0xFF, 0xFF, 0xFF}, // 2147483647
            want: int32(math.MaxInt32),
            wantErr: false,
        },
        {
            name: "min int32",
            data: []byte{0x80, 0x00, 0x00, 0x00}, // -2147483648
            want: int32(math.MinInt32),
            wantErr: false,
        },
        {
            name: "invalid length",
            data: []byte{0x00, 0x00, 0x2A}, // 3 bytes
            want: nil,
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := handler.Parse(tt.data)
            
            if tt.wantErr {
                assert.Error(t, err)
                assert.Nil(t, result)
            } else {
                require.NoError(t, err)
                assert.Equal(t, tt.want, result)
            }
        })
    }
    
    // Test type metadata
    assert.Equal(t, uint32(23), handler.OID())
    assert.Equal(t, "int4", handler.Name())
    assert.Equal(t, arrow.PrimitiveTypes.Int32, handler.ArrowType())
}

func TestTextType(t *testing.T) {
    handler := &TextType{}
    
    tests := []struct {
        name    string
        data    []byte
        want    interface{}
        wantErr bool
    }{
        {
            name: "simple ascii",
            data: []byte("Hello"),
            want: "Hello",
            wantErr: false,
        },
        {
            name: "utf8 text",
            data: []byte("Hello, 世界"),
            want: "Hello, 世界",
            wantErr: false,
        },
        {
            name: "empty string",
            data: []byte(""),
            want: "",
            wantErr: false,
        },
        {
            name: "special chars",
            data: []byte("Hello\nWorld\t!"),
            want: "Hello\nWorld\t!",
            wantErr: false,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := handler.Parse(tt.data)
            
            if tt.wantErr {
                assert.Error(t, err)
                assert.Nil(t, result)
            } else {
                require.NoError(t, err)
                assert.Equal(t, tt.want, result)
            }
        })
    }
}

func TestBoolType(t *testing.T) {
    handler := &BoolType{}
    
    tests := []struct {
        name    string
        data    []byte
        want    interface{}
        wantErr bool
    }{
        {
            name: "true",
            data: []byte{0x01},
            want: true,
            wantErr: false,
        },
        {
            name: "false",
            data: []byte{0x00},
            want: false,
            wantErr: false,
        },
        {
            name: "invalid length",
            data: []byte{0x01, 0x00},
            want: nil,
            wantErr: true,
        },
        {
            name: "empty data",
            data: []byte{},
            want: nil,
            wantErr: true,
        },
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := handler.Parse(tt.data)
            
            if tt.wantErr {
                assert.Error(t, err)
                assert.Nil(t, result)
            } else {
                require.NoError(t, err)
                assert.Equal(t, tt.want, result)
            }
        })
    }
}
```

### 3. Arrow Builder Tests

```go
package arrow

import (
    "testing"
    
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestRecordBuilder(t *testing.T) {
    allocator := memory.NewGoAllocator()
    
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int32},
        {Name: "name", Type: arrow.BinaryTypes.String},
        {Name: "active", Type: arrow.FixedWidthTypes.Boolean},
    }, nil)
    
    builder := NewRecordBuilder(schema, allocator)
    
    // Test data
    rows := [][]interface{}{
        {int32(1), "Alice", true},
        {int32(2), "Bob", false},
        {nil, "Charlie", true},        // NULL int32
        {int32(4), nil, false},        // NULL string
        {int32(5), "Eve", nil},        // NULL bool
    }
    
    // Append rows
    for _, row := range rows {
        err := builder.AppendRow(row)
        require.NoError(t, err)
    }
    
    // Build record
    record := builder.NewRecord()
    defer record.Release()
    
    // Validate record
    assert.Equal(t, int64(5), record.NumRows())
    assert.Equal(t, int64(3), record.NumCols())
    
    // Validate schema
    assert.True(t, schema.Equal(record.Schema()))
    
    // Validate columns
    idCol := record.Column(0).(*array.Int32)
    nameCol := record.Column(1).(*array.String)
    activeCol := record.Column(2).(*array.Boolean)
    
    // Check values
    assert.Equal(t, int32(1), idCol.Value(0))
    assert.Equal(t, int32(2), idCol.Value(1))
    assert.True(t, idCol.IsNull(2))
    
    assert.Equal(t, "Alice", nameCol.Value(0))
    assert.Equal(t, "Bob", nameCol.Value(1))
    assert.Equal(t, "Charlie", nameCol.Value(2))
    assert.True(t, nameCol.IsNull(3))
    
    assert.True(t, activeCol.Value(0))
    assert.False(t, activeCol.Value(1))
    assert.True(t, activeCol.Value(2))
    assert.False(t, activeCol.Value(3))
    assert.True(t, activeCol.IsNull(4))
}
```

## Integration Testing with pgxmock

### 1. Connection Tests

```go
package pgarrow

import (
    "context"
    "testing"
    
    "github.com/pashagolub/pgxmock/v4"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestPool_QueryArrow(t *testing.T) {
    mock, err := pgxmock.NewPool()
    require.NoError(t, err)
    defer mock.Close()
    
    // Create test binary data
    binaryData := buildTestBinaryData(t, []TestRow{
        {ID: 1, Name: "Alice", Active: true},
        {ID: 2, Name: "Bob", Active: false},
        {ID: 3, Name: "Charlie", Active: true},
    })
    
    // Set up mock expectation
    sql := "SELECT id, name, active FROM users WHERE active = $1"
    copySQL := "COPY \\(SELECT id, name, active FROM users WHERE active = \\$1\\) TO STDOUT \\(FORMAT BINARY\\)"
    
    mock.ExpectCopyTo(copySQL).
        WithArgs(true).
        WillReturnRawContent(binaryData)
    
    // Execute query
    pool := &Pool{pool: mock}
    record, err := pool.QueryArrow(context.Background(), sql, true)
    
    require.NoError(t, err)
    defer record.Release()
    
    // Validate results
    assert.Equal(t, int64(3), record.NumRows())
    assert.Equal(t, int64(3), record.NumCols())
    
    // Validate schema
    schema := record.Schema()
    assert.Equal(t, "id", schema.Field(0).Name)
    assert.Equal(t, "name", schema.Field(1).Name)
    assert.Equal(t, "active", schema.Field(2).Name)
    
    // Validate data
    idCol := record.Column(0).(*array.Int32)
    nameCol := record.Column(1).(*array.String)
    activeCol := record.Column(2).(*array.Boolean)
    
    assert.Equal(t, []int32{1, 2, 3}, idCol.Int32Values())
    assert.Equal(t, []string{"Alice", "Bob", "Charlie"}, 
        []string{nameCol.Value(0), nameCol.Value(1), nameCol.Value(2)})
    assert.Equal(t, []bool{true, false, true},
        []bool{activeCol.Value(0), activeCol.Value(1), activeCol.Value(2)})
    
    // Verify all expectations met
    assert.NoError(t, mock.ExpectationsWereMet())
}

func TestPool_QueryArrow_WithNulls(t *testing.T) {
    mock, err := pgxmock.NewPool()
    require.NoError(t, err)
    defer mock.Close()
    
    // Test data with NULL values
    binaryData := buildTestBinaryData(t, []TestRow{
        {ID: 1, Name: "Alice", Active: true},
        {ID: nil, Name: "Bob", Active: false},      // NULL ID
        {ID: 3, Name: nil, Active: true},           // NULL Name
        {ID: 4, Name: "Dave", Active: nil},         // NULL Active
    })
    
    mock.ExpectCopyTo("COPY \\(.*\\) TO STDOUT \\(FORMAT BINARY\\)").
        WillReturnRawContent(binaryData)
    
    pool := &Pool{pool: mock}
    record, err := pool.QueryArrow(context.Background(), "SELECT id, name, active FROM users")
    
    require.NoError(t, err)
    defer record.Release()
    
    // Validate NULL handling
    idCol := record.Column(0).(*array.Int32)
    nameCol := record.Column(1).(*array.String)
    activeCol := record.Column(2).(*array.Boolean)
    
    assert.False(t, idCol.IsNull(0))
    assert.True(t, idCol.IsNull(1))   // NULL ID
    assert.False(t, idCol.IsNull(2))
    assert.False(t, idCol.IsNull(3))
    
    assert.False(t, nameCol.IsNull(0))
    assert.False(t, nameCol.IsNull(1))
    assert.True(t, nameCol.IsNull(2))  // NULL Name
    assert.False(t, nameCol.IsNull(3))
    
    assert.False(t, activeCol.IsNull(0))
    assert.False(t, activeCol.IsNull(1))
    assert.False(t, activeCol.IsNull(2))
    assert.True(t, activeCol.IsNull(3)) // NULL Active
}

func TestPool_ErrorHandling(t *testing.T) {
    mock, err := pgxmock.NewPool()
    require.NoError(t, err)
    defer mock.Close()
    
    // Test connection error
    expectedErr := pgxmock.NewPgError("connection_failure", "", "", "")
    mock.ExpectCopyTo("COPY \\(.*\\) TO STDOUT \\(FORMAT BINARY\\)").
        WillReturnError(expectedErr)
    
    pool := &Pool{pool: mock}
    record, err := pool.QueryArrow(context.Background(), "SELECT * FROM users")
    
    assert.Error(t, err)
    assert.Nil(t, record)
    
    // Test malformed binary data
    mock.ExpectCopyTo("COPY \\(.*\\) TO STDOUT \\(FORMAT BINARY\\)").
        WillReturnRawContent([]byte("invalid binary data"))
    
    record, err = pool.QueryArrow(context.Background(), "SELECT * FROM users")
    assert.Error(t, err)
    assert.Nil(t, record)
}
```

### 2. Connection Pool Tests

```go
func TestPool_ConcurrentQueries(t *testing.T) {
    mock, err := pgxmock.NewPool()
    require.NoError(t, err)
    defer mock.Close()
    
    // Set up expectations for concurrent queries
    for i := 0; i < 10; i++ {
        binaryData := buildTestBinaryData(t, []TestRow{
            {ID: i, Name: fmt.Sprintf("User%d", i), Active: true},
        })
        
        mock.ExpectCopyTo("COPY \\(.*\\) TO STDOUT \\(FORMAT BINARY\\)").
            WillReturnRawContent(binaryData)
    }
    
    pool := &Pool{pool: mock}
    
    // Execute concurrent queries
    var wg sync.WaitGroup
    results := make([]arrow.Record, 10)
    errors := make([]error, 10)
    
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(idx int) {
            defer wg.Done()
            
            record, err := pool.QueryArrow(context.Background(), 
                fmt.Sprintf("SELECT * FROM users WHERE id = %d", idx))
            results[idx] = record
            errors[idx] = err
        }(i)
    }
    
    wg.Wait()
    
    // Validate all queries succeeded
    for i := 0; i < 10; i++ {
        assert.NoError(t, errors[i])
        assert.NotNil(t, results[i])
        if results[i] != nil {
            results[i].Release()
        }
    }
    
    assert.NoError(t, mock.ExpectationsWereMet())
}
```

## Test Data Generation Helpers

### Binary Format Builder

```go
package helpers

import (
    "bytes"
    "encoding/binary"
    "testing"
)

type TestRow struct {
    ID     interface{} // int32 or nil
    Name   interface{} // string or nil
    Active interface{} // bool or nil
}

func buildTestBinaryData(t *testing.T, rows []TestRow) []byte {
    var buf bytes.Buffer
    
    // Write header
    writeHeader(&buf)
    
    // Write rows
    for _, row := range rows {
        writeRow(&buf, row)
    }
    
    // Write trailer
    writeTrailer(&buf)
    
    return buf.Bytes()
}

func writeHeader(buf *bytes.Buffer) {
    buf.WriteString("PGCOPY\n\377\r\n\000")
    binary.Write(buf, binary.BigEndian, uint32(0)) // flags
    binary.Write(buf, binary.BigEndian, uint32(0)) // extension length
}

func writeRow(buf *bytes.Buffer, row TestRow) {
    binary.Write(buf, binary.BigEndian, uint16(3)) // field count
    
    // Write ID field
    writeField(buf, row.ID, func(buf *bytes.Buffer, val interface{}) {
        binary.Write(buf, binary.BigEndian, val.(int32))
    })
    
    // Write Name field
    writeField(buf, row.Name, func(buf *bytes.Buffer, val interface{}) {
        buf.WriteString(val.(string))
    })
    
    // Write Active field
    writeField(buf, row.Active, func(buf *bytes.Buffer, val interface{}) {
        if val.(bool) {
            buf.WriteByte(0x01)
        } else {
            buf.WriteByte(0x00)
        }
    })
}

func writeField(buf *bytes.Buffer, value interface{}, writer func(*bytes.Buffer, interface{})) {
    if value == nil {
        binary.Write(buf, binary.BigEndian, int32(-1)) // NULL
        return
    }
    
    // Write field data to temp buffer to calculate length
    var fieldBuf bytes.Buffer
    writer(&fieldBuf, value)
    
    // Write length then data
    binary.Write(buf, binary.BigEndian, int32(fieldBuf.Len()))
    buf.Write(fieldBuf.Bytes())
}

func writeTrailer(buf *bytes.Buffer) {
    binary.Write(buf, binary.BigEndian, int16(-1))
}
```

## Performance Testing

### Benchmark Comparisons

```go
package performance

import (
    "context"
    "testing"
    
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func BenchmarkQueryArrow_SmallResult(b *testing.B) {
    pool := setupTestPool(b)
    defer pool.Close()
    
    sql := "SELECT id, name, active FROM users LIMIT 100"
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        record, err := pool.QueryArrow(context.Background(), sql)
        if err != nil {
            b.Fatal(err)
        }
        record.Release()
    }
}

func BenchmarkQueryArrow_LargeResult(b *testing.B) {
    pool := setupTestPool(b)
    defer pool.Close()
    
    sql := "SELECT id, name, active FROM users LIMIT 10000"
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        record, err := pool.QueryArrow(context.Background(), sql)
        if err != nil {
            b.Fatal(err)
        }
        record.Release()
    }
}

func BenchmarkMemoryUsage(b *testing.B) {
    allocator := memory.NewGoAllocator()
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        // Simulate record creation and release
        record := buildLargeRecord(allocator, 10000)
        record.Release()
    }
}
```

## Continuous Integration

### GitHub Actions Configuration

```yaml
# .github/workflows/test.yml
name: Test

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: testdb
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Go
      uses: actions/setup-go@v4
      with:
        go-version: '1.21'
        
    - name: Run unit tests
      run: go test -v ./tests/unit/...
      
    - name: Run integration tests
      run: go test -v ./tests/integration/...
      env:
        DATABASE_URL: postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable
        
    - name: Run benchmarks
      run: go test -bench=. -benchmem ./tests/performance/...
      
    - name: Check test coverage
      run: |
        go test -coverprofile=coverage.out ./...
        go tool cover -html=coverage.out -o coverage.html
        
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.out
```

## Test Execution Strategy

### Development Workflow
1. **Red**: Write failing test first
2. **Green**: Implement minimal code to pass
3. **Refactor**: Clean up implementation
4. **Repeat**: Continue with next feature

### Test Categories
- **Unit Tests**: Run on every commit (< 1 second)
- **Integration Tests**: Run on PR (< 30 seconds)  
- **Performance Tests**: Run nightly (< 5 minutes)
- **End-to-End Tests**: Run before release (< 10 minutes)

### Coverage Targets
- **Unit Tests**: 95% line coverage
- **Integration Tests**: 85% feature coverage
- **Critical Paths**: 100% coverage (connection, parsing, conversion)

This comprehensive testing strategy ensures PGArrow is robust, performant, and maintainable while supporting rapid development through TDD practices.