# PGArrow: Pure Go PostgreSQL-to-Arrow Implementation Plan

## Executive Summary

PGArrow is a pure Go library that provides ADBC-like functionality for converting PostgreSQL query results directly to Apache Arrow format, without CGO dependencies. It addresses the major scalability issue in ADBC where connection initialization takes 6-8 seconds for databases with 20k+ tables due to upfront metadata loading.

**Key Benefits:**
- ✅ **Instant connections** (no metadata preloading)  
- ✅ **Zero CGO dependencies** (pure Go)
- ✅ **High performance** (direct binary format conversion)
- ✅ **pgx compatibility** (uses pgxpool for connection management)
- ✅ **Scalable** (works with any database size)

## Architecture Overview

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   User Code     │───▶│    PGArrow       │───▶│  Apache Arrow   │
│                 │    │                  │    │   RecordBatch   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   pgx/pgxpool    │
                       │                  │
                       └──────────────────┘
                              │
                              ▼
                       ┌──────────────────┐
                       │   PostgreSQL     │
                       │  COPY TO BINARY  │
                       └──────────────────┘
```

## Core Components

### 1. Connection Management
```go
type Pool struct {
    pool *pgxpool.Pool
}

type Conn struct {
    conn *pgx.Conn
}
```

### 2. Binary Format Parser
```go
type BinaryParser struct {
    typeCache map[uint32]TypeInfo
}

type TypeInfo struct {
    OID        uint32
    Name       string
    ArrowType  arrow.DataType
    Parser     func([]byte) (interface{}, error)
}
```

### 3. Arrow Builder
```go
type ArrowBuilder struct {
    schema  *arrow.Schema
    builders []array.Builder
}
```

## Phase 1: Minimal Proof of Concept

### Target Data Types
Focus on the most common PostgreSQL types with straightforward binary encoding:

| PostgreSQL Type | OID | Binary Format | Arrow Type | Priority |
|----------------|-----|---------------|------------|----------|
| `bool` | 16 | 1 byte | `bool` | High |
| `int2` | 21 | 2 bytes (network order) | `int16` | High |
| `int4` | 23 | 4 bytes (network order) | `int32` | High |
| `int8` | 20 | 8 bytes (network order) | `int64` | High |
| `float4` | 700 | IEEE 754 single | `float32` | High |
| `float8` | 701 | IEEE 754 double | `float64` | High |
| `text` | 25 | UTF-8 string | `string` | High |

### Binary Format Specification

#### COPY Binary File Structure
```
File Header (15 bytes):
┌─────────────────────────────────────────────────────────────┐
│ "PGCOPY\n\377\r\n\0" (11 bytes) │ Flags (4 bytes) │ Ext (4 bytes) │
└─────────────────────────────────────────────────────────────┘

For each tuple:
┌─────────────────┬─────────────────┬─────────────────┬─────────┐
│ Field Count     │ Field 1 Length  │ Field 1 Data    │   ...   │
│ (2 bytes)       │ (4 bytes)       │ (variable)      │         │
└─────────────────┴─────────────────┴─────────────────┴─────────┘

File Trailer:
┌─────────────────┐
│ -1 (2 bytes)    │
└─────────────────┘
```

#### Data Type Binary Encoding

**Integer Types** (network byte order / big-endian):
- `int2`: 2-byte signed integer
- `int4`: 4-byte signed integer  
- `int8`: 8-byte signed integer

**Floating Point** (IEEE 754):
- `float4`: 4-byte single precision
- `float8`: 8-byte double precision

**Boolean**:
- `bool`: 1-byte value (0x00 = false, 0x01 = true)

**Text**:
- `text`: UTF-8 encoded string (no null terminator)

**NULL Values**:
- Field length = -1 (0xFFFFFFFF)

## Test-Driven Development Strategy

### Testing Framework
Use `github.com/pashagolub/pgxmock` for mocking pgx interactions:

```go
func TestQueryArrow(t *testing.T) {
    mock, err := pgxmock.NewPool()
    require.NoError(t, err)
    defer mock.Close()
    
    // Mock COPY TO BINARY response
    binaryData := buildMockBinaryData(t, testRows)
    mock.ExpectCopyTo("COPY \\(SELECT.*\\) TO STDOUT \\(FORMAT BINARY\\)").
        WillReturnRawContent(binaryData)
    
    pool := &Pool{pool: mock}
    record, err := pool.QueryArrow(context.Background(), "SELECT id, name FROM users")
    
    require.NoError(t, err)
    assert.Equal(t, 2, int(record.NumCols()))
    assert.Equal(t, 3, int(record.NumRows()))
}
```

### Test Data Generation
Create helper functions to generate valid PostgreSQL binary COPY format:

```go
func buildMockBinaryData(t *testing.T, rows []TestRow) []byte {
    var buf bytes.Buffer
    
    // Write header
    buf.WriteString("PGCOPY\n\377\r\n\000")
    binary.Write(&buf, binary.BigEndian, uint32(0)) // flags
    binary.Write(&buf, binary.BigEndian, uint32(0)) // extension length
    
    // Write rows
    for _, row := range rows {
        writeRowBinary(&buf, row)
    }
    
    // Write trailer
    binary.Write(&buf, binary.BigEndian, int16(-1))
    
    return buf.Bytes()
}
```

### Test Scenarios

#### Unit Tests
1. **Binary Parser Tests**
   - Parse each supported data type
   - Handle NULL values correctly
   - Validate network byte order conversion
   - Error handling for malformed data

2. **Type Mapping Tests**
   - OID to Arrow type conversion
   - Schema generation from column metadata
   - Type compatibility validation

3. **Arrow Builder Tests**  
   - Build Arrow arrays from parsed data
   - Handle mixed NULL/non-NULL values
   - Validate Arrow schema correctness

#### Integration Tests
1. **End-to-End Query Tests**
   - Simple SELECT queries
   - Mixed data type columns
   - Large result sets
   - Empty result sets

2. **Connection Pool Tests**
   - Concurrent query execution
   - Connection reuse
   - Error propagation
   - Resource cleanup

3. **Performance Tests**
   - Connection speed vs ADBC
   - Query performance benchmarks
   - Memory usage validation

## Implementation Phases

### Phase 1: Core Foundation (Week 1-2)
- [ ] Set up project structure and dependencies
- [ ] Implement binary format parser for basic types
- [ ] Create Arrow builders for supported types
- [ ] Write comprehensive unit tests
- [ ] Basic integration with pgx COPY TO

### Phase 2: Integration & Testing (Week 3)
- [ ] Implement connection and pool wrappers
- [ ] Add comprehensive integration tests
- [ ] Performance benchmarking against ADBC
- [ ] Error handling and edge cases
- [ ] Documentation and examples

### Phase 3: Production Readiness (Week 4)
- [ ] Add remaining common data types (dates, arrays)
- [ ] Implement streaming for large result sets
- [ ] Add configuration options
- [ ] CI/CD pipeline setup
- [ ] Production deployment testing

## Detailed Implementation Approach

### 1. Project Structure
```
pgarrow/
├── *.go                   # All implementation files in root
├── *_test.go              # All test files in root  
├── docs/                  # Documentation
├── testdata/              # Binary test samples
└── examples/              # Usage examples
```

### 2. Simple File Structure

**Core files in root:**
- `pgarrow.go` - Main API (Pool, QueryArrow)
- `binary.go` - Binary format parser  
- `types.go` - Type handlers (int4, text, bool, etc.)
- `arrow.go` - Arrow record building
- `pgarrow_test.go` - Main tests
- `binary_test.go` - Parser tests
- `types_test.go` - Type conversion tests
- `testhelpers.go` - Mock data generation

### 3. Core API Design

```go
package pgarrow

import (
    "context"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps pgxpool.Pool and provides Arrow query capabilities
type Pool struct {
    pool *pgxpool.Pool
}

// NewPool creates a new PGArrow pool
func NewPool(ctx context.Context, connString string) (*Pool, error)

// QueryArrow executes a query and returns Arrow record
func (p *Pool) QueryArrow(ctx context.Context, sql string, args ...interface{}) (arrow.Record, error)

// Close closes the pool
func (p *Pool) Close()
```

### 4. Binary Format Parser Implementation

```go
// In binary.go

import (
    "encoding/binary"
    "fmt"
    "io"
)

type Parser struct {
    reader io.Reader
    byteOrder binary.ByteOrder
}

type Header struct {
    Signature   [11]byte
    Flags       uint32
    ExtensionLen uint32
}

type Tuple struct {
    FieldCount uint16
    Fields     []Field
}

type Field struct {
    Length int32  // -1 for NULL
    Data   []byte // nil for NULL
}

func (p *Parser) ParseHeader() (*Header, error) {
    var header Header
    
    // Read signature
    if _, err := io.ReadFull(p.reader, header.Signature[:]); err != nil {
        return nil, fmt.Errorf("failed to read signature: %w", err)
    }
    
    // Validate signature
    expectedSig := [11]byte{'P','G','C','O','P','Y','\n',0xff,'\r','\n',0}
    if header.Signature != expectedSig {
        return nil, fmt.Errorf("invalid signature")
    }
    
    // Read flags and extension length
    if err := binary.Read(p.reader, binary.BigEndian, &header.Flags); err != nil {
        return nil, fmt.Errorf("failed to read flags: %w", err)
    }
    
    if err := binary.Read(p.reader, binary.BigEndian, &header.ExtensionLen); err != nil {
        return nil, fmt.Errorf("failed to read extension length: %w", err)
    }
    
    return &header, nil
}

func (p *Parser) ParseTuple() (*Tuple, error) {
    var tuple Tuple
    
    // Read field count
    if err := binary.Read(p.reader, binary.BigEndian, &tuple.FieldCount); err != nil {
        return nil, fmt.Errorf("failed to read field count: %w", err)
    }
    
    // Handle trailer (-1)
    if tuple.FieldCount == 0xFFFF {
        return nil, io.EOF
    }
    
    // Read fields
    tuple.Fields = make([]Field, tuple.FieldCount)
    for i := range tuple.Fields {
        field, err := p.parseField()
        if err != nil {
            return nil, fmt.Errorf("failed to parse field %d: %w", i, err)
        }
        tuple.Fields[i] = *field
    }
    
    return &tuple, nil
}

func (p *Parser) parseField() (*Field, error) {
    var field Field
    
    // Read field length
    if err := binary.Read(p.reader, binary.BigEndian, &field.Length); err != nil {
        return nil, fmt.Errorf("failed to read field length: %w", err)
    }
    
    // Handle NULL field
    if field.Length == -1 {
        return &field, nil
    }
    
    // Read field data
    field.Data = make([]byte, field.Length)
    if _, err := io.ReadFull(p.reader, field.Data); err != nil {
        return nil, fmt.Errorf("failed to read field data: %w", err)
    }
    
    return &field, nil
}
```

### 5. Type System Implementation

```go
// In types.go

import (
    "encoding/binary"
    "fmt"
    "github.com/apache/arrow-go/v18/arrow"
)

type TypeRegistry struct {
    types map[uint32]TypeHandler
}

type TypeHandler interface {
    OID() uint32
    Name() string
    ArrowType() arrow.DataType
    Parse(data []byte) (interface{}, error)
}

// Register built-in types
func NewRegistry() *TypeRegistry {
    registry := &TypeRegistry{types: make(map[uint32]TypeHandler)}
    
    registry.Register(&BoolType{})
    registry.Register(&Int2Type{})
    registry.Register(&Int4Type{})
    registry.Register(&Int8Type{})
    registry.Register(&Float4Type{})
    registry.Register(&Float8Type{})
    registry.Register(&TextType{})
    
    return registry
}

// Built-in type implementations
type Int4Type struct{}

func (t *Int4Type) OID() uint32 { return 23 }
func (t *Int4Type) Name() string { return "int4" }
func (t *Int4Type) ArrowType() arrow.DataType { return arrow.PrimitiveTypes.Int32 }

func (t *Int4Type) Parse(data []byte) (interface{}, error) {
    if len(data) != 4 {
        return nil, fmt.Errorf("int4 requires 4 bytes, got %d", len(data))
    }
    value := binary.BigEndian.Uint32(data)
    return int32(value), nil
}

type TextType struct{}

func (t *TextType) OID() uint32 { return 25 }
func (t *TextType) Name() string { return "text" }
func (t *TextType) ArrowType() arrow.DataType { return arrow.BinaryTypes.String }

func (t *TextType) Parse(data []byte) (interface{}, error) {
    return string(data), nil
}
```

### 6. Arrow Integration

```go
// In arrow.go

import (
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

type RecordBuilder struct {
    schema   *arrow.Schema
    builders []array.Builder
    allocator memory.Allocator
}

func NewRecordBuilder(schema *arrow.Schema, allocator memory.Allocator) *RecordBuilder {
    builders := make([]array.Builder, len(schema.Fields()))
    for i, field := range schema.Fields() {
        builders[i] = array.NewBuilder(allocator, field.Type)
    }
    
    return &RecordBuilder{
        schema:    schema,
        builders:  builders,
        allocator: allocator,
    }
}

func (rb *RecordBuilder) AppendRow(values []interface{}) error {
    for i, value := range values {
        builder := rb.builders[i]
        
        if value == nil {
            builder.AppendNull()
            continue
        }
        
        switch b := builder.(type) {
        case *array.Int32Builder:
            b.Append(value.(int32))
        case *array.StringBuilder:
            b.Append(value.(string))
        case *array.BooleanBuilder:
            b.Append(value.(bool))
        // Add other types...
        default:
            return fmt.Errorf("unsupported builder type: %T", builder)
        }
    }
    
    return nil
}

func (rb *RecordBuilder) NewRecord() arrow.Record {
    columns := make([]arrow.Array, len(rb.builders))
    for i, builder := range rb.builders {
        columns[i] = builder.NewArray()
    }
    
    return array.NewRecord(rb.schema, columns, -1)
}
```

## Performance Considerations

### Connection Speed
- **Target**: Sub-100ms connection time (vs 6-8s for ADBC)
- **Approach**: No upfront metadata loading
- **Benefit**: Scales to any database size

### Query Performance  
- **Binary Format**: ~2-3x faster than text parsing
- **Zero-Copy**: Minimize data copying where possible
- **Streaming**: Support large result sets without memory issues

### Memory Management
- **Arrow Integration**: Leverage Arrow's efficient memory layout
- **Pool Reuse**: Reuse builders and allocators
- **Type Cache**: Cache type handlers to avoid repeated lookups

## Risk Mitigation

### Data Type Coverage
- **Risk**: Limited PostgreSQL type support
- **Mitigation**: Extensible type system, fallback to binary blobs

### Binary Format Changes
- **Risk**: PostgreSQL binary format evolution
- **Mitigation**: Version detection, backward compatibility

### Performance Regression
- **Risk**: Slower than expected performance
- **Mitigation**: Comprehensive benchmarks, profiling tools

## Success Metrics

1. **Connection Speed**: <100ms vs 6-8s for ADBC with 20k+ tables
2. **Query Performance**: Within 20% of native pgx performance
3. **Memory Usage**: <50% of ADBC memory consumption
4. **Type Coverage**: Support for 80% of common PostgreSQL types
5. **API Compatibility**: Drop-in replacement for basic ADBC usage

## Future Enhancements

### Phase 2: Advanced Types
- Date/time types (timestamp, date, interval)
- Array types (int[], text[])
- JSON/JSONB support
- Geometric types (point, polygon)

### Phase 3: Advanced Features
- Prepared statement support
- Bulk insert capabilities
- Streaming for massive datasets
- Connection failover/load balancing

### Phase 4: Ecosystem Integration
- Database/sql compatibility layer
- ORM integration (GORM, etc.)
- Observability and metrics
- Configuration management

## Conclusion

PGArrow addresses a critical scalability issue in ADBC while providing a pure Go solution that integrates seamlessly with the pgx ecosystem. The test-driven approach ensures reliability, while the phased implementation allows for rapid iteration and validation of core concepts.

The proof of concept focuses on the most common data types and scenarios, providing a solid foundation for future expansion while delivering immediate value for applications requiring fast PostgreSQL-to-Arrow conversion without CGO dependencies.